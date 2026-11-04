import os
import re
import sys
import logging
from decimal import Decimal, ROUND_HALF_UP, localcontext

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from py.common_helper import get_dag_start_date,create_json_from_bqconfig, read_json

from py.get_env_variable import get_curate_project_id, get_enrich_project_id, get_resource_sa, create_json_from_bqconfig, get_raw_project_id, read_json
from py.error_helper import task_failure_log
from py.obserbility_helper import success_log

# ---------------------------------------------------------------------------
# Set variables
# ---------------------------------------------------------------------------
PROJECT_ID_CURATE = get_curate_project_id()
PROJECT_ID_ENRICH = get_enrich_project_id()
PROJECT_ID_RAW = get_raw_project_id()
RESOURCE_SA = get_resource_sa()

PROJECT_SPACE = Variable.get('PROJECT_SPACE')

# One DAG per entry. Everything below (settings, functions, DAG definition) is evaluated for each entry.
dag_configs = ["epm_challenger_model_flatten_enrich_revenue"]
for dag_name in dag_configs:
    DAG_NAME = dag_name

    CONFIG_FILE_PATH = f'dags/{PROJECT_SPACE}/{DAG_NAME}/config/{DAG_NAME}_config.json'

    # Create json file from config
    create_json_from_bqconfig(CONFIG_FILE_PATH, DAG_NAME)

    bq_config_params = read_json(CONFIG_FILE_PATH)

    BQ_CONN_ID = bq_config_params["bq_conn_id"]

    # -----------------------------------------------------------------------
    # Table Variables Split (Project, Dataset, Table)
    # -----------------------------------------------------------------------
    # Curate Config
    CURATE_DATASET = bq_config_params.get("curate_dataset")
    CURATE_TABLE_NAME = bq_config_params.get("curate_table_name")
    CONFIG_TABLE = f"{PROJECT_ID_CURATE}.{CURATE_DATASET}.{CURATE_TABLE_NAME}"

    # Enrich Hierarchy
    ENRICH_PROJECT_ID = bq_config_params.get("enrich_project_id", PROJECT_ID_ENRICH)
    ENRICH_DATASET = bq_config_params.get("enrich_dataset")
    ENRICH_TABLE_NAME = bq_config_params.get("enrich_table_name")
    HIERARCHY_TABLE = f"{PROJECT_ID_CURATE}.{ENRICH_DATASET}.{ENRICH_TABLE_NAME}"

    # Raw Transaction
    RAW_PROJECT_ID = bq_config_params.get("raw_project_id")
    RAW_DATASET = bq_config_params.get("raw_dataset")
    RAW_TABLE_NAME = bq_config_params.get("raw_table_name")
    TRANSACTION_TABLE = f"{PROJECT_ID_CURATE}.{RAW_DATASET}.{RAW_TABLE_NAME}"

    # Publish Output
    PUBLISH_PROJECT_ID = bq_config_params.get("publish_project_id")
    PUBLISH_DATASET = bq_config_params.get("publish_dataset")
    PUBLISH_TABLE_NAME = bq_config_params.get("publish_table_name")
    OUTPUT_TABLE = f"{PROJECT_ID_CURATE}.{PUBLISH_DATASET}.{PUBLISH_TABLE_NAME}"

    # Staging table: the SQL step writes the NON-calculated line item values here; the Python step reads
    # them back, calculates the formula rows and writes OUTPUT_TABLE.
    STAGING_TABLE_NAME = bq_config_params.get("staging_table_name", f"{PUBLISH_TABLE_NAME}_base_values")
    STAGING_TABLE = f"{PROJECT_ID_CURATE}.{PUBLISH_DATASET}.{STAGING_TABLE_NAME}"

    # Dimension Map Metadata
    DIM_MAP_PROJECT_ID = bq_config_params.get("raw_project_id", "vz-it-np-j0nv-dev-oddo-0")
    DIM_MAP_DATASET = bq_config_params.get("raw_dataset", "od_enriched_epmmlops")
    DIM_MAP_TABLE_NAME = f"{DAG_NAME}_dimensions"
    DIMENSION_MAP_TABLE = f"{PROJECT_ID_CURATE}.{ENRICH_DATASET}.{DIM_MAP_TABLE_NAME}"

    OPERATIONAL_DATASET = bq_config_params["operational_dataset"]
    bq_conn_id = BQ_CONN_ID

    schedule_int = bq_config_params["schedule_int"]

    # ---------------------------------------------------------------------------
    # Hierarchy view columns (edm_master_incl_alt_hierarchies_v)
    # ---------------------------------------------------------------------------
    HIER_PARENT_COL = "parent"
    HIER_CHILD_COL = "node"
    HIER_DIM_COL = "dimension_name"
    HIER_PRIMARY_COL = "is_primary"      # TRUE = primary hierarchy
    HIER_NAME_COL = "hierarchy_name"     # TODO: confirm - name of the hierarchy (master or any alternate)
    # Config-table column holding the hierarchy name for a dimension = <config_col> + this suffix,
    # e.g. ProfitCentreHierarchyName, AccountHierarchyName ... The column is OPTIONAL: if it is not
    # in the config table, that dimension uses is_primary = TRUE.
    HIER_NAME_SUFFIX = "HierarchyName"

    # ---------------------------------------------------------------------------
    # Config / actuals / output
    # ---------------------------------------------------------------------------
    CONFIG_ID_COL = "Config_ID"                       # must be unique in the config table
    LINE_ITEM_COL = "LineItem"                        # config column with the line item name (also used in formulas)
    CONFIG_PASSTHROUGH_COLS = ["LineItem"]            # other config columns carried into the Config CTE

    ACTUALS_YEAR_COL = "year"
    ACTUALS_PERIOD_COL = "period"
    ACTUALS_AMOUNT_COL = "amount"
    ACTUALS_BASE_COLS = [ACTUALS_YEAR_COL, ACTUALS_PERIOD_COL, ACTUALS_AMOUNT_COL]   # always read from actuals

    # Column names of the final output table: LineItem, year, MONTH, Amount
    OUT_YEAR = "year"
    OUT_PERIOD = "MONTH"
    OUT_AMOUNT = "Amount"

    # Python step
    AMOUNT_SCALE = 9                   # decimal places when Amount is written as NUMERIC
    KEEP_STAGING_TABLE = True          # False: drop the staging table after OUTPUT_TABLE has been written

    # OPTIONAL config columns (if a column is missing from the config table it is simply ignored):
    #   Formula : blank -> the row is fetched from actuals by the SQL.
    #             filled -> the row is calculated IN PYTHON from OTHER line items, per year and month,
    #             e.g.  Gross Revenue - Discounts     or     Net Revenue / [Gross Revenue]
    #   Output  : N / NO / FALSE / 0 -> the row is an intermediate step: used by formulas but left out
    #             of the final output. Blank or anything else -> the row is in the output.
    CONFIG_FORMULA_COL = "Formula"
    CONFIG_OUTPUT_COL = "Output"
    OUTPUT_NO_VALUES = ("N", "NO", "FALSE", "0")

    # Values that mean "wildcard": no flattening and no filter on that dimension.
    # (They are all normalised to 'ALL' in the Config CTE.)
    PASSTHROUGH_VALUES = ("ALL", "NA")

    # ---------------------------------------------------------------------------
    # Default Field mapping fallback. One entry per dimension.
    # ---------------------------------------------------------------------------
    DEFAULT_DIMENSION_MAP = [
        {"config_col": "ProfitCentre",        "dimension": "Profit_Center",        "txn_col": "profit_center"},
        {"config_col": "Account",             "dimension": "Account",              "txn_col": "Account"},
        {"config_col": "CompanyCode",         "dimension": "Company_Code",         "txn_col": "Company_Code"},
        {"config_col": "ServiceType",         "dimension": "Service_Type",         "txn_col": "Service_Type"},
        {"config_col": "Platform",            "dimension": "Platform",             "txn_col": "Platform"},
        {"config_col": "CostCenter",          "dimension": "Cost_Center",          "txn_col": "Cost_Center"},
        {"config_col": "GeoRegion",           "dimension": "Geo_Region",           "txn_col": "Geo_Region"},
        {"config_col": "Market",              "dimension": "Market",               "txn_col": "Market"},
        {"config_col": "ContractTerm",        "dimension": "Contract_Term",        "txn_col": "Contract_Term"},
        {"config_col": "Product",             "dimension": "Product",              "txn_col": "Product"},
        {"config_col": "Equipment",           "dimension": "Equipment",            "txn_col": "Equipment"},
        {"config_col": "DistributionChannel", "dimension": "Distribution_Channel", "txn_col": "Distribution_Channel"},
        {"config_col": "Source",              "dimension": "Source",               "txn_col": "Source"},
    ]

    # Attribute alias for external scripts inspecting mod.DIMENSION_MAP
    DIMENSION_MAP = DEFAULT_DIMENSION_MAP


    # ---------------------------------------------------------------------------
    # Fetch dynamic dimension map from BigQuery (with fallback)
    # ---------------------------------------------------------------------------
    def get_dimension_map(client):
        """Fetches dimension mapping from DIMENSION_MAP_TABLE if it exists.
        Falls back to DEFAULT_DIMENSION_MAP if the table does not exist."""
        parts = DIMENSION_MAP_TABLE.split(".")
        if len(parts) == 3:
            project, dataset, table_name = parts
            schema_query = f"`{project}.{dataset}`.INFORMATION_SCHEMA.TABLES"
        elif len(parts) == 2:
            dataset, table_name = parts
            schema_query = f"`{dataset}`.INFORMATION_SCHEMA.TABLES"
        else:
            table_name = parts[0]
            schema_query = "INFORMATION_SCHEMA.TABLES"

        check_table_sql = f"SELECT 1 FROM {schema_query} WHERE table_name = '{table_name}'"

        try:
            table_exists = len(list(client.query(check_table_sql).result())) > 0
        except Exception as e:
            logging.warning("Failed to check existence of table %s: %s. Using default DIMENSION_MAP.",
                            DIMENSION_MAP_TABLE, e)
            table_exists = False

        if not table_exists:
            logging.info("Table %s does not exist. Using DEFAULT_DIMENSION_MAP.", DIMENSION_MAP_TABLE)
            return DEFAULT_DIMENSION_MAP

        query = f"""
        SELECT config_col, dimension, txn_col, hier_name_col
        FROM `{DIMENSION_MAP_TABLE}`
        """
        logging.info("Fetching dimension map from BigQuery table:\n%s", query)
        # Dimension Map table expected columns: config_col, dimension, txn_col, hier_name_col
        dimension_map = []
        for row in client.query(query).result():
            item = {
                "config_col": row["config_col"],
                "dimension": row["dimension"],
                "txn_col": row["txn_col"],
            }
            hier_val = row.get("hier_name_col")
            if hier_val and str(hier_val).strip():
                item["hier_name_col"] = str(hier_val).strip()
            dimension_map.append(item)

        if not dimension_map:
            logging.warning("Dimension map table %s is empty. Falling back to DEFAULT_DIMENSION_MAP.",
                            DIMENSION_MAP_TABLE)
            return DEFAULT_DIMENSION_MAP

        return dimension_map


    # >>> end of generate_sql in your file
    # ---------------------------------------------------------------------------
    # SQL helpers
    # ---------------------------------------------------------------------------
    def _q(values):
        return ", ".join(f"'{v}'" for v in values)


    def _is_wild(col_ref):
        """SQL predicate: value is NULL / blank / ALL / NA."""
        return (f"({col_ref} IS NULL OR TRIM({col_ref}) = '' "
                f"OR UPPER(TRIM({col_ref})) IN ({_q(PASSTHROUGH_VALUES)}))")


    def _not_formula(fcol, alias=""):
        """SQL predicate on the raw config table: the row has NO formula (so it is calculated from actuals)."""
        if not fcol:
            return "TRUE"
        ref = f"{alias}.{fcol}" if alias else fcol
        return f"({ref} IS NULL OR TRIM({ref}) = '')"


    def _is_output_expr(ocol):
        """SQL expression: TRUE when the config row belongs in the final output."""
        if not ocol:
            return "TRUE"
        return f"COALESCE(NOT (UPPER(TRIM({ocol})) IN ({_q(OUTPUT_NO_VALUES)})), TRUE)"


    def _hier_cond(h, c, ncol):
        """Which rows of the hierarchy view to use for this config row.
        h = hierarchy alias, c = Config CTE alias, ncol = hierarchy name column in the config."""
        if not ncol:
            return f"{h}.{HIER_PRIMARY_COL} = TRUE"
        n = f"{c}.{ncol}"                       # already NULLIF(TRIM(..), '') in the Config CTE
        return (f"CASE WHEN {n} IS NULL THEN {h}.{HIER_PRIMARY_COL} = TRUE "
                f"ELSE {h}.{HIER_NAME_COL} = {n} END")


    def _hier_col(m):
        return m.get("hier_name_col") or f"{m['config_col']}{HIER_NAME_SUFFIX}"


    def _gran(m):
        return f"{m['config_col']}Granular"


    def _map_name(m):
        return f"{m['config_col'].lower()}_mapping"


    # ---------------------------------------------------------------------------
    # Config table columns (one metadata query, shared by the steps below)
    # ---------------------------------------------------------------------------
    def get_config_columns(client):
        """Returns {lower-case column name: actual column name} for the config table."""
        parts = CONFIG_TABLE.split(".")
        prefix = ".".join(parts[:-1])
        schema = f"`{prefix}`.INFORMATION_SCHEMA.COLUMNS" if prefix else "INFORMATION_SCHEMA.COLUMNS"
        query = f"SELECT column_name FROM {schema} WHERE table_name = '{parts[-1]}'"
        logging.info("Config columns query:\n%s", query)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        client._http.verify = False
        return {r["column_name"].lower(): r["column_name"] for r in client.query(query).result()}


    # ---------------------------------------------------------------------------
    # Step 1: which dimensions have at least one real node value in config?
    # A dimension that is ALL / NA on every (non-formula) row needs no mapping and no filter.
    # ---------------------------------------------------------------------------
    def detect_dimensions_to_join(client, dimension_map=None, formula_col=None):
        if dimension_map is None:
            dimension_map = get_dimension_map(client)

        count_exprs = ",\n  ".join(
            f"COUNTIF(NOT {_is_wild(m['config_col'])}) AS {m['config_col']}_node_cnt"
            for m in dimension_map
        )
        query = (f"SELECT\n  {count_exprs}\nFROM `{CONFIG_TABLE}`\n"
                 f"WHERE {_not_formula(formula_col)}")          # formula rows have no dimension filters
        logging.info("Dimension detection query:\n%s", query)

        row = list(client.query(query).result())[0]
        join_flags = {}
        for m in dimension_map:
            cnt = row[f"{m['config_col']}_node_cnt"]
            join_flags[m["config_col"]] = cnt > 0
            logging.info("%s: %s node values -> %s", m["config_col"], cnt,
                         "USE" if cnt > 0 else "SKIP (all ALL/NA)")
        return join_flags


    # ---------------------------------------------------------------------------
    # Step 1b: which hierarchy name columns exist in the config table?
    # Missing column -> that dimension simply uses is_primary = TRUE.
    # ---------------------------------------------------------------------------
    def detect_hier_name_cols(client, dimension_map=None, config_cols=None):
        if dimension_map is None:
            dimension_map = get_dimension_map(client)
        if config_cols is None:
            config_cols = get_config_columns(client)

        hier_cols = {}
        for m in dimension_map:
            wanted = _hier_col(m)
            hier_cols[m["config_col"]] = config_cols.get(wanted.lower())      # actual column name or None
            logging.info("%s: %s", m["config_col"],
                         f"hierarchy name column {wanted} found" if hier_cols[m["config_col"]]
                         else f"no {wanted} column -> primary hierarchy (is_primary = TRUE)")
        return hier_cols


    # ---------------------------------------------------------------------------
    # Step 2: validate inputs (fail early instead of silently mis-mapping)
    # ---------------------------------------------------------------------------
    def validate_inputs(client, join_flags, hier_cols, dimension_map=None, formula_col=None):
        if dimension_map is None:
            dimension_map = get_dimension_map(client)

        # 2a. mapping CTEs are keyed on Config_ID, so it must be unique
        dup = list(client.query(
            f"SELECT {CONFIG_ID_COL} AS id FROM `{CONFIG_TABLE}` "
            f"GROUP BY {CONFIG_ID_COL} HAVING COUNT(*) > 1 LIMIT 5").result())
        if dup:
            raise ValueError(f"{CONFIG_ID_COL} must be unique in the config table; "
                             f"duplicates: {[r['id'] for r in dup]}")

        joined = [m for m in dimension_map if join_flags[m["config_col"]]]
        if not joined:
            return

        # 2b. dimension_name spelling must exist in the hierarchy view
        rows = client.query(f"SELECT DISTINCT {HIER_DIM_COL} AS d FROM `{HIERARCHY_TABLE}`").result()
        existing = {r["d"] for r in rows}
        missing = [m["dimension"] for m in joined if m["dimension"] not in existing]
        if missing:
            raise ValueError(f"dimension_name not found in hierarchy view: {missing}. "
                             f"Available: {sorted(x for x in existing if x)}")

        # 2c. hierarchy names in config must exist for that dimension in the hierarchy view
        for m in joined:
            ncol = hier_cols[m["config_col"]]
            if not ncol:
                continue
            in_view = (f"SELECT {HIER_NAME_COL} FROM `{HIERARCHY_TABLE}` "
                       f"WHERE {HIER_DIM_COL} = '{m['dimension']}' AND {HIER_NAME_COL} IS NOT NULL")
            q = (f"SELECT DISTINCT TRIM(c.{ncol}) AS v FROM `{CONFIG_TABLE}` c "
                 f"WHERE {_not_formula(formula_col, 'c')} "
                 f"AND NOT {_is_wild('c.' + m['config_col'])} AND NULLIF(TRIM(c.{ncol}), '') IS NOT NULL "
                 f"AND TRIM(c.{ncol}) NOT IN ({in_view})")
            bad = [r["v"] for r in client.query(q).result()]
            if bad:
                avail = sorted(r["v"] for r in client.query(
                    f"SELECT DISTINCT {HIER_NAME_COL} AS v FROM (" + in_view + ")").result())
                raise ValueError(f"{ncol} has hierarchy names not found for {m['dimension']}: {bad}. "
                                 f"Available: {avail}")

        # 2d. warn about config nodes that exist nowhere in the hierarchy (likely typos);
        # such a value is used as-is (treated like a leaf), so it only matches actuals with that exact value.
        parts = []
        for m in joined:
            col, dim = m["config_col"], m["dimension"]
            parts.append(
                f"SELECT '{dim}' AS dim, c.{col} AS value FROM `{CONFIG_TABLE}` c "
                f"WHERE {_not_formula(formula_col, 'c')} "
                f"AND NOT {_is_wild('c.' + col)} AND TRIM(c.{col}) NOT IN ("
                f"SELECT {HIER_PARENT_COL} FROM `{HIERARCHY_TABLE}` "
                f"WHERE {HIER_DIM_COL} = '{dim}' AND {HIER_PARENT_COL} IS NOT NULL "
                f"UNION DISTINCT "
                f"SELECT {HIER_CHILD_COL} FROM `{HIERARCHY_TABLE}` "
                f"WHERE {HIER_DIM_COL} = '{dim}' AND {HIER_CHILD_COL} IS NOT NULL)")
        unknown = [(r["dim"], r["value"]) for r in
                   client.query(" UNION ALL ".join(parts) + " LIMIT 50").result()]
        if unknown:
            logging.warning("Config values not found in the hierarchy (used as-is): %s", unknown)


    # ---------------------------------------------------------------------------
    # Step 3: formulas (line items calculated from other line items, per year + month)
    # ---------------------------------------------------------------------------
    _NUM_RE = re.compile(r"\d+(?:\.\d+)?")
    _OPS = "+-*/()"


    def _tokenize_formula(text, known):
        """known: {lower-case line item: line item}. Line items are matched by name (longest first),
        so names with spaces work as they are; use [ ] for names with unusual characters."""
        names = sorted(known, key=len, reverse=True)
        low = text.lower()
        tokens, i, n = [], 0, len(text)
        while i < n:
            ch = text[i]
            if ch.isspace():
                i += 1
                continue
            if ch == "[":
                j = text.find("]", i + 1)
                if j < 0:
                    raise ValueError("missing ']'")
                key = text[i + 1:j].strip().lower()
                if key not in known:
                    raise ValueError(f"unknown line item [{text[i + 1:j]}]")
                tokens.append(("ref", known[key]))
                i = j + 1
                continue
            hit = next((nm for nm in names if low.startswith(nm, i)
                        and (i + len(nm) == n or text[i + len(nm)].isspace() or text[i + len(nm)] in _OPS)), None)
            if hit:
                tokens.append(("ref", known[hit]))
                i += len(hit)
                continue
            m = _NUM_RE.match(text, i)
            if m:
                tokens.append(("num", m.group(0)))
                i = m.end()
                continue
            if ch in _OPS:
                tokens.append(("op", ch))
                i += 1
                continue
            raise ValueError(f"cannot read '{text[i:i + 25]}' (not a line item, number or + - * / ( ))")
        return tokens


    def _parse_formula(tokens):
        """expr := term (('+'|'-') term)* ; term := unary (('*'|'/') unary)* ; atom := number | line item | (expr)"""
        pos = 0

        def peek():
            return tokens[pos] if pos < len(tokens) else None

        def take():
            nonlocal pos
            t = tokens[pos]
            pos += 1
            return t

        def expr():
            node = term()
            while peek() and peek()[0] == "op" and peek()[1] in "+-":
                op = take()[1]
                node = ("bin", op, node, term())
            return node

        def term():
            node = unary()
            while peek() and peek()[0] == "op" and peek()[1] in "*/":
                op = take()[1]
                node = ("bin", op, node, unary())
            return node

        def unary():
            if peek() == ("op", "-"):
                take()
                return ("neg", unary())
            if peek() == ("op", "+"):
                take()
                return unary()
            return atom()

        def atom():
            t = peek()
            if t is None:
                raise ValueError("formula ends unexpectedly")
            if t[0] in ("num", "ref"):
                take()
                return (t[0], t[1])
            if t == ("op", "("):
                take()
                node = expr()
                if peek() != ("op", ")"):
                    raise ValueError("missing ')'")
                take()
                return node
            raise ValueError(f"unexpected '{t[1]}'")

        if not tokens:
            raise ValueError("empty formula")
        node = expr()
        if pos != len(tokens):
            raise ValueError(f"unexpected '{tokens[pos][1]}'")
        return node


    def _formula_refs(node):
        if node[0] == "ref":
            return {node[1]}
        if node[0] == "num":
            return set()
        if node[0] == "neg":
            return _formula_refs(node[1])
        return _formula_refs(node[2]) | _formula_refs(node[3])


    def _eval_formula(node, env):
        """Evaluates a parsed formula. env: {line item: Decimal}. Division by zero -> None (NULL)."""
        if node[0] == "num":
            return Decimal(node[1])
        if node[0] == "ref":
            return env[node[1]]
        if node[0] == "neg":
            v = _eval_formula(node[1], env)
            return None if v is None else -v
        op = node[1]
        left, right = _eval_formula(node[2], env), _eval_formula(node[3], env)
        if left is None or right is None:
            return None
        if op == "+":
            return left + right
        if op == "-":
            return left - right
        if op == "*":
            return left * right
        return None if right == 0 else left / right


    def plan_formulas(client, formula_col, output_col):
        """Reads the formula rows, validates them and orders them in layers.
        Returns a list of layers; each layer is a list of dicts
        {cid, line_item, formula, ast, refs, is_output}. A layer only depends on base line items
        and on earlier layers. Returns [] when there is no Formula column or no formula rows."""
        if not formula_col:
            logging.info("No %s column in the config table -> no formula rows", CONFIG_FORMULA_COL)
            return []

        li = LINE_ITEM_COL
        summary = client.query(
            f"SELECT {li} AS line_item, COUNTIF({_not_formula(formula_col)}) AS base_rows, "
            f"COUNTIF(NOT {_not_formula(formula_col)}) AS formula_rows "
            f"FROM `{CONFIG_TABLE}` GROUP BY {li}").result()

        known, formula_items = {}, set()
        for r in summary:
            if r["line_item"] is None:
                continue
            name = str(r["line_item"])
            key = name.strip().lower()
            if key in known and known[key] != name:
                raise ValueError(f"{li} values '{known[key]}' and '{name}' differ only by case/spaces")
            known[key] = name
            if r["formula_rows"] > 0:
                if r["base_rows"] > 0:
                    raise ValueError(f"{li} '{name}' has both formula and non-formula config rows; "
                                     f"a formula line item must have exactly one config row")
                if r["formula_rows"] > 1:
                    raise ValueError(f"{li} '{name}' has {r['formula_rows']} formula rows; expected exactly one")
                formula_items.add(name)
        if not formula_items:
            logging.info("No formula rows in the config table")
            return []

        rows = []
        q = (f"SELECT {CONFIG_ID_COL} AS cid, {li} AS line_item, TRIM({formula_col}) AS formula, "
             f"{_is_output_expr(output_col)} AS is_output "
             f"FROM `{CONFIG_TABLE}` WHERE NOT {_not_formula(formula_col)}")
        for r in client.query(q).result():
            where = f"{CONFIG_ID_COL} {r['cid']} ({r['line_item']}): formula '{r['formula']}'"
            try:
                ast = _parse_formula(_tokenize_formula(r["formula"], known))
            except ValueError as e:
                raise ValueError(f"{where}: {e}")
            refs = sorted(_formula_refs(ast))
            if not refs:
                raise ValueError(f"{where}: must reference at least one line item")
            if str(r["line_item"]) in refs:
                raise ValueError(f"{where}: a formula cannot reference its own line item")
            rows.append({"cid": r["cid"], "line_item": str(r["line_item"]), "formula": r["formula"],
                         "ast": ast, "refs": refs, "is_output": bool(r["is_output"])})

        by_item = {r["line_item"]: r for r in rows}
        depth = {}

        def visit(item, path):
            if item in depth:
                return depth[item]
            if item in path:
                raise ValueError("circular formula: " + " -> ".join(path + [item]))
            d = 1
            for ref in by_item[item]["refs"]:
                if ref in by_item:
                    d = max(d, visit(ref, path + [item]) + 1)
            depth[item] = d
            return d

        for item in by_item:
            visit(item, [])
        layers = {}
        for item, d in depth.items():
            layers.setdefault(d, []).append(by_item[item])
        return [sorted(layers[d], key=lambda r: r["line_item"]) for d in sorted(layers)]


    # ---------------------------------------------------------------------------
    # Step 4: build SQL
    # ---------------------------------------------------------------------------
    def build_ctes(join_flags, hier_cols, dimension_map=None, formula_col=None, output_col=None):
        """Returns a list of (cte_name, cte_sql): Config, <dim>_mapping ..., actuals, base_values."""
        if dimension_map is None:
            dimension_map = DEFAULT_DIMENSION_MAP
        joined = [m for m in dimension_map if join_flags[m["config_col"]]]
        ctes = []

        # Config: formula rows are left out (they have no dimension filters);
        # wildcard values (ALL / NA / blank / NULL) are normalised to 'ALL'
        base_cols = list(dict.fromkeys([LINE_ITEM_COL] + CONFIG_PASSTHROUGH_COLS))
        cfg_cols = [f"    {CONFIG_ID_COL}"] + [f"    {c}" for c in base_cols]
        cfg_cols.append(f"    {_is_output_expr(output_col)} AS IsOutput")
        for m in joined:
            col, ncol = m["config_col"], hier_cols[m["config_col"]]
            cfg_cols.append(f"    IF({_is_wild(col)}, 'ALL', TRIM({col})) AS {col}")
            if ncol:
                cfg_cols.append(f"    NULLIF(TRIM({ncol}), '') AS {ncol}")
        ctes.append(("Config", "Config AS (\n  SELECT\n" + ",\n".join(cfg_cols) +
                     f"\n  FROM `{CONFIG_TABLE}`\n  WHERE {_not_formula(formula_col)}\n)"))

        # One mapping CTE per dimension: Config_ID -> granular value(s).
        # A node with children -> its children. A leaf (no children) -> the config value itself.
        for m in joined:
            col, dim, ncol = m["config_col"], m["dimension"], hier_cols[m["config_col"]]
            ctes.append((_map_name(m), f"""{_map_name(m)} AS (
      SELECT DISTINCT C.{CONFIG_ID_COL}, COALESCE(H.{HIER_CHILD_COL}, C.{col}) AS {_gran(m)}
      FROM Config C
      LEFT JOIN `{HIERARCHY_TABLE}` H
        ON C.{col} = H.{HIER_PARENT_COL}
        AND H.{HIER_DIM_COL} = '{dim}'
        AND ({_hier_cond("H", "C", ncol)})
      WHERE C.{col} <> 'ALL'
    )"""))

        act_cols = ACTUALS_BASE_COLS + [m["txn_col"] for m in joined]
        ctes.append(("actuals", "actuals AS (\n  SELECT " + ", ".join(act_cols) +
                     f"\n  FROM `{TRANSACTION_TABLE}`\n)"))

        # base_values: line items calculated from actuals (formula blank).
        # per dimension: wildcard OR the actual's value is one of the config row's granular values
        where_parts = [
            f"(C.{m['config_col']} = 'ALL'\n"
            f"    OR EXISTS (SELECT 1 FROM {_map_name(m)} M\n"
            f"      WHERE M.{CONFIG_ID_COL} = C.{CONFIG_ID_COL}\n"
            f"      AND M.{_gran(m)} = A.{m['txn_col']}))"
            for m in joined]
        base_sql = (f"base_values AS (\n"
                    f"  SELECT C.{LINE_ITEM_COL}, C.IsOutput, A.{ACTUALS_YEAR_COL} AS {OUT_YEAR}, "
                    f"A.{ACTUALS_PERIOD_COL} AS {OUT_PERIOD}, SUM(A.{ACTUALS_AMOUNT_COL}) AS {OUT_AMOUNT}\n"
                    f"  FROM actuals A\n"
                    f"  CROSS JOIN Config C")
        if where_parts:
            base_sql += "\n  WHERE\n  " + "\n  AND ".join(where_parts)
        base_sql += (f"\n  GROUP BY C.{LINE_ITEM_COL}, C.IsOutput, A.{ACTUALS_YEAR_COL}, A.{ACTUALS_PERIOD_COL}\n)")
        ctes.append(("base_values", base_sql))

        return ctes


    def build_base_sql(join_flags, hier_cols, dimension_map=None, formula_col=None, output_col=None):
        """SQL that fetches ONLY the non-calculated values (formula rows are calculated in Python):
           one row per line item / IsOutput / year / month."""
        ctes = build_ctes(join_flags, hier_cols, dimension_map, formula_col, output_col)
        return ("WITH " + ",\n".join(sql for _, sql in ctes) + "\n"
                f"SELECT {LINE_ITEM_COL}, IsOutput, {OUT_YEAR}, {OUT_PERIOD}, {OUT_AMOUNT}\n"
                f"FROM base_values")


    # ---------------------------------------------------------------------------
    # Orchestration: inspect config -> plan -> SQL (the local test runner uses these too)
    # ---------------------------------------------------------------------------
    def prepare_plan(client):
        config_cols = get_config_columns(client)
        formula_col = config_cols.get(CONFIG_FORMULA_COL.lower())
        output_col = config_cols.get(CONFIG_OUTPUT_COL.lower())
        logging.info("Formula column: %s | Output column: %s", formula_col or "not in config",
                     output_col or "not in config (all rows are output)")

        dimension_map = get_dimension_map(client)
        join_flags = detect_dimensions_to_join(client, dimension_map, formula_col)
        hier_cols = detect_hier_name_cols(client, dimension_map, config_cols)
        validate_inputs(client, join_flags, hier_cols, dimension_map, formula_col)
        layers = plan_formulas(client, formula_col, output_col)
        return {"dimension_map": dimension_map, "join_flags": join_flags, "hier_cols": hier_cols,
                "formula_col": formula_col, "output_col": output_col, "layers": layers}


    def render_ctes(plan):
        return build_ctes(plan["join_flags"], plan["hier_cols"], plan["dimension_map"],
                          plan["formula_col"], plan["output_col"])


    def render_sql(plan):
        return build_base_sql(plan["join_flags"], plan["hier_cols"], plan["dimension_map"],
                              plan["formula_col"], plan["output_col"])


    def build_sql(client):
        return render_sql(prepare_plan(client))


    def _check_settings():
        """Fail with a clear message when the DAG config json is missing table settings."""
        bad = [name for name, value in (("CONFIG_TABLE", CONFIG_TABLE), ("HIERARCHY_TABLE", HIERARCHY_TABLE),
                                        ("TRANSACTION_TABLE", TRANSACTION_TABLE), ("OUTPUT_TABLE", OUTPUT_TABLE),
                                        ("STAGING_TABLE", STAGING_TABLE))
               if len(str(value).split(".")) != 3 or "None" in str(value).split(".")]
        if bad:
            raise ValueError(f"{bad} are not project.dataset.table - check the *_dataset / *_table_name keys "
                             f"in the DAG config json of {DAG_NAME}")


    def generate_sql(cfg=None, **context):
        """PythonOperator callable: inspect config, validate, generate SQL, return via XCom."""
        if cfg:
            # Tasks run after the whole file has been parsed, when the module-level names below hold the
            # values of the LAST entry in dag_configs. Re-apply the settings of THIS DAG before building SQL.
            globals().update(cfg)
        _check_settings()

        hook = BigQueryHook(gcp_conn_id=bq_conn_id, use_legacy_sql=False)
        client = hook.get_client(project_id=PROJECT_ID_CURATE)

        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        client._http.verify = False

        sql = build_sql(client)
        logging.info("Generated SQL:\n%s", sql)
        return sql

    # ---------------------------------------------------------------------------
    # Step 5 (Python): read the non-calculated values, calculate formula rows, write the output table
    # ---------------------------------------------------------------------------
    def _dec(value):
        """Amount as Decimal; None counts as 0."""
        if value is None:
            return Decimal(0)
        return value if isinstance(value, Decimal) else Decimal(str(value))


    def read_base_rows(client):
        """Non-calculated values written by the SQL step (the staging table):
        dicts {line_item, is_output, year, month, amount}."""
        q = (f"SELECT {LINE_ITEM_COL} AS line_item, IsOutput AS is_output, {OUT_YEAR} AS year, "
             f"{OUT_PERIOD} AS month, {OUT_AMOUNT} AS amount FROM `{STAGING_TABLE}`")
        return [{k: r[k] for k in ("line_item", "is_output", "year", "month", "amount")}
                for r in client.query(q).result()]


    def get_staging_types(client):
        """BigQuery types of the staging table's year / month / amount columns."""
        types = {f.name.lower(): f.field_type for f in client.get_table(STAGING_TABLE).schema}
        return {"year": types.get(OUT_YEAR.lower(), "INTEGER"),
                "month": types.get(OUT_PERIOD.lower(), "INTEGER"),
                "amount": types.get(OUT_AMOUNT.lower(), "FLOAT")}


    def calculate_values(base_rows, layers):
        """Adds the formula rows to the base rows. A formula is evaluated per (year, month) from the
        TOTAL of each line item it references (all its config rows, intermediate ones included).
        A missing value counts as 0; a period where none of the referenced line items has a row gets no row.
        Layers are in dependency order, so a formula can use the result of an earlier formula."""
        rows = [dict(r) for r in base_rows]
        totals = {}                                              # line item -> {(year, month): Decimal}

        def add(line_item, period, amount):
            per = totals.setdefault(line_item, {})
            per[period] = per.get(period, Decimal(0)) + _dec(amount)

        for r in rows:
            add(r["line_item"], (r["year"], r["month"]), r["amount"])

        with localcontext() as ctx:
            ctx.prec = 50
            for layer in layers:
                new_rows = []
                for f in layer:
                    periods = set()
                    for ref in f["refs"]:
                        periods |= set(totals.get(ref, {}))
                    for period in sorted(periods):
                        env = {ref: totals.get(ref, {}).get(period, Decimal(0)) for ref in f["refs"]}
                        new_rows.append({"line_item": f["line_item"], "is_output": f["is_output"],
                                         "year": period[0], "month": period[1],
                                         "amount": _eval_formula(f["ast"], env)})
                for r in new_rows:                               # formulas in one layer do not depend on each other
                    add(r["line_item"], (r["year"], r["month"]), r["amount"])
                rows.extend(new_rows)
        return rows


    def aggregate_output(rows):
        """Keeps the rows that belong in the output (IsOutput) and sums them per line item, year and month."""
        agg = {}
        for r in rows:
            if not r["is_output"]:
                continue
            key = (r["line_item"], r["year"], r["month"])
            amount = None if r["amount"] is None else _dec(r["amount"])
            if key not in agg:
                agg[key] = amount
            elif amount is not None:
                agg[key] = amount if agg[key] is None else agg[key] + amount
        return [{"line_item": k[0], "year": k[1], "month": k[2], "amount": v}
                for k, v in sorted(agg.items(), key=lambda kv: (str(kv[0][0]), kv[0][1], kv[0][2]))]


    def build_output_rows(final_rows, types):
        """(schema, rows) for the load job: schema is [(column, BigQuery type)], rows are JSON-serialisable.
        Amount is NUMERIC when the staging table's amount is NUMERIC / BIGNUMERIC, otherwise FLOAT."""
        amount_type = types["amount"] if types["amount"] in ("NUMERIC", "BIGNUMERIC") else "FLOAT"
        schema = [(LINE_ITEM_COL, "STRING"), (OUT_YEAR, types["year"]), (OUT_PERIOD, types["month"]),
                  (OUT_AMOUNT, amount_type)]
        rows = []
        with localcontext() as ctx:
            ctx.prec = 50
            scale = Decimal(1).scaleb(-AMOUNT_SCALE)
            for r in final_rows:
                a = r["amount"]
                if a is not None:
                    if amount_type == "FLOAT":
                        a = float(a)
                    else:
                        a = a.quantize(scale, rounding=ROUND_HALF_UP)
                        a = format(abs(a) if a == 0 else a, "f")        # no "-0.000000000"
                rows.append({LINE_ITEM_COL: r["line_item"], OUT_YEAR: r["year"], OUT_PERIOD: r["month"],
                             OUT_AMOUNT: a})
        return schema, rows


    def write_output(client, table, schema, rows):
        """Replaces `table` with `rows` in one load job."""
        from google.cloud import bigquery
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(name, bq_type) for name, bq_type in schema],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        client.load_table_from_json(rows, table, job_config=job_config).result()


    def calculate_and_write_with_client(client):
        """The whole Python step: staging table -> formula calculation -> output table."""
        config_cols = get_config_columns(client)
        layers = plan_formulas(client, config_cols.get(CONFIG_FORMULA_COL.lower()),
                               config_cols.get(CONFIG_OUTPUT_COL.lower()))
        base_rows = read_base_rows(client)
        all_rows = calculate_values(base_rows, layers)
        final_rows = aggregate_output(all_rows)
        schema, out_rows = build_output_rows(final_rows, get_staging_types(client))
        logging.info("%s non-calculated rows + %s formula rows -> %s output rows",
                     len(base_rows), len(all_rows) - len(base_rows), len(out_rows))
        write_output(client, OUTPUT_TABLE, schema, out_rows)
        if not KEEP_STAGING_TABLE:
            client.delete_table(STAGING_TABLE, not_found_ok=True)
        return len(out_rows)


    def calculate_and_write(cfg=None, **context):
        """PythonOperator callable for the Python step."""
        if cfg:
            globals().update(cfg)                                # this DAG's own settings (see generate_sql)
        _check_settings()
        hook = BigQueryHook(gcp_conn_id=bq_conn_id, use_legacy_sql=False)
        client = hook.get_client(project_id=PROJECT_ID_CURATE)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        client._http.verify = False
        return calculate_and_write_with_client(client)


    # ---------------------------------------------------------------------------
    # Default arguments
    # ---------------------------------------------------------------------------
    default_args = {
        'start_date': days_ago(0),
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
        'dag_start_datetime': get_dag_start_date(),
        'on_failure_callback': task_failure_log,
        'on_success_callback': success_log
    }

    # ---------------------------------------------------------------------------
    # DAG definition
    # ---------------------------------------------------------------------------
    # This DAG's own settings, handed to the python tasks (see generate_sql)
    DAG_CFG = {
        "DAG_NAME": DAG_NAME,
        "bq_conn_id": bq_conn_id,
        "CONFIG_TABLE": CONFIG_TABLE,
        "HIERARCHY_TABLE": HIERARCHY_TABLE,
        "TRANSACTION_TABLE": TRANSACTION_TABLE,
        "OUTPUT_TABLE": OUTPUT_TABLE,
        "STAGING_TABLE": STAGING_TABLE,
        "DIMENSION_MAP_TABLE": DIMENSION_MAP_TABLE,
    }

    with DAG(dag_id=DAG_NAME,
             catchup=False,
             schedule_interval=schedule_int,
             max_active_runs=1,
             default_args=default_args,
             tags=[DAG_NAME, "epm"],
             params={
                 "PROJECT_ID_CURATE": PROJECT_ID_CURATE,
                 "DAG_NAME": DAG_NAME,
                 "OPERATIONAL_DATASET": OPERATIONAL_DATASET,
                 "RESOURCE_SA": RESOURCE_SA,
             }
             ) as dag:

        # 1. build the SQL for the non-calculated values (also validates config and formulas)
        generate_flatten_sql = PythonOperator(
            task_id='generate_flatten_sql',
            python_callable=generate_sql,
            op_kwargs={"cfg": DAG_CFG},
            provide_context=True,
            dag=dag
        )

        # 2. run it: only the NON-calculated values, written to the staging table
        fetch_base_values = BigQueryInsertJobOperator(
            task_id='fetch_base_values',
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='generate_flatten_sql') }}",
                    "useLegacySql": False,
                    "allow_large_results": True,
                    "destinationTable": {
                        "projectId": STAGING_TABLE.split(".")[0],
                        "datasetId": STAGING_TABLE.split(".")[1],
                        "tableId": STAGING_TABLE.split(".")[2],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            gcp_conn_id=bq_conn_id,
            trigger_rule='none_failed_or_skipped',
            dag=dag
        )

        # 3. Python: calculate the formula rows, apply Output, write the output table
        calculate_and_write_output = PythonOperator(
            task_id='calculate_and_write_output',
            python_callable=calculate_and_write,
            op_kwargs={"cfg": DAG_CFG},
            provide_context=True,
            dag=dag
        )

        generate_flatten_sql >> fetch_base_values >> calculate_and_write_output

    # Register this DAG under its own name so that every entry of dag_configs is picked up
    globals()[DAG_NAME] = dag
