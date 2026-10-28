"""
EPM Challenger Model driver DAG: compiles the config table's formulas into BigQuery SQL.

One config-driven DAG instead of the driver_line_item view -> view_to_table DAG -> driver_line_item_all.sql chain:
  generate_sql_query       reads the config table, checks it and builds two SQL statements (returned via XCom)
  materialize_base_values  base_sql: line items WITHOUT a formula, fetched from actuals through the hierarchy
                           (the generated driver_line_item.sql) -> STAGING_TABLE
  execute_compiled_sql     formula_sql: line items WITH a formula, calculated tier by tier from STAGING_TABLE
                           (the generated driver_line_item_all.sql) -> OUTPUT_TABLE
All the maths runs in BigQuery; Python only reads config metadata.

Formula column (blank = the line item is fetched from actuals through its dimension columns)
  [Line Item]                 another line item; brackets required, case-insensitive
  + - * / ( )  numbers        maths within a month; a missing value counts as 0 and x / 0 = 0
  SAFE_DIVIDE(a, b)           a / b, 0 when b = 0
  LAG(x, n [, default])       x n months earlier; default (else 0) for the first n months
  ROLLING_AVG(x, n)           average of this month and the n-1 months before it (fewer at the start)
  ROLLING_SUM(x, n)           sum of this month and the n-1 months before it
  ROLL_FORWARD(connections=[this line item], net_adds=[A], adj=[B], actual=[C])
                              this(t) = this(t-1) + A(t) + B(t), starting from 0. connections=, adj= and actual= are
                              optional; connections= must name the formula's own line item. With actual=, the roll
                              forward restarts from the latest earlier month in which C has a (non-zero) value.
  Rules: a formula cannot reference itself, directly or through other formulas (connections= excepted);
         LAG / ROLLING_* cannot be nested inside each other; ROLL_FORWARD is a whole formula and takes [line items];
         a formula line item has exactly one config row and is never fetched from actuals.
  e.g.   [Consumer - Phone Gross Adds] - [Consumer - Phone Disconnects]
         SAFE_DIVIDE([Consumer - Phone Disconnects], [Consumer - Average Phone Connections])
         [Consumer - Phone Connections] - LAG([Consumer - Phone Connections], 1)
FinalOutput column: N / NO / FALSE / 0 -> intermediate line item, left out of OUTPUT_TABLE; blank or anything else -> output.
"""
import difflib
import logging
import os
import re
import sys
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from py.common_helper import get_dag_start_date, read_json
from py.get_env_variable import get_curate_project_id, get_enrich_project_id, get_resource_sa, get_raw_project_id
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

# One DAG per entry. Each reads dags/<PROJECT_SPACE>/<name>/config/<name>_config.json, written by the config DAG.
dag_configs = ["epm_challenger_model_flatten_enrich_driver"]

# ---------------------------------------------------------------------------
# Hierarchy view columns (edm_master_incl_alt_hierarchies_v)
# ---------------------------------------------------------------------------
HIER_PARENT_COL = "parent"
HIER_CHILD_COL = "node"
HIER_DIM_COL = "dimension_name"
HIER_PRIMARY_COL = "is_primary"      # TRUE = primary hierarchy
HIER_NAME_COL = "hierarchy_name"     # TODO: confirm - name of the hierarchy (master or any alternate)
# Config column holding the hierarchy name of a dimension: the map's hier_name_col, else <config_col> + this suffix.
# The column is OPTIONAL: if it is not in the config table, that dimension uses is_primary = TRUE.
HIER_NAME_SUFFIX = "HierarchyName"

# ---------------------------------------------------------------------------
# Config / actuals / output
# ---------------------------------------------------------------------------
CONFIG_ID_COL = "Config_ID"             # must be unique in the config table
LINE_ITEM_COL = "LineItem"              # line item name (also used in formulas)
CONFIG_FORMULA_COL = "Formula"          # blank -> fetched from actuals; filled -> calculated (see the module docstring)
CONFIG_OUTPUT_COL = "FinalOutput"       # N / NO / FALSE / 0 -> intermediate line item, left out of the output
OUTPUT_NO_VALUES = ("N", "NO", "FALSE", "0")

# Values that mean "wildcard": no flattening and no filter on that dimension (normalised to 'ALL' in the Config CTE).
PASSTHROUGH_VALUES = ("ALL", "NA")

ACTUALS_YEAR_COL = "year"
ACTUALS_PERIOD_COL = "period"
ACTUALS_AMOUNT_COL = "amount"

ACTUALS_YEARS_BACK = 5          # fiscal years read from actuals, counting the current one; None = all years
YEAR_LABEL_FORMAT = "FY%y"      # FORMAT_DATE pattern of the actuals year labels (FY26)
EXCLUDED_PERIODS = ()           # periods never read from actuals, e.g. ("13",) for an adjustment period
FISCAL_YEAR_START_MONTH = 1     # 1 = fiscal year = calendar year; 4 = FY26 runs Apr 2025 - Mar 2026 (named by end year)

AMOUNT_TYPE = "FLOAT64"         # type the formula maths runs in: FLOAT64, NUMERIC or BIGNUMERIC
RF_ZERO_IS_NO_ACTUAL = True     # ROLL_FORWARD actual=: a 0 counts as "no actual" (forecast months stored as 0)
DRY_RUN = True                  # dry-run the generated SQL in generate_sql_query (fails early, logs bytes processed)
DISABLE_SSL_VERIFY = False      # True only where a proxy breaks TLS verification of the BigQuery API

# ---------------------------------------------------------------------------
# Default field mapping fallback. One entry per dimension.
# hier_name_col: the config column with that dimension's hierarchy name (optional).
# ---------------------------------------------------------------------------
DEFAULT_DIMENSION_MAP = [
    {"config_col": "ProfitCentre",        "dimension": "Profit_Center",        "txn_col": "profit_center",
     "hier_name_col": "ProfitCentreHierarchyName"},
    {"config_col": "Account",             "dimension": "Account",              "txn_col": "Account",
     "hier_name_col": "AccountHierarchy"},
    {"config_col": "CompanyCode",         "dimension": "Company_Code",         "txn_col": "Company_Code"},
    {"config_col": "ServiceType",         "dimension": "Service_Type",         "txn_col": "Service_Type",
     "hier_name_col": "ServiceTypeHierarchy"},
    {"config_col": "Platform",            "dimension": "Platform",             "txn_col": "Platform"},
    {"config_col": "CostCenter",          "dimension": "Cost_Center",          "txn_col": "Cost_Center"},
    {"config_col": "GeoRegion",           "dimension": "Geo_Region",           "txn_col": "Geo_Region"},
    {"config_col": "Market",              "dimension": "Market",               "txn_col": "Market"},
    {"config_col": "ContractTerm",        "dimension": "Contract_Term",        "txn_col": "Contract_Term"},
    {"config_col": "Product",             "dimension": "Product",              "txn_col": "Product",
     "hier_name_col": "ProductHierarchy"},
    {"config_col": "Equipment",           "dimension": "Equipment",            "txn_col": "Equipment",
     "hier_name_col": "EquipmentHierachy"},         # sic: the config table's spelling
    {"config_col": "DistributionChannel", "dimension": "Distribution_Channel", "txn_col": "Distribution_Channel",
     "hier_name_col": "DistributionChannelHierarchy"},
    {"config_col": "Source",              "dimension": "Source",               "txn_col": "Source"},
]

# Attribute alias for external scripts inspecting mod.DIMENSION_MAP
DIMENSION_MAP = DEFAULT_DIMENSION_MAP


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _param(params, *keys, default=None):
    """First non-empty value among keys. The config table uses *_bq_table / schedule; older DAGs *_table_name /
    schedule_int."""
    for key in keys:
        value = params.get(key)
        if value not in (None, ""):
            return value
    return default


def _dag_params(raw, dag_name):
    """The json written by the config DAG holds one config row, possibly wrapped in a list."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        if len(raw) == 1:
            return raw[0]
        mine = [p for p in raw if isinstance(p, dict) and p.get("source_file_name") == dag_name]
        if len(mine) == 1:
            return mine[0]
    raise ValueError(f"config json of {dag_name}: expected one config row, got {str(raw)[:200]}")


def _lit(value):
    """SQL string literal."""
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _q(values):
    return ", ".join(_lit(v) for v in values)


def _num(text):
    """Number literal in AMOUNT_TYPE (a FLOAT64 literal would turn NUMERIC maths into FLOAT64)."""
    return text if AMOUNT_TYPE == "FLOAT64" else f"CAST('{text}' AS {AMOUNT_TYPE})"


def _one_line(text):
    return " ".join(str(text).split())


def _row_text(row, col):
    """Config value as trimmed text ('' for NULL). Rows are dicts keyed by lower-case column name."""
    value = row.get(col.lower())
    return "" if value is None else str(value).strip()


def _is_wild_value(value):
    return value is None or str(value).strip() == "" or str(value).strip().upper() in PASSTHROUGH_VALUES


def _is_output_value(value):
    return value is None or str(value).strip().upper() not in OUTPUT_NO_VALUES


def _is_wild(col_ref):
    """SQL predicate: value is NULL / blank / ALL / NA."""
    return (f"({col_ref} IS NULL OR TRIM({col_ref}) = '' "
            f"OR UPPER(TRIM({col_ref})) IN ({_q(PASSTHROUGH_VALUES)}))")


def _not_formula(fcol, alias=""):
    """SQL predicate on the raw config table: the row has NO formula (so it is fetched from actuals)."""
    if not fcol:
        return "TRUE"
    ref = f"{alias}.{fcol}" if alias else fcol
    return f"({ref} IS NULL OR TRIM({ref}) = '')"


def _hier_cond(h, c, ncol):
    """Which rows of the hierarchy view to use for this config row.
    h = hierarchy alias, c = Config CTE alias, ncol = hierarchy name column in the config."""
    if not ncol:
        return f"{h}.{HIER_PRIMARY_COL} = TRUE"
    n = f"{c}.{ncol}"                       # already NULLIF(TRIM(..), '') in the Config CTE
    return (f"CASE WHEN {n} IS NULL THEN {h}.{HIER_PRIMARY_COL} = TRUE "
            f"ELSE {h}.{HIER_NAME_COL} = {n} END")


def _hier_cond_raw(h, c, ncol):
    """_hier_cond on the raw config table (hierarchy name not trimmed yet)."""
    if not ncol:
        return f"{h}.{HIER_PRIMARY_COL} = TRUE"
    n = f"NULLIF(TRIM({c}.{ncol}), '')"
    return (f"CASE WHEN {n} IS NULL THEN {h}.{HIER_PRIMARY_COL} = TRUE "
            f"ELSE {h}.{HIER_NAME_COL} = {n} END")


def _hier_col(m):
    return m.get("hier_name_col") or f"{m['config_col']}{HIER_NAME_SUFFIX}"


def _gran(m):
    return f"{m['config_col']}Granular"


def _map_name(m):
    return f"{m['config_col'].lower()}_mapping"


def _map_alias(m):
    return f"M_{m['config_col']}"


def _sort_key(value):
    text = str(value)
    return (0, int(text), "") if text.isdigit() else (1, 0, text)


# ---------------------------------------------------------------------------
# Config table metadata (BigQuery)
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


def get_config_columns(client):
    """Returns {lower-case column name: actual column name} for the config table."""
    parts = CONFIG_TABLE.split(".")
    prefix = ".".join(parts[:-1])
    schema = f"`{prefix}`.INFORMATION_SCHEMA.COLUMNS" if prefix else "INFORMATION_SCHEMA.COLUMNS"
    query = f"SELECT column_name FROM {schema} WHERE table_name = '{parts[-1]}'"
    logging.info("Config columns query:\n%s", query)
    return {r["column_name"].lower(): r["column_name"] for r in client.query(query).result()}


def load_config_rows(client):
    """All config rows (a few dozen), as dicts keyed by lower-case column name."""
    query = f"SELECT * FROM `{CONFIG_TABLE}` ORDER BY {CONFIG_ID_COL}"
    return [{k.lower(): v for k, v in row.items()} for row in client.query(query).result()]


# ---------------------------------------------------------------------------
# Step 1: dimensions (from the config rows; no BigQuery calls)
# ---------------------------------------------------------------------------
def detect_dimensions_to_join(rows, dimension_map):
    """Per dimension, looking at the base rows (Formula blank):
    use      = at least one row has a real node value (otherwise no mapping and no filter at all);
    required = every row has one, so actuals can be pre-filtered on that dimension before the cross join."""
    base = [r for r in rows if not _row_text(r, CONFIG_FORMULA_COL)]
    flags = {}
    for m in dimension_map:
        nodes = sum(1 for r in base if not _is_wild_value(r.get(m["config_col"].lower())))
        flags[m["config_col"]] = {"use": nodes > 0, "required": 0 < nodes == len(base)}
        logging.info("%s: %s node values on %s base rows -> %s", m["config_col"], nodes, len(base),
                     "SKIP (all ALL/NA)" if not nodes else "USE, pre-filter actuals" if nodes == len(base) else "USE")
    return flags


def detect_hier_name_cols(dimension_map, config_cols):
    """Which hierarchy name columns exist in the config table? Missing column -> is_primary = TRUE."""
    hier_cols = {}
    for m in dimension_map:
        wanted = _hier_col(m)
        hier_cols[m["config_col"]] = config_cols.get(wanted.lower())      # actual column name or None
        logging.info("%s: %s", m["config_col"],
                     f"hierarchy name column {wanted} found" if hier_cols[m["config_col"]]
                     else f"no {wanted} column -> primary hierarchy (is_primary = TRUE)")
    return hier_cols


# ---------------------------------------------------------------------------
# Step 2: the Formula DSL - tokenizer, parser, checks
# ---------------------------------------------------------------------------
class FormulaError(ValueError):
    def __init__(self, message, pos=None):
        super().__init__(message)
        self.pos = pos


_TOKEN_RE = re.compile(r"""
      (?P<ws>\s+)
    | (?P<ref>\[[^\[\]]*\])
    | (?P<num>\d+(?:\.\d+)?)
    | (?P<name>[A-Za-z_]\w*)
    | (?P<op>[-+*/(),=])
""", re.VERBOSE)


def _tokenize_formula(text):
    """Tokens (kind, value, position) with kind ref / num / name / op; a ref's value is the text inside [ ]."""
    tokens, pos = [], 0
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if not m:
            if text[pos] == "[":
                raise FormulaError("missing ']'", pos)
            raise FormulaError(f"cannot read '{text[pos:pos + 20]}' "
                               f"(expected a [line item], a number, a function or + - * / ( ))", pos)
        kind = m.lastgroup
        if kind == "ref":
            name = m.group()[1:-1].strip()
            if not name:
                raise FormulaError("empty []", pos)
            tokens.append(("ref", name, pos))
        elif kind != "ws":
            tokens.append((kind, m.group(), pos))
        pos = m.end()
    return tokens


def _parse_formula(tokens, length):
    """expr := term (('+'|'-') term)* ; term := unary (('*'|'/') unary)* ; unary := ('-'|'+') unary | atom
    atom := number | [line item] | FUNCTION '(' [arg (',' arg)*] ')' | '(' expr ')' ; arg := name '=' expr | expr
    Nodes: ('num', text) ('ref', name, pos) ('neg', node) ('bin', op, left, right) ('call', NAME, args, kwargs, pos)"""
    pos = 0

    def peek(offset=0):
        i = pos + offset
        return tokens[i] if i < len(tokens) else None

    def take():
        nonlocal pos
        tok = tokens[pos]
        pos += 1
        return tok

    def is_op(tok, chars):
        return tok is not None and tok[0] == "op" and tok[1] in chars

    def expect(char):
        tok = peek()
        if not is_op(tok, char):
            raise FormulaError(f"expected '{char}'", tok[2] if tok else length)
        take()

    def expr():
        node = term()
        while is_op(peek(), "+-"):
            op = take()[1]
            node = ("bin", op, node, term())
        return node

    def term():
        node = unary()
        while is_op(peek(), "*/"):
            op = take()[1]
            node = ("bin", op, node, unary())
        return node

    def unary():
        if is_op(peek(), "-"):
            take()
            return ("neg", unary())
        if is_op(peek(), "+"):
            take()
            return unary()
        return atom()

    def atom():
        tok = peek()
        if tok is None:
            raise FormulaError("formula ends unexpectedly", length)
        kind, value, at = tok
        if kind == "num":
            take()
            return ("num", value)
        if kind == "ref":
            take()
            return ("ref", value, at)
        if kind == "name":
            if not is_op(peek(1), "("):
                raise FormulaError(f"unexpected '{value}' - put line item names in [brackets]", at)
            take()
            take()
            return call(value.upper(), at)
        if is_op(tok, "("):
            take()
            node = expr()
            expect(")")
            return node
        raise FormulaError(f"unexpected '{value}'", at)

    def call(name, at):
        args, kwargs = [], {}
        if is_op(peek(), ")"):
            take()
            return ("call", name, args, kwargs, at)
        while True:
            tok = peek()
            if tok is not None and tok[0] == "name" and is_op(peek(1), "="):
                key = take()[1].lower()
                take()
                if key in kwargs:
                    raise FormulaError(f"{key}= is given twice", tok[2])
                kwargs[key] = expr()
            else:
                if kwargs:
                    raise FormulaError("a plain argument cannot follow name=value arguments",
                                       tok[2] if tok else length)
                args.append(expr())
            if is_op(peek(), ","):
                take()
                continue
            expect(")")
            return ("call", name, args, kwargs, at)

    if not tokens:
        raise FormulaError("empty formula", 0)
    node = expr()
    if pos != len(tokens):
        raise FormulaError(f"unexpected '{tokens[pos][1]}'", tokens[pos][2])
    return node


# Formula functions. kind: scalar = within the month, window = looks at other months, rollforward = whole formula.
# args = (min, max) plain arguments; int_args = positions that must be a whole number of months (passed to sql as int).
# sql(args) gets the compiled arguments. Adding a function = adding an entry here.
FUNCTIONS = {
    "SAFE_DIVIDE": {"kind": "scalar", "args": (2, 2),
                    "sql": lambda a: f"SAFE_DIVIDE({a[0]}, {a[1]})"},
    "LAG": {"kind": "window", "args": (2, 3), "int_args": (1,),
            "sql": lambda a: f"COALESCE(LAG({a[0]}, {a[1]}) OVER (ORDER BY month_date), "
                             f"{a[2] if len(a) > 2 else 0})"},
    "ROLLING_AVG": {"kind": "window", "args": (2, 2), "int_args": (1,),
                    "sql": lambda a: f"AVG({a[0]}) OVER (ORDER BY month_date "
                                     f"ROWS BETWEEN {a[1] - 1} PRECEDING AND CURRENT ROW)"},
    "ROLLING_SUM": {"kind": "window", "args": (2, 2), "int_args": (1,),
                    "sql": lambda a: f"SUM({a[0]}) OVER (ORDER BY month_date "
                                     f"ROWS BETWEEN {a[1] - 1} PRECEDING AND CURRENT ROW)"},
    "ROLL_FORWARD": {"kind": "rollforward", "kwargs": ("connections", "net_adds", "adj", "actual"),
                     "required": ("net_adds",)},
}


def _resolve(node, known, top=True, in_window=False):
    """Checks a parsed formula and replaces each [name] with the config spelling of the line item.
    known: {lower-case line item: line item}. ROLL_FORWARD becomes ('rollforward', {argument: line item})."""
    kind = node[0]
    if kind == "num":
        return node
    if kind == "ref":
        key = node[1].lower()
        if key not in known:
            close = difflib.get_close_matches(key, list(known), n=3, cutoff=0.6)
            hint = (" - did you mean " + " or ".join(f"[{known[c]}]" for c in close) + "?") if close else ""
            raise FormulaError(f"unknown line item [{node[1]}]{hint}", node[2])
        return ("ref", known[key])
    if kind == "neg":
        return ("neg", _resolve(node[1], known, False, in_window))
    if kind == "bin":
        return ("bin", node[1], _resolve(node[2], known, False, in_window),
                _resolve(node[3], known, False, in_window))

    _, name, args, kwargs, at = node
    spec = FUNCTIONS.get(name)
    if spec is None:
        raise FormulaError(f"unknown function {name} (known: {', '.join(sorted(FUNCTIONS))})", at)

    if spec["kind"] == "rollforward":
        if not top:
            raise FormulaError(f"{name} must be the whole formula; reference its line item from other formulas", at)
        if args:
            raise FormulaError(f"{name} takes name=value arguments only: "
                               f"{', '.join(k + '=[...]' for k in spec['kwargs'])}", at)
        unknown = sorted(set(kwargs) - set(spec["kwargs"]))
        if unknown:
            raise FormulaError(f"{name}: unknown argument {', '.join(unknown)} "
                               f"(known: {', '.join(spec['kwargs'])})", at)
        missing = [k for k in spec["required"] if k not in kwargs]
        if missing:
            raise FormulaError(f"{name}: {', '.join(missing)}= is required", at)
        resolved = {}
        for key, value in kwargs.items():
            if value[0] != "ref":
                raise FormulaError(f"{name}: {key}= must be a single [line item] "
                                   f"(define anything else as its own line item)", at)
            resolved[key] = _resolve(value, known)[1]
        return ("rollforward", resolved)

    if kwargs:
        raise FormulaError(f"{name} does not take name=value arguments", at)
    low, high = spec["args"]
    if not low <= len(args) <= high:
        expected = str(low) if low == high else f"{low} or {high}"
        raise FormulaError(f"{name} takes {expected} arguments, got {len(args)}", at)
    window = spec["kind"] == "window"
    if window and in_window:
        raise FormulaError(f"{name} cannot be used inside another LAG / ROLLING_*; "
                           f"make the inner part its own line item", at)
    resolved = []
    for i, arg in enumerate(args):
        if i in spec.get("int_args", ()):
            if arg[0] != "num" or not arg[1].isdigit() or int(arg[1]) < 1:
                raise FormulaError(f"{name}: argument {i + 1} must be a whole number of months (1 or more)", at)
            resolved.append(("int", int(arg[1])))
        else:
            resolved.append(_resolve(arg, known, False, in_window or window))
    return ("call", name, resolved)


def _formula_refs(node):
    """Line items a resolved formula depends on (ROLL_FORWARD connections= is the line item itself: not a dependency)."""
    kind = node[0]
    if kind == "ref":
        return {node[1]}
    if kind in ("num", "int"):
        return set()
    if kind == "neg":
        return _formula_refs(node[1])
    if kind == "bin":
        return _formula_refs(node[2]) | _formula_refs(node[3])
    if kind == "call":
        return set().union(*(_formula_refs(a) for a in node[2]))
    return {item for key, item in node[1].items() if key != "connections"}


def plan_formulas(rows):
    """Reads the line items from the config rows (dicts keyed by lower-case column name), parses and checks every
    formula and puts the formula line items in tiers: tier k only uses base line items and tiers before k.
    Raises one ValueError listing every problem found."""
    cid_key = CONFIG_ID_COL.lower()
    errors = []

    known, by_name = {}, {}
    for row in rows:
        name = _row_text(row, LINE_ITEM_COL)
        if not name:
            errors.append(f"{CONFIG_ID_COL} {row.get(cid_key)}: {LINE_ITEM_COL} is empty")
            continue
        key = name.lower()
        if key in known and known[key] != name:
            errors.append(f"{LINE_ITEM_COL} values '{known[key]}' and '{name}' differ only by case")
            continue
        known[key] = name
        by_name.setdefault(name, []).append(row)

    items = {}
    for name, item_rows in by_name.items():
        formula_rows = [r for r in item_rows if _row_text(r, CONFIG_FORMULA_COL)]
        outputs = {_is_output_value(r.get(CONFIG_OUTPUT_COL.lower())) for r in item_rows}
        if len(outputs) > 1:
            errors.append(f"{LINE_ITEM_COL} '{name}': {CONFIG_OUTPUT_COL} differs between its config rows")
        if formula_rows and len(formula_rows) < len(item_rows):
            errors.append(f"{LINE_ITEM_COL} '{name}' has both formula and non-formula config rows; "
                          f"a formula line item is never fetched from actuals")
        elif len(formula_rows) > 1:
            errors.append(f"{LINE_ITEM_COL} '{name}' has {len(formula_rows)} formula rows; expected exactly one")
        row = formula_rows[0] if formula_rows else item_rows[0]
        items[name] = {"name": name, "cid": row.get(cid_key), "is_output": all(outputs),
                       "formula": _row_text(row, CONFIG_FORMULA_COL), "kind": "base", "deps": set()}

    for item in items.values():
        if not item["formula"]:
            continue
        try:
            ast = _resolve(_parse_formula(_tokenize_formula(item["formula"]), len(item["formula"])), known)
            if ast[0] == "rollforward":
                if ast[1].get("connections", item["name"]) != item["name"]:
                    raise FormulaError(f"ROLL_FORWARD connections= must be the line item itself, [{item['name']}]")
                if ast[1].get("actual") == item["name"]:
                    raise FormulaError("ROLL_FORWARD actual= must be another line item (a formula line item is "
                                       "never in actuals) - e.g. a base row with the reported value")
            deps = _formula_refs(ast)
            if item["name"] in deps:
                raise FormulaError("a formula cannot reference its own line item")
        except FormulaError as e:
            at = f" (at character {e.pos + 1})" if e.pos is not None else ""
            errors.append(f"{CONFIG_ID_COL} {item['cid']} ({item['name']}): formula '{item['formula']}': {e}{at}")
            continue
        item.update(kind="rollforward" if ast[0] == "rollforward" else "formula", ast=ast, deps=deps)
        if ast[0] == "rollforward":
            item["rf"] = ast[1]
    if errors:
        raise ValueError("Config problems:\n  " + "\n  ".join(errors))

    depth = {}

    def visit(name, path):
        if name in depth:
            return depth[name]
        if name in path:
            raise ValueError("circular formula: " + " -> ".join(path[path.index(name):] + [name]))
        item = items[name]
        d = 0 if item["kind"] == "base" else 1 + max((visit(dep, path + [name]) for dep in item["deps"]), default=0)
        depth[name] = d
        return d

    for name in items:
        visit(name, [])

    tiers, columns = {}, set()
    for name, d in depth.items():
        if d:
            item = items[name]
            item["tier"] = d
            item["col"] = "f_" + re.sub(r"\W", "_", str(item["cid"]))
            if item["col"] in columns:
                raise ValueError(f"{CONFIG_ID_COL} {item['cid']} gives a duplicate column name {item['col']}")
            columns.add(item["col"])
            tiers.setdefault(d, []).append(item)

    used = set().union(*(item["deps"] for item in items.values()))
    unused = [n for n, it in items.items() if it["kind"] == "base" and not it["is_output"] and n not in used]
    if unused:
        logging.warning("Base line items that are neither output nor used by a formula: %s", unused)

    return {"items": items,
            "tiers": [sorted(tiers[d], key=lambda it: _sort_key(it["cid"])) for d in sorted(tiers)],
            "output_items": sorted((n for n, it in items.items() if it["is_output"]),
                                   key=lambda n: _sort_key(items[n]["cid"]))}


def prepare_plan(rows, config_cols, dimension_map):
    """Everything the SQL builders need, from the config rows alone (no BigQuery calls - also runs on a CSV export).
    config_cols: {lower-case column name: actual column name} of the config table."""
    missing = [c for c in (CONFIG_ID_COL, LINE_ITEM_COL) if c.lower() not in config_cols]
    if missing:
        raise ValueError(f"config table has no {missing} column")
    seen, dups = set(), set()
    for row in rows:
        cid = str(row.get(CONFIG_ID_COL.lower()))
        (dups if cid in seen else seen).add(cid)
    if dups:
        raise ValueError(f"{CONFIG_ID_COL} must be unique in the config table; duplicates: {sorted(dups)[:5]}")

    plan = plan_formulas(rows)
    plan.update(dimension_map=dimension_map,
                dim_flags=detect_dimensions_to_join(rows, dimension_map),
                hier_cols=detect_hier_name_cols(dimension_map, config_cols),
                formula_col=config_cols.get(CONFIG_FORMULA_COL.lower()))
    return plan


def describe_plan(plan):
    base = [n for n, it in plan["items"].items() if it["kind"] == "base"]
    lines = [f"{len(base)} base line items (fetched from actuals), "
             f"{len(plan['output_items'])} line items in the output"]
    for k, tier in enumerate(plan["tiers"], start=1):
        lines.append(f"Tier {k}: {len(tier)} line items")
        lines += [f"  {it['name']} = {_one_line(it['formula'])}" for it in tier]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Step 2b: hierarchy checks (BigQuery; fail early instead of silently mis-mapping)
# ---------------------------------------------------------------------------
def adjacency_check_sql(plan):
    """Config values whose deeper nodes are reached only through their children. The mapping joins
    parent = config value once, so this means the hierarchy view holds direct parent -> child rows only and
    actuals booked on those deeper nodes are missed. Joins only (no correlated subquery). None when no dimension
    is used."""
    joined = [m for m in plan["dimension_map"] if plan["dim_flags"][m["config_col"]]["use"]]
    parts = []
    for m in joined:
        col, dim, ncol = m["config_col"], m["dimension"], plan["hier_cols"][m["config_col"]]
        parts.append(
            f"SELECT '{dim}' AS dim, TRIM(c.{col}) AS value, h2.{HIER_CHILD_COL} AS deeper_node "
            f"FROM `{CONFIG_TABLE}` c "
            f"JOIN `{HIERARCHY_TABLE}` h1 ON h1.{HIER_DIM_COL} = '{dim}' "
            f"AND h1.{HIER_PARENT_COL} = TRIM(c.{col}) AND ({_hier_cond_raw('h1', 'c', ncol)}) "
            f"JOIN `{HIERARCHY_TABLE}` h2 ON h2.{HIER_DIM_COL} = '{dim}' "
            f"AND h2.{HIER_PARENT_COL} = h1.{HIER_CHILD_COL} AND ({_hier_cond_raw('h2', 'c', ncol)}) "
            f"LEFT JOIN `{HIERARCHY_TABLE}` h3 ON h3.{HIER_DIM_COL} = '{dim}' "
            f"AND h3.{HIER_PARENT_COL} = TRIM(c.{col}) AND h3.{HIER_CHILD_COL} = h2.{HIER_CHILD_COL} "
            f"AND ({_hier_cond_raw('h3', 'c', ncol)}) "
            f"WHERE {_not_formula(plan['formula_col'], 'c')} AND NOT {_is_wild('c.' + col)} "
            f"AND h3.{HIER_CHILD_COL} IS NULL")
    if not parts:
        return None
    return ("SELECT dim, value, COUNT(DISTINCT deeper_node) AS deeper_nodes FROM (\n"
            + "\nUNION ALL\n".join(parts) + "\n) GROUP BY dim, value ORDER BY dim, value LIMIT 50")


def validate_hierarchy(client, plan):
    joined = [m for m in plan["dimension_map"] if plan["dim_flags"][m["config_col"]]["use"]]
    if not joined:
        return
    formula_col, hier_cols = plan["formula_col"], plan["hier_cols"]

    # a. dimension_name spelling must exist in the hierarchy view
    rows = client.query(f"SELECT DISTINCT {HIER_DIM_COL} AS d FROM `{HIERARCHY_TABLE}`").result()
    existing = {r["d"] for r in rows}
    missing = [m["dimension"] for m in joined if m["dimension"] not in existing]
    if missing:
        raise ValueError(f"dimension_name not found in hierarchy view: {missing}. "
                         f"Available: {sorted(x for x in existing if x)}")

    # b. hierarchy names in config must exist for that dimension in the hierarchy view
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

    # c. warn about config nodes that exist nowhere in the hierarchy (likely typos);
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

    # d. warn when a config node reaches deeper nodes only through its children (parent -> child view)
    try:
        deep = [(r["dim"], r["value"], r["deeper_nodes"]) for r in client.query(adjacency_check_sql(plan)).result()]
    except Exception as e:      # a warning only: never block the run on it
        logging.warning("Could not check the hierarchy depth: %s", e)
        deep = []
    if deep:
        logging.warning("Config values that map to their direct children only - actuals on these deeper nodes "
                        "are NOT counted (dimension, value, deeper nodes): %s", deep)


# ---------------------------------------------------------------------------
# Step 3: SQL
# ---------------------------------------------------------------------------
def build_base_sql(plan):
    """Base line items (Formula blank) fetched from actuals: one row per line item / year / month.
    The generated, config-driven driver_line_item.sql; written to STAGING_TABLE."""
    flags, hier_cols = plan["dim_flags"], plan["hier_cols"]
    joined = [m for m in plan["dimension_map"] if flags[m["config_col"]]["use"]]
    ctes = []

    # Config: formula rows are left out (they have no dimension filters);
    # wildcard values (ALL / NA / blank / NULL) are normalised to 'ALL'
    cfg_cols = [f"    {CONFIG_ID_COL}", f"    TRIM({LINE_ITEM_COL}) AS {LINE_ITEM_COL}"]
    for m in joined:
        col, ncol = m["config_col"], hier_cols[m["config_col"]]
        cfg_cols.append(f"    IF({_is_wild(col)}, 'ALL', TRIM({col})) AS {col}")
        if ncol:
            cfg_cols.append(f"    NULLIF(TRIM({ncol}), '') AS {ncol}")
    ctes.append("Config AS (\n  SELECT\n" + ",\n".join(cfg_cols) +
                f"\n  FROM `{CONFIG_TABLE}`\n  WHERE {_not_formula(plan['formula_col'])}\n)")

    # One mapping CTE per dimension: Config_ID -> granular value(s).
    # A node with children -> its children. A leaf (no children) -> the config value itself.
    for m in joined:
        col, dim, ncol = m["config_col"], m["dimension"], hier_cols[m["config_col"]]
        ctes.append(f"""{_map_name(m)} AS (
  SELECT DISTINCT C.{CONFIG_ID_COL}, COALESCE(H.{HIER_CHILD_COL}, C.{col}) AS {_gran(m)}
  FROM Config C
  LEFT JOIN `{HIERARCHY_TABLE}` H
    ON C.{col} = H.{HIER_PARENT_COL}
    AND H.{HIER_DIM_COL} = '{dim}'
    AND ({_hier_cond("H", "C", ncol)})
  WHERE C.{col} <> 'ALL'
)""")

    # Required dimensions: every base row filters them (no wildcard). Matching to config rows uses joins only -
    # BigQuery rejects correlated subqueries on tables, and cannot always de-correlate IN (SELECT ...) either.
    required = [m for m in joined if flags[m["config_col"]]["required"]]
    optional = [m for m in joined if not flags[m["config_col"]]["required"]]

    # actuals: only the years, columns and rows the config can use, summed before they are matched to config rows.
    # Each required dimension pre-filters the actuals with a join on its granular values.
    group_cols = [f"T.{c}" for c in [ACTUALS_YEAR_COL, ACTUALS_PERIOD_COL] + [m["txn_col"] for m in joined]]
    filters = []
    if ACTUALS_YEARS_BACK:
        # label of the current fiscal year = year of today shifted to the fiscal year end
        shift = 0 if FISCAL_YEAR_START_MONTH == 1 else 13 - FISCAL_YEAR_START_MONTH
        filters.append(f"T.{ACTUALS_YEAR_COL} IN (\n"
                       f"      SELECT FORMAT_DATE('{YEAR_LABEL_FORMAT}', DATE_SUB(DATE_ADD(CURRENT_DATE(), "
                       f"INTERVAL {shift} MONTH), INTERVAL i YEAR))\n"
                       f"      FROM UNNEST(GENERATE_ARRAY(0, {ACTUALS_YEARS_BACK - 1})) AS i)")
    if EXCLUDED_PERIODS:
        filters.append(f"CAST(T.{ACTUALS_PERIOD_COL} AS STRING) NOT IN ({_q(EXCLUDED_PERIODS)})")
    actuals_sql = (f"actuals AS (\n  SELECT {', '.join(group_cols)}, "
                   f"SUM(T.{ACTUALS_AMOUNT_COL}) AS {ACTUALS_AMOUNT_COL}\n"
                   f"  FROM `{TRANSACTION_TABLE}` T\n")
    for m in required:
        f_alias = f"F_{m['config_col']}"
        actuals_sql += (f"  INNER JOIN (SELECT DISTINCT {_gran(m)} FROM {_map_name(m)}) {f_alias}\n"
                        f"    ON {f_alias}.{_gran(m)} = T.{m['txn_col']}\n")
    if filters:
        actuals_sql += "  WHERE " + "\n    AND ".join(filters) + "\n"
    ctes.append(actuals_sql + f"  GROUP BY {', '.join(group_cols)}\n)")

    # base_values: an actual counts for a config row when, per dimension, the row is a wildcard or the actual's value
    # is one of the row's granular values. Required dimensions are inner joins (the first also pairs each actual with
    # only the config rows it can match, instead of every row); the others are LEFT JOINs + "wildcard OR matched",
    # as in driver_line_item.sql. The mapping CTEs are DISTINCT, so no join repeats an actual.
    if required:
        first = required[0]
        joins = [f"  INNER JOIN {_map_name(first)} {_map_alias(first)}\n"
                 f"    ON {_map_alias(first)}.{_gran(first)} = A.{first['txn_col']}\n"
                 f"  INNER JOIN Config C\n"
                 f"    ON C.{CONFIG_ID_COL} = {_map_alias(first)}.{CONFIG_ID_COL}"]
    else:
        joins = ["  CROSS JOIN Config C"]
    joins += [f"  {'INNER' if m in required else 'LEFT'} JOIN {_map_name(m)} {_map_alias(m)}\n"
              f"    ON {_map_alias(m)}.{CONFIG_ID_COL} = C.{CONFIG_ID_COL}\n"
              f"    AND {_map_alias(m)}.{_gran(m)} = A.{m['txn_col']}"
              for m in required[1:] + optional]
    base_sql = (f"base_values AS (\n"
                f"  SELECT C.{LINE_ITEM_COL}, A.{ACTUALS_YEAR_COL} AS year, A.{ACTUALS_PERIOD_COL} AS month, "
                f"SUM(A.{ACTUALS_AMOUNT_COL}) AS Amount\n"
                f"  FROM actuals A\n" + "\n".join(joins))
    if optional:
        base_sql += "\n  WHERE\n    " + "\n    AND ".join(
            f"(C.{m['config_col']} = 'ALL' OR {_map_alias(m)}.{CONFIG_ID_COL} IS NOT NULL)" for m in optional)
    base_sql += f"\n  GROUP BY C.{LINE_ITEM_COL}, A.{ACTUALS_YEAR_COL}, A.{ACTUALS_PERIOD_COL}\n)"
    ctes.append(base_sql)

    return ("WITH\n" + ",\n".join(ctes) + "\n"
            f"SELECT {LINE_ITEM_COL}, year, month, Amount\nFROM base_values")


_PRECEDENCE = {"+": 1, "-": 1, "*": 2}


def _compile(node, parent=0, right=False):
    """SQL for a resolved formula inside a tier's grouped pass (one row per month over the long rows):
    [X] -> SUM(IF(LineItem = 'X', Amount, 0)), so a missing line item counts as 0."""
    kind = node[0]
    if kind == "num":
        return _num(node[1])
    if kind == "ref":
        return f"SUM(IF({LINE_ITEM_COL} = {_lit(node[1])}, Amount, 0))"
    if kind == "neg":
        return f"-({_compile(node[1])})"                # never "--": that starts a SQL comment
    if kind == "call":
        return FUNCTIONS[node[1]]["sql"]([a[1] if a[0] == "int" else _compile(a) for a in node[2]])
    op, left, right_node = node[1], node[2], node[3]
    if op == "/":
        return f"SAFE_DIVIDE({_compile(left)}, {_compile(right_node)})"
    prec = _PRECEDENCE[op]
    sql = f"{_compile(left, prec)} {op} {_compile(right_node, prec, True)}"
    return f"({sql})" if prec < parent or (right and prec == parent) else sql


def _monthly_summary_dated():
    months = [("jan", "january"), ("feb", "february"), ("mar", "march"), ("apr", "april"), ("may", "may"),
              ("jun", "june"), ("jul", "july"), ("aug", "august"), ("sep", "september"), ("oct", "october"),
              ("nov", "november"), ("dec", "december")]
    whens = "\n".join(f"          WHEN '{short}' THEN {i}" + (f" WHEN '{full}' THEN {i}" if full != short else "")
                      for i, (short, full) in enumerate(months, start=1))
    date_sql = "SAFE.DATE(IF(year_num < 100, 2000 + year_num, year_num), month_num, 1)"
    if FISCAL_YEAR_START_MONTH != 1:
        # the fiscal year is named after the calendar year it ends in: its first months are in the year before
        date_sql = f"DATE_SUB({date_sql}, INTERVAL IF(month_num >= {FISCAL_YEAR_START_MONTH}, 12, 0) MONTH)"
    return f"""monthly_summary_dated AS (
  -- month_date orders the months for LAG / rolling / roll-forward; a year or month it cannot read stops the query
  SELECT
    year, month, {LINE_ITEM_COL}, Amount,
    IF(month_date IS NULL,
       ERROR(FORMAT('cannot turn year %T / month %T into a date (fix the month mapping or add the period to EXCLUDED_PERIODS)', year, month)),
       month_date) AS month_date
  FROM (
    SELECT *, {date_sql} AS month_date
    FROM (
      SELECT
        *,
        SAFE_CAST(REGEXP_REPLACE(CAST(year AS STRING), r'^[Ff][Yy]', '') AS INT64) AS year_num,
        CASE LOWER(TRIM(CAST(month AS STRING)))
{whens}
          ELSE SAFE_CAST(TRIM(CAST(month AS STRING)) AS INT64)
        END AS month_num
      FROM monthly_summary
    )
  )
)"""


def build_tier_ctes(k, tier, source):
    """CTEs of one tier: its formulas calculated from `source` (every row of the earlier tiers), then combined_<k>.
    All formulas of the tier are calculated in ONE grouped pass: BigQuery re-runs a CTE for every reference, so one
    branch per formula would multiply the work with every tier."""
    li = LINE_ITEM_COL
    ctes, parts = [], []
    calc = [it for it in tier if it["kind"] == "formula"]
    rolls = [it for it in tier if it["kind"] == "rollforward"]

    if calc:
        cols = [f"    -- {_one_line(it['name'])} = {_one_line(it['formula'])}  ({CONFIG_ID_COL} {it['cid']})\n"
                f"    {_compile(it['ast'])} AS {it['col']}" for it in calc]
        ctes.append(f"tier_{k}_calc AS (\n"
                    f"  -- one row per month; [X] = SUM(IF({li} = 'X', Amount, 0))\n"
                    f"  SELECT\n    year, month, month_date,\n" + ",\n".join(cols) +
                    f"\n  FROM {source}\n  GROUP BY year, month, month_date\n)")
        pairs = ",\n    ".join(f"({_lit(it['name'])}, calc.{it['col']})" for it in calc)
        parts.append(f"  SELECT calc.year, calc.month, calc.month_date, item.{li}, "
                     f"COALESCE(item.Amount, 0) AS Amount\n"
                     f"  FROM tier_{k}_calc AS calc\n"
                     f"  CROSS JOIN UNNEST(ARRAY<STRUCT<{li} STRING, Amount {AMOUNT_TYPE}>>[\n"
                     f"    {pairs}\n  ]) AS item")

    if rolls:
        # Roll-forward: this(t) = this(t-1) + net_adds(t) + adj(t). The recursion is resolved without a loop:
        # anchor on the latest month before t with an actual (if any), then add every month's movement since:
        #   this(t) = actual(anchor) + SUM(net_adds + adj) over (anchor, t]; no actual yet -> running sum from 0.
        segments = ",\n    ".join(
            f"-- {_one_line(it['name'])} = {_one_line(it['formula'])}  ({CONFIG_ID_COL} {it['cid']})\n    ("
            + ", ".join(_lit(v) if v else "NULL"
                        for v in (it["name"], it["rf"]["net_adds"], it["rf"].get("adj"), it["rf"].get("actual")))
            + ")"
            for it in rolls)
        actual_sql = f"SUM(IF(c.{li} = s.actual_item, c.Amount, NULL))"
        if RF_ZERO_IS_NO_ACTUAL:
            actual_sql = f"NULLIF({actual_sql}, 0)"
        ctes.append(f"""tier_{k}_rf_segments AS (
  SELECT *
  FROM UNNEST(ARRAY<STRUCT<line_item STRING, net_adds_item STRING, adj_item STRING, actual_item STRING>>[
    {segments}
  ])
)""")
        ctes.append(f"""tier_{k}_rf_inputs AS (
  -- one row per roll-forward per month: the actual (NULL = none) and the month's movement
  SELECT
    s.line_item AS {li}, c.year, c.month, c.month_date,
    {actual_sql} AS actual_value,
    SUM(IF(c.{li} IN (s.net_adds_item, s.adj_item), c.Amount, 0)) AS movement
  FROM {source} AS c
  CROSS JOIN tier_{k}_rf_segments AS s
  GROUP BY s.line_item, c.year, c.month, c.month_date
)""")
        ctes.append(f"""tier_{k}_rf_running AS (
  SELECT
    *,
    SUM(movement) OVER (
      PARTITION BY {li} ORDER BY month_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_movement
  FROM tier_{k}_rf_inputs
)""")
        ctes.append(f"""tier_{k}_rf AS (
  SELECT
    {li}, year, month, month_date,
    COALESCE(
      -- latest prior actual + movement accumulated since that month
      LAST_VALUE(actual_value IGNORE NULLS) OVER w_prior
        + cum_movement
        - LAST_VALUE(IF(actual_value IS NOT NULL, cum_movement, NULL) IGNORE NULLS) OVER w_prior,
      -- first month with an actual (nothing prior to roll from): its own actual
      actual_value,
      -- no actual yet: roll forward from an opening balance of 0
      cum_movement
    ) AS Amount
  FROM tier_{k}_rf_running
  WINDOW w_prior AS (
    PARTITION BY {li} ORDER BY month_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )
)""")
        parts.append(f"  SELECT year, month, month_date, {li}, COALESCE(Amount, 0) AS Amount\n"
                     f"  FROM tier_{k}_rf")

    names = ",\n    ".join(_lit(it["name"]) for it in tier)
    ctes.append(f"tier_{k} AS (\n" + "\n  UNION ALL\n".join(parts) + "\n)")
    ctes.append(f"tier_{k}_names AS (\n  SELECT {li} FROM UNNEST(ARRAY<STRING>[\n    {names}\n  ]) AS {li}\n)")
    ctes.append(f"""combined_{k} AS (
  -- anti-join: a calculated line item replaces source rows with the same name (no double counting)
  SELECT c.year, c.month, c.month_date, c.{li}, c.Amount
  FROM {source} AS c
  LEFT JOIN tier_{k}_names AS n
    ON c.{li} = n.{li}
  WHERE n.{li} IS NULL
  UNION ALL
  SELECT year, month, month_date, {li}, Amount FROM tier_{k}
)""")
    return ctes


def build_formula_sql(plan):
    """Every output line item, the formula ones calculated tier by tier from STAGING_TABLE.
    The generated, config-driven driver_line_item_all.sql; written to OUTPUT_TABLE."""
    li = LINE_ITEM_COL
    ctes = [f"""monthly_summary AS (
  SELECT year, month, {li}, CAST(COALESCE(SUM(Amount), 0) AS {AMOUNT_TYPE}) AS Amount
  FROM `{STAGING_TABLE}`
  GROUP BY year, month, {li}
)""",
            _monthly_summary_dated(),
            f"combined_0 AS (\n  SELECT year, month, month_date, {li}, Amount FROM monthly_summary_dated\n)"]
    source = "combined_0"
    for k, tier in enumerate(plan["tiers"], start=1):
        ctes += build_tier_ctes(k, tier, source)
        source = f"combined_{k}"
    outputs = ",\n    ".join(_lit(n) for n in plan["output_items"])
    ctes.append(f"output_items AS (\n"
                f"  -- line items whose {CONFIG_OUTPUT_COL} is not {' / '.join(OUTPUT_NO_VALUES)}\n"
                f"  SELECT {li} FROM UNNEST(ARRAY<STRING>[\n    {outputs}\n  ]) AS {li}\n)")
    return ("WITH\n" + ",\n\n".join(ctes) + "\n\n"
            f"SELECT c.{li}, c.year, c.month, COALESCE(c.Amount, 0) AS Amount\n"
            f"FROM {source} AS c\n"
            f"JOIN output_items AS o\n  ON c.{li} = o.{li}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def _check_settings():
    """Fail with a clear message when the DAG config json is missing table settings."""
    bad = [name for name, value in (("CONFIG_TABLE", CONFIG_TABLE), ("HIERARCHY_TABLE", HIERARCHY_TABLE),
                                    ("TRANSACTION_TABLE", TRANSACTION_TABLE), ("OUTPUT_TABLE", OUTPUT_TABLE),
                                    ("STAGING_TABLE", STAGING_TABLE))
           if len(str(value).split(".")) != 3 or "None" in str(value).split(".")]
    if bad:
        raise ValueError(f"{bad} are not project.dataset.table - check the *_dataset / *_bq_table keys "
                         f"in the DAG config json of {DAG_NAME}")


def _bq_client():
    hook = BigQueryHook(gcp_conn_id=bq_conn_id, use_legacy_sql=False)
    client = hook.get_client(project_id=PROJECT_ID_CURATE)
    if DISABLE_SSL_VERIFY:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        client._http.verify = False
    return client


def dry_run(client, base_sql, formula_sql):
    """Lets BigQuery check both statements without running them. The formula SQL reads the staging table, which
    does not exist before the first run."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    job = client.query(base_sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    logging.info("Base SQL dry run OK: %.2f GB to process", job.total_bytes_processed / 1e9)
    try:
        client.get_table(STAGING_TABLE)
    except NotFound:
        logging.info("%s does not exist yet (first run): formula SQL dry run skipped", STAGING_TABLE)
        return
    job = client.query(formula_sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    logging.info("Formula SQL dry run OK: %.2f GB to process", job.total_bytes_processed / 1e9)


def generate_sql_query(cfg=None, **context):
    """PythonOperator callable: config -> checks -> {"base_sql", "formula_sql"} (returned via XCom)."""
    if cfg:
        # Tasks run after the whole file has been parsed, when the module-level names hold the values of the
        # LAST entry in dag_configs. Re-apply the settings of THIS DAG before building SQL.
        globals().update(cfg)
    _check_settings()
    client = _bq_client()

    config_cols = get_config_columns(client)
    plan = prepare_plan(load_config_rows(client), config_cols, get_dimension_map(client))
    validate_hierarchy(client, plan)

    base_sql, formula_sql = build_base_sql(plan), build_formula_sql(plan)
    logging.info("Formula plan:\n%s", describe_plan(plan))
    logging.info("Base SQL (-> %s):\n%s", STAGING_TABLE, base_sql)
    logging.info("Formula SQL (-> %s):\n%s", OUTPUT_TABLE, formula_sql)
    if DRY_RUN:
        dry_run(client, base_sql, formula_sql)
    return {"base_sql": base_sql, "formula_sql": formula_sql}


def _destination(table):
    project, dataset, table_id = table.split(".")
    return {"projectId": project, "datasetId": dataset, "tableId": table_id}


for dag_name in dag_configs:
    DAG_NAME = dag_name

    CONFIG_FILE_PATH = f'dags/{PROJECT_SPACE}/{DAG_NAME}/config/{DAG_NAME}_config.json'

    # Written by the config DAG from the config table row of DAG_NAME; this DAG only reads it.
    bq_config_params = _dag_params(read_json(CONFIG_FILE_PATH), DAG_NAME)

    BQ_CONN_ID = bq_config_params["bq_conn_id"]

    # -----------------------------------------------------------------------
    # Table Variables Split (Project, Dataset, Table)
    # -----------------------------------------------------------------------
    # Curate: the config table
    CURATE_DATASET = _param(bq_config_params, "curate_dataset")
    CURATE_TABLE_NAME = _param(bq_config_params, "curate_bq_table", "curate_table_name")
    CONFIG_TABLE = f"{PROJECT_ID_CURATE}.{CURATE_DATASET}.{CURATE_TABLE_NAME}"

    # Enrich: the hierarchy view
    ENRICH_DATASET = _param(bq_config_params, "enrich_dataset")
    ENRICH_TABLE_NAME = _param(bq_config_params, "enrich_bq_table", "enrich_table_name")
    HIERARCHY_TABLE = f"{PROJECT_ID_CURATE}.{ENRICH_DATASET}.{ENRICH_TABLE_NAME}"

    # Raw: the actuals
    RAW_DATASET = _param(bq_config_params, "raw_dataset")
    RAW_TABLE_NAME = _param(bq_config_params, "raw_bq_table", "raw_table_name")
    TRANSACTION_TABLE = f"{PROJECT_ID_CURATE}.{RAW_DATASET}.{RAW_TABLE_NAME}"

    # Publish: the output
    PUBLISH_DATASET = _param(bq_config_params, "publish_dataset")
    PUBLISH_TABLE_NAME = _param(bq_config_params, "publish_bq_table", "publish_table_name")
    OUTPUT_TABLE = f"{PROJECT_ID_CURATE}.{PUBLISH_DATASET}.{PUBLISH_TABLE_NAME}"

    # Staging table: base_sql writes the line items fetched from actuals here; formula_sql reads them back.
    # Created by the first run (CREATE_IF_NEEDED) and replaced on every run.
    STAGING_TABLE_NAME = _param(bq_config_params, "staging_table_name", default=f"{PUBLISH_TABLE_NAME}_base_values")
    STAGING_TABLE = f"{PROJECT_ID_CURATE}.{PUBLISH_DATASET}.{STAGING_TABLE_NAME}"

    # Dimension Map Metadata
    DIMENSION_MAP_TABLE = f"{PROJECT_ID_CURATE}.{ENRICH_DATASET}.{DAG_NAME}_dimensions"

    OPERATIONAL_DATASET = bq_config_params["operational_dataset"]
    bq_conn_id = BQ_CONN_ID

    schedule_int = _param(bq_config_params, "schedule", "schedule_int")

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
    # This DAG's own settings, handed to the python task (see generate_sql_query)
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

        # 1. read + check the config, build base_sql and formula_sql
        generate_sql = PythonOperator(
            task_id='generate_sql_query',
            python_callable=generate_sql_query,
            op_kwargs={"cfg": DAG_CFG},
            provide_context=True,
            dag=dag
        )

        # 2. base line items from actuals -> staging table
        materialize_base_values = BigQueryInsertJobOperator(
            task_id='materialize_base_values',
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='generate_sql_query')['base_sql'] }}",
                    "useLegacySql": False,
                    "destinationTable": _destination(STAGING_TABLE),
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            gcp_conn_id=bq_conn_id,
            dag=dag
        )

        # 3. formula line items calculated from the staging table -> output table
        execute_compiled_sql = BigQueryInsertJobOperator(
            task_id='execute_compiled_sql',
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='generate_sql_query')['formula_sql'] }}",
                    "useLegacySql": False,
                    "destinationTable": _destination(OUTPUT_TABLE),
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            gcp_conn_id=bq_conn_id,
            dag=dag
        )

        generate_sql >> materialize_base_values >> execute_compiled_sql

    # Register this DAG under its own name so that every entry of dag_configs is picked up
    globals()[DAG_NAME] = dag
