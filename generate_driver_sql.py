"""
Writes the BigQuery SQL of the corp_challenger_model_driver_line_item_all DAG for a config CSV: DDL and DML for its two
tables, and previews to run by hand in the BigQuery console. It uses the DAG file's own compiler
(corp_challenger_model_driver_line_item_all.py, loaded with Airflow stubbed out), so the SQL is the SQL the DAG runs.

    python generate_driver_sql.py                          config_driver.csv -> generated_sql/
    python generate_driver_sql.py --project my-project     other project in the previews

Files written (the DDL and DML use --deploy-project, default oddo_project):
    ddl/corp_challenger_driver_formula_config.sql        CREATE TABLE for the config table the DAG reads
    ddl/corp_challenger_driver_line_item_all_table.sql   CREATE TABLE for the output table the DAG fills
    dml/corp_challenger_driver_formula_config.sql        replaces the config table's rows with the CSV rows
    dml/corp_challenger_driver_line_item_all_table.sql   exactly what the DAG runs: truncates and refills the output table
    dml/ingestion_config_table.sql                       this DAG's row in ingestion_config_table, from its config json
    run_all.sql            preview: the output line items, sorted (writes no tables)
    base.sql               preview: the line items fetched from actuals (writes no tables)
    hierarchy_check.sql    config values whose deeper hierarchy nodes the mapping misses (no rows = fine)
The previews carry the CSV rows in the SQL, so they work before the config table is loaded.
The dimension map is the DAG file's DEFAULT_DIMENSION_MAP.
"""
import argparse
import csv
import importlib.util
import json
import logging
import os
import sys
import types
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DAG_NAME = "corp_challenger_model_driver_line_item_all"
DAG_FILE = os.path.join(HERE, f"{DAG_NAME}.py")
DEFAULT_PROJECT = "vz-it-np-j0nv-dev-oddo-0"
DEFAULT_DEPLOY_PROJECT = "oddo_project"
CONFIG_TABLE = "od_enriched_epmmlops.corp_challenger_driver_formula_config"          # curate_*
OUTPUT_TABLE = "od_enriched_epmmlops.corp_challenger_driver_line_item_all_table"     # publish_*
HIERARCHY_TABLE = "od_enriched_edmcs.edm_master_incl_alt_hierarchies_v"              # enrich_*
ACTUALS_TABLE = "od_published_epm.epm_p00all_actuals_fullhist_v"                     # raw_*
INGESTION_CONFIG_TABLE = "od_operational.ingestion_config_table"                     # read by config_creation_dag
DAG_CONFIG_JSON = os.path.join(HERE, f"{DAG_NAME}_config.json")                       # this DAG's row in it
INLINE_CONFIG = "config_csv"                 # CTE with the CSV rows, standing in for the config table in the previews


def load_dag_module():
    """Imports the DAG file with Airflow and the Composer helpers (py.*) stubbed out; only its SQL builders are used."""
    class Stub:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __rshift__(self, other):
            return other

    dag_json = [{"bq_conn_id": "-", "operational_dataset": "-", "schedule": None}]
    stubs = {
        "airflow": {"DAG": Stub},
        "airflow.models": {"Variable": types.SimpleNamespace(get=lambda key: "local")},
        "airflow.utils": {},
        "airflow.utils.dates": {"days_ago": lambda n: None},
        "airflow.operators": {},
        "airflow.operators.python_operator": {"PythonOperator": Stub},
        "airflow.providers": {},
        "airflow.providers.google": {},
        "airflow.providers.google.cloud": {},
        "airflow.providers.google.cloud.operators": {},
        "airflow.providers.google.cloud.operators.bigquery": {"BigQueryInsertJobOperator": Stub},
        "airflow.providers.google.cloud.hooks": {},
        "airflow.providers.google.cloud.hooks.bigquery": {"BigQueryHook": Stub},
        "py": {},
        "py.common_helper": {"get_dag_start_date": lambda: None, "read_json": lambda path: dag_json},
        "py.get_env_variable": {"get_curate_project_id": lambda: "local", "get_enrich_project_id": lambda: "local",
                                "get_resource_sa": lambda: "local", "get_raw_project_id": lambda: "local"},
        "py.error_helper": {"task_failure_log": None},
        "py.obserbility_helper": {"success_log": None},
    }
    for name, attrs in stubs.items():
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        sys.modules[name] = module
    spec = importlib.util.spec_from_file_location(DAG_NAME, DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_config_csv(path):
    """Config rows the way the DAG reads them from BigQuery: dicts keyed by lower-case column name, blank = NULL."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = [{k.lower(): (v if v and v.strip() else None) for k, v in row.items() if k is not None}
                for row in reader]
    return rows, {c.lower(): c for c in header}, header


def config_types(mod, rows, header):
    """BigQuery type per config column: Config_ID INT64 when all ids are whole numbers, FinalOutput BOOL, the rest
    STRING."""
    id_col, output_col = mod.CONFIG_ID_COL.lower(), mod.CONFIG_OUTPUT_COL.lower()
    numeric_ids = all(str(r.get(id_col) or "").isdigit() for r in rows)
    return {c: "INT64" if c.lower() == id_col and numeric_ids else "BOOL" if c.lower() == output_col else "STRING"
            for c in header}


def config_rows_sql(mod, rows, header):
    """The CSV rows as a query. FinalOutput follows the DAG's rule: N / NO / FALSE / 0 -> FALSE, else (blank too) TRUE."""
    def literal(value, kind):
        if kind == "BOOL":
            return "TRUE" if mod._is_output_value(value) else "FALSE"
        if value is None:
            return "NULL"
        return value if kind == "INT64" else mod._lit(value)

    col_types = config_types(mod, rows, header)
    fields = ", ".join(f"`{c}` {t}" for c, t in col_types.items())
    values = ",\n    ".join("(" + ", ".join(literal(r.get(c.lower()), t) for c, t in col_types.items()) + ")"
                            for r in rows)
    return f"SELECT * FROM UNNEST(ARRAY<STRUCT<{fields}>>[\n    {values}\n  ])"


def ingestion_config_sql(mod, table, json_path):
    """INSERT of the row(s) of a DAG config json (same keys as the ingestion_config_table columns)."""
    with open(json_path, encoding="utf-8") as f:
        rows = json.load(f)
    rows = rows if isinstance(rows, list) else [rows]
    columns = list(rows[0])
    values = ",\n  ".join("(" + ", ".join("NULL" if r.get(c) is None else mod._lit(r[c]) for c in columns) + ")"
                          for r in rows)
    return f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in columns)})\nVALUES\n  {values};\n"


def create_table_sql(table, columns):
    return f"CREATE TABLE IF NOT EXISTS `{table}` (\n  " + ",\n  ".join(f"`{c}` {t}" for c, t in columns) + "\n);"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=os.path.join(HERE, "config_driver.csv"), help="config CSV (default: %(default)s)")
    p.add_argument("--out-dir", default=os.path.join(HERE, "generated_sql"), help="default: %(default)s")
    p.add_argument("--deploy-project", default=DEFAULT_DEPLOY_PROJECT,
                   help="project in the DDL and DML files (default: %(default)s)")
    p.add_argument("--project", default=DEFAULT_PROJECT, help="project in the previews (default: %(default)s)")
    p.add_argument("--hierarchy-table", help=f"for the previews; default: <project>.{HIERARCHY_TABLE}")
    p.add_argument("--actuals-table", help=f"for the previews; default: <project>.{ACTUALS_TABLE}")
    p.add_argument("-v", "--verbose", action="store_true", help="also print the dimension checks and formula tiers")
    args = p.parse_args()
    args.hierarchy_table = args.hierarchy_table or f"{args.project}.{HIERARCHY_TABLE}"
    args.actuals_table = args.actuals_table or f"{args.project}.{ACTUALS_TABLE}"
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    mod = load_dag_module()

    rows, config_cols, header = load_config_csv(args.config)
    try:
        plan = mod.prepare_plan(rows, config_cols, mod.DEFAULT_DIMENSION_MAP)    # the DAG's own config checks
    except ValueError as e:
        sys.exit(f"{args.config}:\n{e}")

    # DDL / DML: every table in the deploy project, as the DAG sees them
    deploy = args.deploy_project
    config_table, output_table = f"{deploy}.{CONFIG_TABLE}", f"{deploy}.{OUTPUT_TABLE}"
    mod.CONFIG_TABLE, mod.OUTPUT_TABLE = config_table, output_table
    mod.HIERARCHY_TABLE, mod.TRANSACTION_TABLE = f"{deploy}.{HIERARCHY_TABLE}", f"{deploy}.{ACTUALS_TABLE}"
    output_dml = mod.build_dag_script(plan)

    # previews: the CSV rows once per file, as a CTE the SQL reads instead of the config table
    mod.HIERARCHY_TABLE, mod.TRANSACTION_TABLE = args.hierarchy_table, args.actuals_table
    mod.CONFIG_TABLE = INLINE_CONFIG
    config_cte = f"{INLINE_CONFIG} AS (\n  {config_rows_sql(mod, rows, header)}\n)"
    base_sql = (mod.build_base_sql(plan).replace("WITH\n", f"WITH\n{config_cte},\n", 1)
                .replace(f"`{INLINE_CONFIG}`", INLINE_CONFIG))
    check_sql = mod.adjacency_check_sql(plan)
    check_sql = check_sql and f"WITH {config_cte}\n" + check_sql.replace(f"`{INLINE_CONFIG}`", INLINE_CONFIG)

    csv_name = os.path.basename(args.config)
    generated = f"-- Generated by generate_driver_sql.py from {csv_name} on {datetime.now():%Y-%m-%d %H:%M} (DAG {DAG_NAME})"
    preview_tables = [f"-- hierarchy: `{args.hierarchy_table}`", f"-- actuals:   `{args.actuals_table}`"]
    config_name, output_name = CONFIG_TABLE.split(".")[1], OUTPUT_TABLE.split(".")[1]
    files = {
        f"ddl/{config_name}.sql": (
            [f"-- DDL: the config table the DAG reads (curate_*). Load its rows with dml/{config_name}.sql."],
            create_table_sql(config_table, config_types(mod, rows, header).items()) + "\n"),
        f"ddl/{output_name}.sql": (
            ["-- DDL: the output table the DAG fills (publish_*). The DAG never creates tables; it only truncates and",
             "-- inserts."],
            create_table_sql(output_table, mod.output_columns()) + "\n"),
        f"dml/{config_name}.sql": (
            [f"-- DML: replaces the rows of the config table with the rows of {csv_name}.",
             "-- Run again whenever the CSV changes."],
            f"TRUNCATE TABLE `{config_table}`;\n\n"
            f"INSERT INTO `{config_table}` ({', '.join(f'`{c}`' for c in header)})\n"
            f"{config_rows_sql(mod, rows, header)};\n"),
        f"dml/{output_name}.sql": (
            ["-- DML: exactly what task execute_compiled_sql of the DAG runs. The base line items go to a temp table,",
             "-- then the output table is truncated and refilled. Reads the config table, so load it first."],
            output_dml),
        "dml/ingestion_config_table.sql": (
            [f"-- DML: this DAG's row in ingestion_config_table (config_creation_dag turns it into its config json), from",
             f"-- {os.path.basename(DAG_CONFIG_JSON)}. Replace the id placeholder first; running it twice adds the row twice."],
            ingestion_config_sql(mod, f"{deploy}.{INGESTION_CONFIG_TABLE}", DAG_CONFIG_JSON)
            if os.path.exists(DAG_CONFIG_JSON) else None),
        "run_all.sql": (
            ["-- Preview: run as one script in the BigQuery console; the last statement shows the output line",
             f"-- items. Uses the rows of {csv_name} (written into this SQL) and writes no tables."] + preview_tables,
            f"CREATE TEMP TABLE {mod.BASE_VALUES_TABLE} AS\n{base_sql};\n\n"
            f"{mod.build_formula_sql(plan)}\nORDER BY c.{mod.LINE_ITEM_COL}, c.month_date;\n"),
        "base.sql": (
            [f"-- Preview: the line items fetched from actuals, from the rows of {csv_name} (written into this SQL).",
             "-- Writes no tables."] + preview_tables,
            base_sql + ";\n"),
        "hierarchy_check.sql": (
            ["-- Config values that reach deeper hierarchy nodes only through their children. The mapping joins",
             "-- parent = config value once, so actuals booked on those deeper nodes are NOT counted. No rows = fine."]
            + preview_tables,
            check_sql + ";\n" if check_sql else None),
    }

    written = []
    for name, (lines, body) in files.items():
        if body is None:
            continue
        path = os.path.abspath(os.path.join(args.out_dir, name))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join([generated] + lines) + "\n\n" + body)
        written.append(path)

    items = plan["items"].values()
    print(f"{csv_name}: {sum(it['kind'] == 'base' for it in items)} base line items, "
          f"{sum(it['kind'] != 'base' for it in items)} formula line items in {len(plan['tiers'])} tiers "
          f"({', '.join(str(len(t)) for t in plan['tiers'])}), {len(plan['output_items'])} line items in the output")
    if args.verbose:
        print(mod.describe_plan(plan))
    print("Wrote:\n  " + "\n  ".join(written))


if __name__ == "__main__":
    main()
