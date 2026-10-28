"""
Writes the BigQuery SQL of the corp_challenger_model_driver_line_item_all DAG for a config CSV: the table deployment
scripts, exactly what the DAG runs, and previews to run by hand in the BigQuery console. It uses the DAG file's own
compiler (corp_challenger_model_driver_line_item_all.py, loaded with Airflow stubbed out), so the SQL is the SQL the
DAG runs; only the table names come from the command line.

    python generate_driver_sql.py                          config_driver.csv -> generated_sql/
    python generate_driver_sql.py --project my-project     other project in the default table names

Files written:
    create_tables.sql      CREATE TABLE IF NOT EXISTS for the config table and the output table (table deployment)
    load_config_table.sql  replaces the rows of the config table with the CSV rows
    dag_script.sql         exactly what the DAG runs: reads the config table, truncates and refills the output table
    run_all.sql            preview: the output line items, sorted (writes no tables)
    base.sql               preview: the line items fetched from actuals (writes no tables)
    hierarchy_check.sql    config values whose deeper hierarchy nodes the mapping misses (no rows = fine)
The previews carry the CSV rows in the SQL, so they work before the config table is loaded.
The dimension map is the DAG file's DEFAULT_DIMENSION_MAP.
"""
import argparse
import csv
import importlib.util
import logging
import os
import sys
import types
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DAG_NAME = "corp_challenger_model_driver_line_item_all"
DAG_FILE = os.path.join(HERE, f"{DAG_NAME}.py")
DEFAULT_PROJECT = "vz-it-np-j0nv-dev-oddo-0"
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
    """BigQuery type per config column: Config_ID INT64 when all ids are whole numbers, everything else STRING."""
    id_col = mod.CONFIG_ID_COL.lower()
    numeric_ids = all(str(r.get(id_col) or "").isdigit() for r in rows)
    return {c: "INT64" if c.lower() == id_col and numeric_ids else "STRING" for c in header}


def config_rows_sql(mod, rows, header):
    """The CSV rows as a query."""
    col_types = config_types(mod, rows, header)
    fields = ", ".join(f"`{c}` {t}" for c, t in col_types.items())
    values = ",\n    ".join(
        "(" + ", ".join("NULL" if r.get(c.lower()) is None else r[c.lower()] if t == "INT64" else mod._lit(r[c.lower()])
                        for c, t in col_types.items()) + ")"
        for r in rows)
    return f"SELECT * FROM UNNEST(ARRAY<STRUCT<{fields}>>[\n    {values}\n  ])"


def create_table_sql(table, columns):
    return f"CREATE TABLE IF NOT EXISTS `{table}` (\n  " + ",\n  ".join(f"`{c}` {t}" for c, t in columns) + "\n);"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=os.path.join(HERE, "config_driver.csv"), help="config CSV (default: %(default)s)")
    p.add_argument("--out-dir", default=os.path.join(HERE, "generated_sql"), help="default: %(default)s")
    p.add_argument("--project", default=DEFAULT_PROJECT, help="project in the default table names (default: %(default)s)")
    p.add_argument("--config-table", help="the config table the DAG reads (curate_*); "
                                          "default: <project>.od_enriched_epmmlops.corp_challenger_driver_formula_config")
    p.add_argument("--output-table", help="the table the DAG fills (publish_*); "
                                          "default: <project>.od_enriched_epmmlops.corp_challenger_driver_line_item_all_table")
    p.add_argument("--hierarchy-table", help="default: <project>.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v")
    p.add_argument("--actuals-table", help="default: <project>.od_published_epm.epm_p00all_actuals_fullhist_v")
    p.add_argument("-v", "--verbose", action="store_true", help="also print the dimension checks and formula tiers")
    args = p.parse_args()
    args.config_table = args.config_table or f"{args.project}.od_enriched_epmmlops.corp_challenger_driver_formula_config"
    args.output_table = (args.output_table
                         or f"{args.project}.od_enriched_epmmlops.corp_challenger_driver_line_item_all_table")
    args.hierarchy_table = (args.hierarchy_table
                            or f"{args.project}.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v")
    args.actuals_table = args.actuals_table or f"{args.project}.od_published_epm.epm_p00all_actuals_fullhist_v"
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

    mod.HIERARCHY_TABLE, mod.TRANSACTION_TABLE = args.hierarchy_table, args.actuals_table
    mod.CONFIG_TABLE, mod.OUTPUT_TABLE = args.config_table, args.output_table
    dag_script = mod.build_dag_script(plan)

    # previews: the CSV rows once per file, as a CTE the SQL reads instead of the config table
    mod.CONFIG_TABLE = INLINE_CONFIG
    config_cte = f"{INLINE_CONFIG} AS (\n  {config_rows_sql(mod, rows, header)}\n)"
    base_sql = (mod.build_base_sql(plan).replace("WITH\n", f"WITH\n{config_cte},\n", 1)
                .replace(f"`{INLINE_CONFIG}`", INLINE_CONFIG))
    check_sql = mod.adjacency_check_sql(plan)
    check_sql = check_sql and f"WITH {config_cte}\n" + check_sql.replace(f"`{INLINE_CONFIG}`", INLINE_CONFIG)

    csv_name = os.path.basename(args.config)
    header_lines = [
        f"-- Generated by generate_driver_sql.py from {csv_name} on {datetime.now():%Y-%m-%d %H:%M} "
        f"(DAG {DAG_NAME})",
        f"-- config table: `{args.config_table}`",
        f"-- output table: `{args.output_table}`",
        f"-- hierarchy:    `{args.hierarchy_table}`",
        f"-- actuals:      `{args.actuals_table}`",
    ]
    files = {
        "create_tables.sql": (
            ["-- Table deployment: the config table (curate_*) and the output table (publish_*) of the DAG.",
             "-- The DAG never creates tables; it only truncates and inserts."],
            create_table_sql(args.config_table, config_types(mod, rows, header).items()) + "\n\n"
            + create_table_sql(args.output_table, mod.output_columns()) + "\n"),
        "load_config_table.sql": (
            [f"-- Replaces the rows of the config table with the rows of {csv_name}.",
             "-- Run after create_tables.sql, and again whenever the CSV changes."],
            f"TRUNCATE TABLE `{args.config_table}`;\n\n"
            f"INSERT INTO `{args.config_table}` ({', '.join(f'`{c}`' for c in header)})\n"
            f"{config_rows_sql(mod, rows, header)};\n"),
        "dag_script.sql": (
            ["-- Exactly what task execute_compiled_sql runs: the base line items go to a temp table, then the",
             "-- output table is truncated and refilled. Reads the config table, so load it first."],
            dag_script),
        "run_all.sql": (
            ["-- Preview: run as one script in the BigQuery console; the last statement shows the output line",
             f"-- items. Uses the rows of {csv_name} (written into this SQL) and writes no tables."],
            f"CREATE TEMP TABLE {mod.BASE_VALUES_TABLE} AS\n{base_sql};\n\n"
            f"{mod.build_formula_sql(plan)}\nORDER BY c.{mod.LINE_ITEM_COL}, c.month_date;\n"),
        "base.sql": (
            [f"-- Preview: the line items fetched from actuals, from the rows of {csv_name} (written into this SQL).",
             "-- Writes no tables."],
            base_sql + ";\n"),
        "hierarchy_check.sql": (
            ["-- Config values that reach deeper hierarchy nodes only through their children. The mapping joins",
             "-- parent = config value once, so actuals booked on those deeper nodes are NOT counted. No rows = fine."],
            check_sql + ";\n" if check_sql else None),
    }

    os.makedirs(args.out_dir, exist_ok=True)
    written = []
    for name, (lines, body) in files.items():
        if body is None:
            continue
        path = os.path.abspath(os.path.join(args.out_dir, name))
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(header_lines + lines) + "\n\n" + body)
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
