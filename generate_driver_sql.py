"""
Writes the BigQuery SQL of the epm_flatten_enrich_driver DAG for a config CSV, to run by hand in the BigQuery console.
It uses the DAG file's own compiler (epm_flatten_enrich_driver.py, loaded with Airflow stubbed out), so the SQL is the
SQL the DAG would run; only the table names come from the command line.

    python generate_driver_sql.py                                  config_driver.csv -> generated_sql/
    python generate_driver_sql.py --project my-project             other project in the default table names
    python generate_driver_sql.py --config-table proj.ds.table     read the config rows from BigQuery instead

By default the CSV rows are written into the SQL, so the CSV does not have to be loaded into BigQuery first.
The dimension map is the DAG file's DEFAULT_DIMENSION_MAP.

Files written:
    run_all.sql          one script: base values into a temp table, then the output line items (writes no tables)
    base.sql             what task materialize_base_values runs (the DAG writes its result to the staging table)
    formula.sql          what task execute_compiled_sql runs; reads the staging table
    hierarchy_check.sql  config values whose deeper hierarchy nodes the mapping misses (no rows = fine)
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
DAG_FILE = os.path.join(HERE, "epm_flatten_enrich_driver.py")
DEFAULT_PROJECT = "vz-it-np-j0nv-dev-oddo-0"
INLINE_CONFIG = "config_csv"                 # CTE with the CSV rows, standing in for the config table
TEMP_STAGING = "base_values"                 # temp table of run_all.sql


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
    spec = importlib.util.spec_from_file_location("epm_flatten_enrich_driver", DAG_FILE)
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


def inline_config(mod, rows, header):
    """The CSV rows as a query standing in for the config table (Config_ID INT64 when all ids are whole numbers)."""
    id_col = mod.CONFIG_ID_COL.lower()
    numeric_ids = all(str(r.get(id_col) or "").isdigit() for r in rows)

    def is_id(col):
        return col.lower() == id_col and numeric_ids

    fields = ", ".join(f"`{c}` {'INT64' if is_id(c) else 'STRING'}" for c in header)
    values = ",\n    ".join(
        "(" + ", ".join("NULL" if r.get(c.lower()) is None else r[c.lower()] if is_id(c) else mod._lit(r[c.lower()])
                        for c in header) + ")"
        for r in rows)
    return f"SELECT * FROM UNNEST(ARRAY<STRUCT<{fields}>>[\n    {values}\n  ])"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=os.path.join(HERE, "config_driver.csv"), help="config CSV (default: %(default)s)")
    p.add_argument("--out-dir", default=os.path.join(HERE, "generated_sql"), help="default: %(default)s")
    p.add_argument("--project", default=DEFAULT_PROJECT, help="project in the default table names (default: %(default)s)")
    p.add_argument("--config-table", help="read the config rows from this table instead of writing the CSV rows into "
                                          "the SQL (the table must hold the same rows as the CSV)")
    p.add_argument("--hierarchy-table", help="default: <project>.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v")
    p.add_argument("--actuals-table", help="default: <project>.od_published_epm.epm_p00all_actuals_fullhist_v")
    p.add_argument("--staging-table", help="where the base.sql result goes and formula.sql reads it from; "
                                           "default: <project>.od_enriched_epmmlops.corp_challenger_driver_base_values")
    p.add_argument("-v", "--verbose", action="store_true", help="also print the dimension checks and formula tiers")
    args = p.parse_args()
    args.hierarchy_table = (args.hierarchy_table
                            or f"{args.project}.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v")
    args.actuals_table = args.actuals_table or f"{args.project}.od_published_epm.epm_p00all_actuals_fullhist_v"
    args.staging_table = (args.staging_table
                          or f"{args.project}.od_enriched_epmmlops.corp_challenger_driver_base_values")
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

    mod.CONFIG_TABLE = args.config_table or INLINE_CONFIG
    mod.HIERARCHY_TABLE, mod.TRANSACTION_TABLE = args.hierarchy_table, args.actuals_table
    mod.STAGING_TABLE = args.staging_table
    base_sql, formula_sql, check_sql = (mod.build_base_sql(plan), mod.build_formula_sql(plan),
                                        mod.adjacency_check_sql(plan))
    mod.STAGING_TABLE = TEMP_STAGING
    formula_from_temp = mod.build_formula_sql(plan)
    if not args.config_table:
        # the CSV rows once per file, as a CTE the SQL reads instead of the config table
        config_cte = f"{INLINE_CONFIG} AS (\n  {inline_config(mod, rows, header)}\n)"
        base_sql = base_sql.replace("WITH\n", f"WITH\n{config_cte},\n", 1).replace(f"`{INLINE_CONFIG}`", INLINE_CONFIG)
        check_sql = check_sql and (f"WITH {config_cte}\n"
                                   + check_sql.replace(f"`{INLINE_CONFIG}`", INLINE_CONFIG))

    csv_name = os.path.basename(args.config)
    header_lines = [
        f"-- Generated by generate_driver_sql.py from {csv_name} on {datetime.now():%Y-%m-%d %H:%M}",
        "-- config:    " + (f"`{args.config_table}`" if args.config_table else f"the rows of {csv_name}, written into this SQL"),
        f"-- hierarchy: `{args.hierarchy_table}`",
        f"-- actuals:   `{args.actuals_table}`",
    ]
    files = {
        "run_all.sql": [
            "-- Run as one script in the BigQuery console: the base line items go to a temp table and the last",
            "-- statement shows the output line items. Writes no tables.",
        ], "base.sql": [
            "-- Base line items (Formula blank) fetched from actuals - what task materialize_base_values runs.",
            f"-- The DAG writes the result to the staging table. By hand: Query settings > Destination table",
            f"-- = `{args.staging_table}`, overwrite table; then run formula.sql.",
        ], "formula.sql": [
            "-- Output line items, the formula ones calculated tier by tier - what task execute_compiled_sql runs.",
            f"-- Reads `{args.staging_table}`: fill it with base.sql first.",
        ], "hierarchy_check.sql": [
            "-- Config values that reach deeper hierarchy nodes only through their children. The mapping joins",
            "-- parent = config value once, so actuals booked on those deeper nodes are NOT counted. No rows = fine.",
        ]}
    bodies = {
        "run_all.sql": (f"CREATE TEMP TABLE {TEMP_STAGING} AS\n{base_sql};\n\n"
                        f"{formula_from_temp}\nORDER BY c.{mod.LINE_ITEM_COL}, c.month_date;\n"),
        "base.sql": base_sql + ";\n",
        "formula.sql": formula_sql + ";\n",
        "hierarchy_check.sql": check_sql + ";\n" if check_sql else None,
    }

    os.makedirs(args.out_dir, exist_ok=True)
    written = []
    for name, lines in files.items():
        if bodies[name] is None:
            continue
        path = os.path.abspath(os.path.join(args.out_dir, name))
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(header_lines + lines) + "\n\n" + bodies[name])
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
