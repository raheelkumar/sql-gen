# Plan: deploying the formula DAG as `corp_challenger_model_driver_line_item_all`

## Context
**How your deployment works:**
- `epm_dag_metadata` holds `dag_Name`, `config` (the folder) and `config_path` for each DAG.
- `ingestion_config_table` holds the config.json values, keyed by `source_file_name` = `dag_Name`.
- `config_creation_dag` turns these into each DAG's config.json.
- Input and output tables are named in the raw/curate/enrich/publish columns.
- Output tables are **created by a separate deployment**; DAGs only insert into them.
- DAGs are named `corp_challenger_*`, and **the Python file has the same name as the DAG**.

**Where the current `epm_flatten_enrich_driver.py` doesn't fit:**
- Its name doesn't follow the convention.
- It creates tables (`CREATE_IF_NEEDED`, `WRITE_TRUNCATE`) instead of inserting.
- It needs a second permanent table, the staging table.

**Outcome:**
1. The DAG is renamed.
2. The rows for both metadata tables.
3. Scripts to create the two new tables and load config_driver.csv.
4. The DAG only truncates and inserts into the pre-created output table, like `corp_challenger_model_driver_line_item_view_to_table.sql`.

## 0. Naming
- **DAG name:** `corp_challenger_model_driver_line_item_all`.
  - It follows the `corp_challenger_model_driver_line_item_view_to_table` pattern.
  - It produces what driver_line_item_all.sql produced, into `corp_challenger_driver_line_item_all_table`.
- **File:** rename `epm_flatten_enrich_driver.py` → `corp_challenger_model_driver_line_item_all.py`, and set `dag_configs = ["corp_challenger_model_driver_line_item_all"]`.
- **Everything else follows from the name:**
  - the config folder and file;
  - `source_file_name`;
  - the optional dimension-map table `<enrich_dataset>.corp_challenger_model_driver_line_item_all_dimensions`.
- `generate_driver_sql.py` is a local tool, not a DAG, so it keeps its name. It loads the renamed file.

## 1. Metadata rows
### `epm_dag_metadata`
| dag_Name | config | config_path |
|---|---|---|
| `corp_challenger_model_driver_line_item_all` | `corp_challenger_model_driver_line_item_all` | `dags/<PROJECT_SPACE>/corp_challenger_model_driver_line_item_all/config/corp_challenger_model_driver_line_item_all_config.json` (write `<PROJECT_SPACE>` the way your other rows do) |

This is the path the DAG reads: `dags/{PROJECT_SPACE}/{DAG_NAME}/config/{DAG_NAME}_config.json`.

### `ingestion_config_table` (becomes config.json)
✔ = read by this DAG. All other columns are ignored; give them your usual values.

| Column | Value |
|---|---|
| id | your next id |
| source_system | `challenger_model` |
| source_file_name ✔ | `corp_challenger_model_driver_line_item_all` (= dag_Name) |
| raw_dataset ✔ / raw_bq_table ✔ | `od_published_epm` / `epm_p00all_actuals_fullhist_v`: the actuals, as in driver_line_item.sql |
| curate_dataset ✔ / curate_bq_table ✔ | `od_enriched_epmmlops` / `corp_challenger_driver_formula_config`: **new**, holds the config_driver.csv rows |
| enrich_dataset ✔ / enrich_bq_table ✔ | `od_enriched_edmcs` / `edm_master_incl_alt_hierarchies_v`: the hierarchy view |
| publish_dataset ✔ / publish_bq_table ✔ | `od_enriched_epmmlops` / `corp_challenger_driver_line_item_all_table`: **new** output table |
| schedule ✔ | `30 2 * * *` |
| bq_conn_id ✔ | `sa-vz-it-j0nv-oddo-0-app` |
| operational_dataset ✔ | `od_operational` (required key) |
| active_flag / frequency | `1` / `Once a day` |
| write_disposition_mode / create_disposition_mode | `WRITE_TRUNCATE` / `CREATE_NEVER` (descriptive only; not read) |
| raw_gcs_bucket, input_file_path, delimiter, sa_account_name, per_allowed_bad_records, regex_pattern | not used; copy them from the existing challenger_model row |
| schema, source_format, raw_gcs_archive_bucket, lookup_datasets, processing_file, audit_file_regex_pattern | `null` |

Both new table names are suggestions. They're new tables because the old chain still reads `corp_challenger_driver_config_table` and reloads `corp_challenger_driver_line_item_table` every day.

## 2. New tables (for your table deployment)
| Table | Schema | Contents |
|---|---|---|
| `od_enriched_epmmlops.corp_challenger_driver_formula_config` | `Config_ID INT64`, then every other config_driver.csv column as `STRING` (24 columns) | the config_driver.csv rows |
| `od_enriched_epmmlops.corp_challenger_driver_line_item_all_table` | `LineItem STRING, year STRING, month STRING, Amount FLOAT64` | filled by the DAG |

- The INSERT names its columns, so extra nullable audit columns in the output table are fine.
- No staging table is needed any more.

## 3. DAG change: truncate + insert only (`corp_challenger_model_driver_line_item_all.py`)
**Tasks:** `generate_sql_query` (PythonOperator) → `execute_compiled_sql` (BigQueryInsertJobOperator). `materialize_base_values` is removed.

**`execute_compiled_sql` runs one script**, no `destinationTable` and no write or create disposition:
```sql
CREATE TEMP TABLE base_values AS <base SQL>;          -- base line items, computed once, gone after the run
BEGIN TRANSACTION;                                    -- only if BigQuery accepts TRUNCATE inside a transaction
TRUNCATE TABLE `<OUTPUT_TABLE>`;
INSERT INTO `<OUTPUT_TABLE>` (LineItem, year, month, Amount)
<formula SQL reading base_values>;
COMMIT TRANSACTION;
```
- **Same pattern as `corp_challenger_model_driver_line_item_view_to_table.sql`**, but with a column list on the INSERT.
- **Transaction:** BigQuery should accept TRUNCATE inside one; this gets tested first. With the transaction, readers never see an empty table.
- **Fallback if it doesn't:** plain `TRUNCATE` + `INSERT`. The table is then empty only while the INSERT runs, which takes seconds because the base values are already in the temp table.

**New and changed functions:**
- `build_dag_script(plan)` builds the script above.
- `build_formula_sql` reads `base_values` instead of the staging table.
- Its final SELECT casts `year` and `month` to STRING, so the output schema is fixed.

**Dry run in `generate_sql_query`:** it dry-runs `INSERT INTO <OUTPUT_TABLE> (...) <formula SQL with base_values replaced by the base SQL>`. One check covers:
- every source table;
- the generated SQL;
- that the output table exists, can be written to, and has the right column types.

**Removed:** `STAGING_TABLE` and the `staging_table_name` key, plus their entries in `DAG_CFG`, `_check_settings` and the docstring.

## 4. Generator change (`generate_driver_sql.py`)
**Changes:**
- `DAG_FILE` → `corp_challenger_model_driver_line_item_all.py`.
- The docstring and file headers name the new DAG and its tasks.

**Files in `generated_sql/`:**

| File | Contents | Config source |
|---|---|---|
| `create_tables.sql` | `CREATE TABLE IF NOT EXISTS` for the config and output tables (section 2) | — |
| `load_config_table.sql` | `TRUNCATE` + `INSERT` of the config_driver.csv rows into the config table | — |
| `dag_script.sql` | exactly what the DAG runs; writes the output table | reads the config table |
| `run_all.sql` | preview; writes nothing | CSV rows |
| `base.sql` | preview; writes nothing | CSV rows |
| `hierarchy_check.sql` | preview; writes nothing | CSV rows |

`formula.sql` (the staging-table version) is dropped.

**Arguments:** `--config-table` and `--output-table` take the section 2 defaults under `--project`. `--staging-table` goes.

## 5. Reference config JSON
Add `corp_challenger_model_driver_line_item_all_config.json` to the repo: the section 1 row in config.json's shape. You can also upload it to the config path to test before `config_creation_dag` runs.

## Verification
**BigQuery** (gcp-tutorial-test-336507, fixture data in TEMP tables, nothing permanent):
- First, check whether BigQuery accepts `TRUNCATE` inside a transaction, which decides whether the script uses one.
- Run the generated `dag_script.sql` against a temp output table:
  - check the 108 hand-checked rows;
  - run it twice and check the count is still 108, so TRUNCATE replaces rather than appends.
  - If temp tables can't stand in for the output table, I'll ask you before creating a scratch dataset.
- Run `load_config_table.sql` against a temp copy of the config table and check all 76 rows.

**Rerun the existing suites**, updated for the new file name and the removed staging table:
- offline compiler checks;
- inline BigQuery fixture tests;
- real-table checks;
- generator checks.

**DAG import** through the offline harness:
- dag_id `corp_challenger_model_driver_line_item_all`;
- 2 tasks;
- task 2's query is task 1's XCom;
- no `destinationTable`;
- the config JSON is read from the `corp_challenger_model_driver_line_item_all` folder.

**Leftovers:** no file named `epm_flatten_enrich_driver.py` remains, and no reference to it remains in the repo.
