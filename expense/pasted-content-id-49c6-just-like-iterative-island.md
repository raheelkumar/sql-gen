# Plan: expense chain (config, DAG, generator, SQL) + NUMERIC amounts in all three DAGs

## Context
Driver and revenue each have the full chain:
1. `*_config_raw.csv` holds plain-English formulas.
2. `config_*.csv` holds them in the DAG's syntax (`driver/Formula-Conversion-Guide.md`).
3. A DAG and its config JSON.
4. A local generator writes `generated_sql/` (the DDL and DML).

Expense has only `expense/expense_config_raw.csv` (297 rows). This builds the same chain in `expense/`, copied from
revenue's files, which are driver's with the names changed.

You also asked for every amount to be NUMERIC, because long float decimals cause problems. Driver's DAG still runs in
FLOAT64.

## Already checked (read-only, nothing written)
I ran the DAG's own `prepare_plan` and SQL builders in memory on the expense rows, with Airflow stubbed out.
- **The raw header would silently break the DAG.** It says `ProfitCenter`, but the DAG reads `ProfitCentre`. The DAG
  would drop the profit-centre filter and sum every line item across all profit centres, with no error.
  `ContractTerms` vs `ContractTerm` is the same kind of mismatch; it does no harm because every value is `ALL`. The
  config gets the driver/revenue header.
- With that header: 263 base + 34 formula line items in 2 tiers (33, 1), so 209 line items in the output. The formulas
  are plain arithmetic; there is no ROLL_FORWARD or LAG.
- Six dimensions filter every base row: ProfitCentre, Account, CompanyCode, **CostCenter**, Market and
  DistributionChannel.
  - CostCenter has never been filtered before. GNT, CSS and CMO share PC.0.1150000 and differ only by cost centre.
  - It has no hierarchy-name column, so the DAG uses the primary `Cost_Center` hierarchy.
  - The deployed `driver_line_item.sql` view already uses the dimension name `Cost_Center` and the actuals column of
    the same name.
- Names containing `( )` or `/` are safe: they only appear as string literals.
- The DAG script is about 37 KB, the same size as driver's.
- **So the DAG needs no logic change for expense.** The only code change is the NUMERIC one.

## Your decisions
- NUMERIC in **all three DAGs**, including a cast of the actuals.
- Row 11 is renamed to `Calculated Business - Devices WLS`.
- Carried over from revenue:
  - formula rows are outputs (they are already blank in the raw file);
  - `Calculated …` rows get `False`;
  - `MLProcessing` stays;
  - data fixes go in `config_expense.csv` only.

## Changes
### 1. NUMERIC amounts: the driver and revenue DAG files
Make the same edits in `driver/corp_challenger_model_driver_line_item_all.py` and
`revenue/corp_challenger_model_revenue_line_item_all.py`:
- `AMOUNT_TYPE = "NUMERIC"`. Driver changes from FLOAT64; revenue already has it. Its comment changes to say that
  every amount uses this type.
- In the `build_base_sql` actuals CTE, `SUM(T.amount)` becomes `SUM(CAST(T.amount AS {AMOUNT_TYPE}))`.
  - The `base_values` temp table is then NUMERIC, whatever type the actuals view has.
  - Everything after it already uses `AMOUNT_TYPE`: monthly_summary, the tiers, number literals and the output.
- Afterwards the two files differ only in names and docstrings.
- Driver's averages, churn rates and roll forwards become NUMERIC: exact, with 9 decimals.

### 2. `expense/config_expense.csv` (new)
A one-off script in the scratchpad makes it, checking every step. The script is not committed.
- **Header:** the raw file's 25 columns, with `ProfitCenter` → `ProfitCentre` and `ContractTerms` → `ContractTerm`.
  It then matches `config_revenue.csv` exactly.
- **Rows:** Config_IDs 1–297 in the same order, each padded to 25 fields.
- **Two LineItem fixes:**
  - Row 11: `Calculated Business - Device WLS` → `Calculated Business - Devices WLS` (your choice). Formula 266 then
    resolves as written.
  - Row 285: `Org Other -Building and Facilities` → `Org Other - Building and Facilities`. This matches the other 27
    `Org Other - …` outputs and the fix made to driver's `Business- Fios`. This row is an output name.
- **Formulas:** unchanged. They are already in the guide's `[A] - [B]` and `[A] + [B] + [C]` forms. The script fails if
  a reference does not match a LineItem exactly.
- **FinalOutput:** `FALSE` → `False` on the 88 `Calculated …` rows. Every other row stays blank, which means output.
- **Hierarchy names:** already filled on all 263 base rows, with the values the revenue rules give.
  - No expense profit centre or account is in `hierarchy_name.csv`, so they get PROFIT_CENTER and STATISTICS.
  - The other four columns have that file's single values.
  - The script checks this; nothing changes.
- **Layout:** LF line endings and no trailing newline, like `config_revenue.csv`.
- `expense_config_raw.csv` is not changed.

### 3. Expense DAG, JSON and generator (new copies)
A script copies the revenue files after step 1. It replaces `revenue` with `expense`, which has the same length, so
alignment holds. It also checks how many replacements it made.
- `expense/corp_challenger_model_expense_line_item_all.py`: 3 lines change (docstring lines 2 and 4, `dag_configs`).
- `expense/corp_challenger_model_expense_line_item_all_config.json`: 3 values change. The views, datasets, schedule
  (`30 2 * * *`) and `<next id>` stay the same.

  | Key | Value |
  |---|---|
  | `source_file_name` | `corp_challenger_model_expense_line_item_all` |
  | `curate_bq_table` | `corp_challenger_expense_formula_config` |
  | `publish_bq_table` | `corp_challenger_expense_line_item_all_table` |
- `expense/generate_expense_sql.py`: 13 lines change.
  - the docstring;
  - `DAG_NAME`, `CONFIG_TABLE` and `OUTPUT_TABLE`;
  - the `--config` default, which becomes `config_expense.csv`;
  - the "Generated by" header.

### 4. Generated SQL
Run `venv\Scripts\python.exe expense\generate_expense_sql.py -v`. It writes these files to `expense/generated_sql/`:
- `ddl/corp_challenger_expense_formula_config.sql`: Config_ID INT64, FinalOutput BOOL, every other column STRING,
  including MLProcessing.
- `ddl/corp_challenger_expense_line_item_all_table.sql`: LineItem, year and month STRING; **Amount NUMERIC**.
- `dml/corp_challenger_expense_formula_config.sql`: TRUNCATE, then INSERT of the 297 rows.
- `dml/corp_challenger_expense_line_item_all_table.sql`: what the DAG runs.
- `dml/ingestion_config_table.sql`.
- Previews: `run_all.sql`, `base.sql` and `hierarchy_check.sql`.

Step 1 changes the driver and revenue SQL, so rerun `driver\generate_driver_sql.py` and
`revenue\generate_revenue_sql.py` too:
- **Driver:** the output DDL becomes `Amount NUMERIC`, and the output DML and the previews get the cast.
  - Those 4 files were already out of date. They were generated on 2026-11-02, before the SafeLink fix and the fiber
    and Beginning rows. Their diff therefore also brings in those config rows.
  - The config DDL, config DML and ingestion SQL change only in their timestamp line.
- **Revenue:** the output DML and the previews get the cast. The other files change only in their timestamp line.

## Verification
1. **Generator output:**
   - expense: `263 base line items, 34 formula line items in 2 tiers (33, 1), 209 line items in the output`, with no
     warnings;
   - driver, unchanged: `56 base, 42 formula in 4 tiers (16, 10, 8, 8), 70 output`;
   - revenue, unchanged: `30 base, 11 formula in 3 tiers (8, 2, 1), 29 output`, plus its expected Corporate FTR
     warning.
2. **Offline checks** (a scratchpad script, not committed):
   - Compare `config_expense.csv` with the raw file field by field. Only the header names, the padding, the
     FinalOutput spelling and rows 11 and 285 may differ.
   - Every formula reference is an exact, case-sensitive LineItem.
   - `git diff --no-index` shows only:
     - names and docstrings between the driver and revenue DAGs (the AMOUNT_TYPE line no longer differs);
     - the 3 lines between the revenue and expense DAGs;
     - the matching name changes between the generators and between the JSON files.
   - None of the three `generated_sql/` folders contains `FLOAT64`, and each contains `SUM(CAST(T.amount AS NUMERIC))`.
   - The expense DAG imports through recording Airflow stubs:
     - its dag_id;
     - tasks `generate_sql_query >> execute_compiled_sql`;
     - its config path, `dags/<PROJECT_SPACE>/corp_challenger_model_expense_line_item_all/config/…_config.json`;
     - the expense tables.
3. **BigQuery fixture run** in `gcp-tutorial-test-336507`, as in earlier sessions. It uses TEMP tables only. If bq auth
   has expired, I'll report it and skip this step.
   - **Expense checks:**
     - GNT, CSS and CMO share a profit centre, and each gets only its own cost centre's actuals.
     - CO.DW1000 rows filter by company. CO.PRIM_EXT_SG rows take every company under it.
     - Each Org Other line is the sum of its 3 Calculated rows.
     - Each non-WLS line is Calculated − WLS.
     - Total Interest feeds BTL correctly.
     - The output holds exactly the 209 output line items.
   - **NUMERIC checks:**
     - FLOAT64 fixture actuals of 0.1 + 0.2 give a base Amount of exactly 0.3, as NUMERIC.
     - Amount is NUMERIC in the result for all three DAGs.
     - Driver's LAG, ROLLING_AVG, SAFE_DIVIDE and ROLL_FORWARD all run in NUMERIC.
4. **Memory:**
   - save the expense conversion rules: the header fix, CostCenter, and the two slips;
   - save the rule that every amount is NUMERIC;
   - update the revenue memory.

## After it lands (for you)
1. **Expense:**
   1. Run the two DDL files, then `dml/corp_challenger_expense_formula_config.sql`.
   2. Replace `<next id>` and run `dml/ingestion_config_table.sql`.
   3. Add the `epm_dag_metadata` row, as you did for driver.
   4. Deploy the DAG file.
2. **Driver and revenue:** deploy both DAG files. Then find the output tables that still store FLOAT64:
   ```sql
   SELECT table_name, data_type
   FROM `oddo_project.od_enriched_epmmlops.INFORMATION_SCHEMA.COLUMNS`
   WHERE column_name = 'Amount' AND table_name LIKE 'corp_challenger_%_line_item_all_table';
   ```
   BigQuery can't change a FLOAT64 column to NUMERIC in place, and `CREATE TABLE IF NOT EXISTS` skips a table that
   already exists. Re-create each FLOAT64 table instead. This keeps its rows:
   ```sql
   CREATE OR REPLACE TABLE `oddo_project.od_enriched_epmmlops.corp_challenger_driver_line_item_all_table` AS
   SELECT LineItem, year, month, CAST(Amount AS NUMERIC) AS Amount
   FROM `oddo_project.od_enriched_epmmlops.corp_challenger_driver_line_item_all_table`;
   ```
   Until you do, the DAG still runs: NUMERIC values are converted to FLOAT64 on insert.
3. **Check that the hierarchies hold the expense parents.** A value with no rows in its selected hierarchy maps only to
   itself. Its line items then come out as 0 without an error. The biggest risks are:
   - the AC.IS* and AC.7xxxx accounts under STATISTICS, as with revenue's AC.4xxx accounts;
   - CO.PRIM_EXT_SG and the CC.* values in the primary hierarchies.

   ```sql
   SELECT dimension_name, parent, hierarchy_name, is_primary, COUNT(DISTINCT node) AS nodes
   FROM `oddo_project.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v`
   WHERE (dimension_name = 'Profit_Center' AND parent IN ('PC.20P2100000', 'PC.20P1100000', 'PC.0.1150000',
           'PC.11119', 'PC.20P3000000', 'PC.20P1200000', 'PC.20P2200000', 'PC.20P0000000'))
      OR (dimension_name = 'Cost_Center' AND parent IN ('CC.PRIM_CC', 'CC.0.400000', 'CC.0.114440',
           'CC.F000054070', 'CC.0.448710'))
      OR (dimension_name = 'Company_Code' AND parent IN ('CO.PRIM_EXT_SG', 'CO.DW1000'))
      OR (dimension_name = 'Account' AND (parent LIKE 'AC.IS%' OR parent IN ('AC.67000', 'AC.71105', 'AC.71111',
           'AC.71115', 'AC.71205', 'AC.75000', 'AC.79100')))
   GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3;
   ```
   What you want to see:
   - every profit centre has rows under PROFIT_CENTER;
   - every account has rows under STATISTICS;
   - every cost centre and company has `is_primary = TRUE` rows.

## Not in this change
- An optional follow-up: a DAG check that fails on a config column the dimension map doesn't know. It would have
  caught `ProfitCenter`.
- The hand-written `driver_line_item_all.sql`, the legacy view chain, divides by `2.0`. That is a FLOAT64 literal, so
  its averages stay FLOAT64.
- These stay as they are: `expense_config_raw.csv`, `config_driver.csv`, `config_revenue.csv` and
  `Formula-Conversion-Guide.md`.
- No commit unless you ask.
