# Plan: budget_revenue DAG, JSON and generator, on revenue's config table

## Context
Driver, revenue, expense and budget_expense each have the full chain: config CSV → DAG + config JSON → local
generator → `generated_sql/`. `budget_revenue/` holds only your two hand-written views:
- `corp_challenger_budget_revenue_line_item.sql` reads the budget view `od_published_epm.epm_p00rev_plan_bdgt_v`
  (scenario `SC.CV`, version `VR.SUB.8`) through revenue's config table. A view-to-table DAG copies it to
  `corp_challenger_budget_revenue_line_item_table`.
- `corp_challenger_budget_revenue_line_item_all.sql` reads that table and calculates 2 lines: Consumer and Business
  Volume/Services.

This builds the same chain for budget_revenue, exactly as budget_expense did for expense:
- It uses revenue's config (`config_revenue.csv` / `corp_challenger_revenue_formula_config`), so there is no new config
  CSV, config table, config DDL or config DML.
- It reads the budget view instead of actuals, for one scenario and version.
- The DAG's `base_values` temp table does the job of `…_line_item_table`, so no view-to-table DAG is needed.

## Already checked (read-only, nothing written)
I ran the revenue DAG's `prepare_plan` in memory on `config_revenue.csv`:
- 30 base and 11 formula line items in 3 tiers (8, 2, 1), so 29 line items in the output. There is no ROLL_FORWARD or
  LAG.
- 4 dimensions are used: ProfitCentre, Account, CompanyCode, Market. The budget view's SELECT has all four columns
  (`profit_center`, `Account`, `Company_Code`, `Market`). The other 9, including CostCenter (which the view doesn't
  select), are `ALL` on every row, so the DAG skips them.
- The DAG already generates the rest of the view's base logic: the 5-year FY window, `ALL` as a wildcard, and grouping
  by line item, year and period.
- Your 2 formulas are already config rows 69–70. The view's `NOT LIKE '%Calculated%'` does what FinalOutput `False`
  does on the Calculated rows.
- **So the only logic change is the scenario/version filter**, the same `BUDGET_FILTERS` change as budget_expense.
- There is an existing revenue warning: `Calculated Corporate - FTR Only Total WLN Operating Revenue` is neither output
  nor used by a formula. It will show here too. I won't change it.

## Differences from your views, by design (same choices as budget_expense)
1. **Names:** the config's names (`Consumer - Volume/Services`), not the view's (`Consumer Volume/Services`).
2. **Formulas:** it calculates all 11, the same 29 output line items as revenue: Fiber, Mobility and Broadband Service
   Revenue, the WLN Less Fiber lines and Corporate and Other, not just the 2 Volume/Services lines.
3. **Hierarchies:** the DAG uses the config's hierarchy names. The 6 Consumer/Business VZ/FTR Fiber and WLN rows use
   `ALT_PC_VZ_FTR_Split`; the view reads only `is_primary = TRUE`, so its numbers for those lines can differ.
4. **Amounts are NUMERIC**, as in every DAG.
5. **Zero rows:** neither drops them (this view has no `HAVING Amount <> 0`).

## Changes
### 1. `budget_revenue/corp_challenger_model_budget_revenue_line_item_all.py` (new)
A copy of the revenue DAG with exactly the budget_expense changes (see the expense → budget_expense diff):
- docstring lines 1–3 (names; "actuals" means the budget view's rows of one scenario/version) and `dag_configs`;
- after `ACTUALS_AMOUNT_COL`:
  ```python
  BUDGET_FILTERS = {"scenario": "SC.CV", "version": "VR.SUB.8"}
  ```
  with the same comment as budget_expense, citing `corp_challenger_budget_revenue_line_item.sql`;
- in `build_base_sql`: `filters = [f"T.{col} = {_lit(value)}" for col, value in BUDGET_FILTERS.items()]`;
- the `# Raw: the actuals` comment says budget view.

A diff against the revenue DAG shows only these lines.

### 2. `budget_revenue/corp_challenger_model_budget_revenue_line_item_all_config.json` (new)
The revenue JSON with 3 values changed:

| Key | Value |
|---|---|
| `source_file_name` | `corp_challenger_model_budget_revenue_line_item_all` |
| `raw_bq_table` | `epm_p00rev_plan_bdgt_v` (`raw_dataset` stays `od_published_epm`) |
| `publish_bq_table` | `corp_challenger_budget_revenue_line_item_all_table` |

`curate_bq_table` stays `corp_challenger_revenue_formula_config`.

### 3. `budget_revenue/generate_budget_revenue_sql.py` (new)
A copy of `budget_expense/generate_budget_expense_sql.py` with expense → revenue:
- `DAG_NAME`, `OUTPUT_TABLE`, docstring and header names;
- `CONFIG_CSV = ../revenue/config_revenue.csv`, `CONFIG_TABLE = …corp_challenger_revenue_formula_config`;
- `BUDGET_TABLE = "od_published_epm.epm_p00rev_plan_bdgt_v"`, `--budget-table`;
- no config DDL/DML; the output DML header says to load `revenue/generated_sql/dml/corp_challenger_revenue_formula_config.sql` first.

### 4. Generated SQL
Run `venv\Scripts\python.exe budget_revenue\generate_budget_revenue_sql.py -v`. It writes to
`budget_revenue/generated_sql/`:
- `ddl/corp_challenger_budget_revenue_line_item_all_table.sql` (LineItem, year, month STRING; Amount NUMERIC);
- `dml/corp_challenger_budget_revenue_line_item_all_table.sql` (what the DAG runs);
- `dml/ingestion_config_table.sql`;
- the previews `run_all.sql`, `base.sql`, `hierarchy_check.sql`.

## Verification
1. **Generator output:** `config_revenue.csv: 30 base line items, 11 formula line items in 3 tiers (8, 2, 1), 29 line
   items in the output`, with only the existing FTR Only warning.
2. **Diffs** (`git diff --no-index`):
   - revenue DAG vs budget DAG: only the lines in step 1;
   - budget_expense DAG vs budget_revenue DAG: only expense→revenue names (proves parity with budget_expense);
   - the two JSONs: the 3 values;
   - budget_expense vs budget_revenue generator: only expense→revenue names and tables;
   - revenue vs budget_revenue output DML: only header, source/output table names and the two filter lines.
3. **DAG import** through the generator's Airflow stubs: dag_id, `generate_sql_query >> execute_compiled_sql`, config
   path `dags/<PROJECT_SPACE>/corp_challenger_model_budget_revenue_line_item_all/config/…_config.json`, and the tables
   (CONFIG_TABLE revenue's, TRANSACTION_TABLE the budget view, OUTPUT_TABLE the budget table).
4. **BigQuery fixture run** in `gcp-tutorial-test-336507` (TEMP tables only; if bq auth has expired I'll report it and
   skip this step). One fixture of a slice of the revenue config, a hierarchy (incl. an `ALT_PC_VZ_FTR_Split` node) and
   budget rows, run through the DAG SQL and your two views:
   - rows of another scenario/version and years outside the window are left out;
   - base line items and the 2 Volume/Services lines match the views, apart from names and the alt-hierarchy rows;
   - Amount is NUMERIC.
5. **Memory:** save a budget_revenue note (shares revenue's config, BUDGET_FILTERS, reads `epm_p00rev_plan_bdgt_v`,
   generator reads revenue's CSV), linked to the budget_expense one.

## After it lands (for you)
1. Load revenue's config table with `revenue/generated_sql/dml/corp_challenger_revenue_formula_config.sql` if you
   haven't yet.
2. Run `ddl/corp_challenger_budget_revenue_line_item_all_table.sql`.
3. Replace `<next id>` and run `dml/ingestion_config_table.sql`, add the `epm_dag_metadata` row, deploy the DAG file.
4. Check the budget periods before the first run (the DAG stops on a period it can't turn into a month, e.g. 13 or
   BegBalance; put such periods in `EXCLUDED_PERIODS`):
   ```sql
   SELECT year, period, COUNT(*) AS n FROM `oddo_project.od_published_epm.epm_p00rev_plan_bdgt_v`
   WHERE scenario = 'SC.CV' AND version = 'VR.SUB.8' GROUP BY 1, 2 ORDER BY 1, 2;
   ```
5. For a new version, edit `BUDGET_FILTERS` and redeploy. Values that match no rows empty the output table without an
   error.

## Not in this change
- The two view files, the revenue files and `config_revenue.csv` stay as they are.
- If a revenue config row ever filters CostCenter, the DAG would read `Cost_Center` from the budget view, which your
  view doesn't select. Check the view has that column before adding such a row.
- No commit unless you ask.
