# Plan: budget_expense DAG, JSON and generator, on expense's config table

## Context
Driver, revenue and expense each have the full chain: config CSV → DAG + config JSON → local generator →
`generated_sql/`. `buget_expense/` holds only your two hand-written views:
- `corp_challenger_budget_expense_line_item.sql` reads the budget view `od_published_epm.epm_p00exp_plan_bdgt_v`
  (scenario `SC.CV`, version `VR.SUB.8`) through `corp_challenger_expense_config_table`. A view-to-table DAG copies it to
  `corp_challenger_budget_expense_line_item_table`.
- `corp_challenger_budget_expense_line_item_all.sql` reads that table and calculates the 4 non-WLS lines.

This builds the DAG chain for budget_expense:
- It uses expense's config, so there is no new config CSV or config table.
- It reads the budget view instead of actuals.
- As with driver, the DAG replaces the whole view chain. Its `base_values` temp table does the job of
  `…_line_item_table`, so it needs no view-to-table DAG.

## Already checked (read-only, nothing written)
I ran the expense DAG's `prepare_plan` in memory on `config_expense.csv` and compared its SQL with your two views.
- It finds 263 base and 34 formula line items in 2 tiers (33, 1), so 209 line items in the output. There is no
  ROLL_FORWARD or LAG.
- It uses six dimensions, and every base row filters on all six: ProfitCentre, Account, CompanyCode, CostCenter, Market
  and DistributionChannel.
  - Your budget view's SELECT has all six of their columns (`profit_center`, `Account`, `Company_Code`, `Cost_Center`,
    `Market`, `Distribution_Channel`).
  - GeoRegion and Source are `ALL` on every row, so the DAG skips them. The view selects them but filters nothing.
- The DAG already generates the rest of the view's base logic:
  - the year window (today's FY and the 4 before it);
  - the cross join where `ALL` is a wildcard;
  - the grouping by line item, year and period.
- Your 4 non-WLS formulas are already config rows 264–267. The view's `NOT LIKE '%Calculated%'` does what the config's
  FinalOutput `False` on the 88 Calculated rows does.
- **So the only logic change is the scenario/version filter.** Everything else is the source table and the names.

## Differences from your views, by design
The DAG follows the config, as the expense DAG does, so budget and actuals use the same definitions.
1. **Names:** it uses the config's names (`Consumer - Devices non-WLS`), not the view's (`Consumer Devices non-WLS`).
   The view reads its own table, `corp_challenger_expense_config_table`.
2. **Formulas:** it calculates all 34.
   - So the output also gets the 28 `Org Other - …` sums, Total Interest Expense and BTL: the same 209 line items as
     the expense output.
   - The view calculates only the 4 non-WLS lines, so it has no Org Other lines at all.
3. **Hierarchies:**
   - The DAG uses the config's hierarchy names (PROFIT_CENTER, STATISTICS, DISTRIBUTION_CHANNEL), and a value with no
     children maps to itself.
   - The view reads only `is_primary = TRUE` and drops a value with no children.
4. **Amounts are NUMERIC**, as in every DAG.
5. **Zero rows (your choice):** the DAG keeps a formula line item's 0 months, as for expense. The view drops them with
   `HAVING Amount <> 0`. No code is needed for this.

## Changes
### 1. Folder
Rename `buget_expense/` → `budget_expense/`. It is untracked, and the two view files move unchanged.

### 2. `budget_expense/corp_challenger_model_budget_expense_line_item_all.py` (new)
A copy of the expense DAG with these changes:
- the names: docstring line 2 and `dag_configs`;
- the docstring says where the rows come from: TRANSACTION_TABLE (the raw_* keys) is the budget view, read for one
  scenario and version. "Actuals" in the rest of the file means its rows.
- a new setting next to `ACTUALS_AMOUNT_COL`. It does the job of the view's `params` CTE:
  ```python
  BUDGET_FILTERS = {"scenario": "SC.CV", "version": "VR.SUB.8"}   # column -> value; change them here only
  ```
- in `build_base_sql`, the actuals filters start with one `T.<column> = '<value>'` per entry:
  ```sql
  WHERE T.scenario = 'SC.CV'
    AND T.version = 'VR.SUB.8'
    AND T.year IN (...)
  ```
- the `# Raw: the actuals` comment says budget view.

Everything else stays as it is, including the `ACTUALS_*` and `TRANSACTION_TABLE` names and the `actuals` CTE. A diff
against the expense DAG therefore shows only these lines.

### 3. `budget_expense/corp_challenger_model_budget_expense_line_item_all_config.json` (new)
The expense JSON with 3 values changed:

| Key | Value |
|---|---|
| `source_file_name` | `corp_challenger_model_budget_expense_line_item_all` |
| `raw_bq_table` | `epm_p00exp_plan_bdgt_v` (`raw_dataset` stays `od_published_epm`) |
| `publish_bq_table` | `corp_challenger_budget_expense_line_item_all_table` |

`curate_bq_table` stays `corp_challenger_expense_formula_config`, the expense table.

### 4. `budget_expense/generate_budget_expense_sql.py` (new)
A copy of `expense/generate_expense_sql.py` with these changes:
- `DAG_NAME`, `OUTPUT_TABLE` and the names in the docstring and header change. `CONFIG_TABLE` stays expense's.
- `ACTUALS_TABLE` becomes `BUDGET_TABLE = "od_published_epm.epm_p00exp_plan_bdgt_v"`, and `--actuals-table` becomes
  `--budget-table`.
- The `--config` default is `expense/config_expense.csv`, read where it is. There is no copy.
- It doesn't write the config table's DDL and DML, because `expense/generate_expense_sql.py` already does. The output
  DML's header says to load that table first.

### 5. Generated SQL
Run `venv\Scripts\python.exe budget_expense\generate_budget_expense_sql.py -v`. It writes to
`budget_expense/generated_sql/`:
- `ddl/corp_challenger_budget_expense_line_item_all_table.sql`: LineItem, year and month STRING; Amount NUMERIC.
- `dml/corp_challenger_budget_expense_line_item_all_table.sql`: what the DAG runs.
- `dml/ingestion_config_table.sql`.
- the previews: `run_all.sql`, `base.sql` and `hierarchy_check.sql`.

## Verification
1. **Generator output:** `config_expense.csv: 263 base line items, 34 formula line items in 2 tiers (33, 1), 209 line
   items in the output`, with no warnings.
2. **Diffs** (`git diff --no-index`):
   - expense DAG vs budget DAG: only the lines in step 2;
   - the two JSON files: the 3 values;
   - the expense and budget DML: only the header, the source and output table names and the two filter lines. The
     formula SQL is identical.
3. **DAG import:** the budget DAG imports through recording Airflow stubs. Check:
   - its dag_id;
   - the tasks `generate_sql_query >> execute_compiled_sql`;
   - its config path, `dags/<PROJECT_SPACE>/corp_challenger_model_budget_expense_line_item_all/config/…_config.json`;
   - its tables: CONFIG_TABLE is expense's, TRANSACTION_TABLE is the budget view and OUTPUT_TABLE is the budget table.
4. **BigQuery fixture run** in `gcp-tutorial-test-336507`. It uses TEMP tables only; if bq auth has expired, I'll
   report it and skip this step.
   - One fixture runs through both the DAG's SQL and your two views. It holds a slice of the expense config, a
     hierarchy and budget rows.
   - Rows of another scenario or version, and years outside the window, are left out.
   - The base line items and the 4 non-WLS lines match your views. The only differences are the names and the 0 rows
     the view drops.
   - Each Org Other line is the sum of its 3 Calculated rows. GNT, CSS and CMO split by cost centre.
   - Amount is NUMERIC.
5. **Memory:** save the budget_expense rules:
   - the shared config table;
   - BUDGET_FILTERS;
   - the generator reads expense's CSV;
   - 0 rows are kept, unlike the view.

## After it lands (for you)
1. The budget DAG reads expense's config table. Load it with `expense/generated_sql/dml/corp_challenger_expense_formula_config.sql`
   if you haven't yet.
2. Run `ddl/corp_challenger_budget_expense_line_item_all_table.sql`.
3. Register and deploy the DAG:
   1. Replace `<next id>` and run `dml/ingestion_config_table.sql`.
   2. Add the `epm_dag_metadata` row.
   3. Deploy the DAG file.
4. Check the budget periods before the first run. The DAG stops on a period it can't turn into a month, such as 13 or
   BegBalance. Put any such period in `EXCLUDED_PERIODS`.
   ```sql
   SELECT year, period, COUNT(*) AS n FROM `oddo_project.od_published_epm.epm_p00exp_plan_bdgt_v`
   WHERE scenario = 'SC.CV' AND version = 'VR.SUB.8' GROUP BY 1, 2 ORDER BY 1, 2;
   ```
   This also shows the years. The window is FY22–FY26, as in your view, so both would leave out an FY27 budget.
5. For a new version (e.g. VR.SUB.9), edit `BUDGET_FILTERS` and redeploy. If the values match no rows, the run
   empties the output table without an error.

## Not in this change
- An optional follow-up: an `ASSERT` before the TRUNCATE that there are budget rows. A wrong version could then not
  empty the table.
- These stay as they are: the two view files, the expense files and `config_expense.csv`.
- No commit unless you ask.
