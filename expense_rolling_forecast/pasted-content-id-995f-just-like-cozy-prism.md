# Plan: expense_rolling_forecast DAG, JSON and generator, on expense's config table

## Context
`expense_rolling_forecast/` holds only your two hand-written views. They are budget_expense's views with:
- the `rolling_forecast_expense` names, and `line_item_all` reading `…_rolling_forecast_expense_line_item_table`;
- `params` = `'SC.OL'` / `'VR.SUB.10'` instead of `'SC.CV'` / `'VR.SUB.8'`.

The source (`od_published_epm.epm_p00exp_plan_bdgt_v`), the config table, the hierarchy logic and the 4 non-WLS
formulas are the same. `git diff --no-index` against budget_expense's views shows only those lines.

This builds the budget_expense chain for the rolling forecast:
- It uses expense's config (`expense/config_expense.csv` / `corp_challenger_expense_formula_config`). There is no new
  config CSV, config table, config DDL or config DML. You wrote "use the expense_rolling_forecast one" for the config
  table; I read that as expense's table, as the rest of your message says.
- The DAG's `base_values` temp table does the job of `…_line_item_table`, so no view-to-table DAG is needed.

## Already checked (read-only, nothing written)
I ran the budget_expense DAG's compiler in memory on `config_expense.csv`, with the forecast values swapped in:
- 263 base and 34 formula line items in 2 tiers (33, 1), so 209 line items in the output.
- 6 dimensions are filtered (ProfitCentre, Account, CompanyCode, CostCenter, Market, DistributionChannel). The
  forecast view's `budget` CTE selects all six columns.
- The generated filter is the view's `params`:
  `WHERE T.scenario = 'SC.OL' AND T.version = 'VR.SUB.10' AND T.year IN (<FY26 … FY22>)`.
- **So the DAG needs no logic change.** budget_expense's `BUDGET_FILTERS` mechanism already covers it, and only its
  two values differ.

## Names

| | budget_expense | expense_rolling_forecast |
|---|---|---|
| DAG (dag_id, `.py`, JSON `source_file_name`) | `corp_challenger_model_budget_expense_line_item_all` | `corp_challenger_model_rolling_forecast_expense_line_item_all` |
| Output table (`publish_bq_table`) | `corp_challenger_budget_expense_line_item_all_table` | `corp_challenger_rolling_forecast_expense_line_item_all_table` |
| Generator | `generate_budget_expense_sql.py` | `generate_expense_rolling_forecast_sql.py` |
| `BUDGET_FILTERS` | `SC.CV` / `VR.SUB.8` | `SC.OL` / `VR.SUB.10` |
| Source (`raw_bq_table`) | `epm_p00exp_plan_bdgt_v` | same |
| Config table (`curate_bq_table`) | `corp_challenger_expense_formula_config` | same |

- The DAG and table names follow your views' `rolling_forecast_expense` order, as budget_expense's follow its views.
- The generator follows the folder name, as `generate_<folder>_sql.py` does in every other folder.

## Differences from your views, by design (the same choices as budget_expense)
1. **Names:** the config's names (`Consumer - Devices non-WLS`), not the view's (`Consumer Devices non-WLS`).
2. **Formulas:** it calculates all 34, so the 209 line items include the Org Other sums, Total Interest and BTL.
3. **Hierarchies:**
   - The DAG uses the config's hierarchy names, and a value with no children maps to itself.
   - The view reads `is_primary = TRUE` only and drops such a value.
4. **Amounts are NUMERIC**, as in every DAG.
5. **Zero rows:** the DAG keeps a formula's 0 months. The view drops them with `HAVING Amount <> 0`.

## Changes
### 1. `expense_rolling_forecast/corp_challenger_model_rolling_forecast_expense_line_item_all.py` (new)
The budget_expense DAG with `budget_expense` → `rolling_forecast_expense`, plus:
- `BUDGET_FILTERS = {"scenario": "SC.OL", "version": "VR.SUB.10"}`;
- the docstring: "on the rolling forecast". It is the same code as the budget_expense DAG, on the same config table
  and budget view; only the scenario and version differ.
- the `# Raw:` comment names the rolling forecast's scenario / version;
- lines that the longer name pushes past 120 characters are re-wrapped.

It keeps the name `BUDGET_FILTERS`, because it filters the same budget view and your view calls that CTE `budget`.
`build_base_sql` and the generator are then unchanged, and a diff against budget_expense shows only the names, the
docstrings and the two values.

### 2. `expense_rolling_forecast/corp_challenger_model_rolling_forecast_expense_line_item_all_config.json` (new)
The budget_expense JSON with 2 values changed: `source_file_name` and `publish_bq_table` (see the table above).

### 3. `expense_rolling_forecast/generate_expense_rolling_forecast_sql.py` (new)
`budget_expense/generate_budget_expense_sql.py` with the new `DAG_NAME` and `OUTPUT_TABLE`, and the new names in its
docstring and header.

These stay as they are:
- `CONFIG_CSV` is `../expense/config_expense.csv`, read where it is.
- `CONFIG_TABLE` is expense's.
- `BUDGET_TABLE` and `--budget-table` stay.
- It writes no config DDL/DML. The output DML header says to load expense's config table first.

### 4. Generated SQL
Run `venv\Scripts\python.exe expense_rolling_forecast\generate_expense_rolling_forecast_sql.py -v`. It writes to
`expense_rolling_forecast/generated_sql/`:
- `ddl/corp_challenger_rolling_forecast_expense_line_item_all_table.sql`: LineItem, year and month STRING; Amount
  NUMERIC.
- `dml/corp_challenger_rolling_forecast_expense_line_item_all_table.sql`: what the DAG runs.
- `dml/ingestion_config_table.sql`.
- the previews: `run_all.sql`, `base.sql` and `hierarchy_check.sql`.

## Verification
1. **Generator output:** `config_expense.csv: 263 base line items, 34 formula line items in 2 tiers (33, 1), 209 line
   items in the output`, with no warnings.
2. **Diffs** against budget_expense (`git diff --no-index`):
   - DAG: only the names, the docstrings and the two filter values;
   - JSON: only the two values;
   - generator: only the names and their alignment;
   - output DML and `ingestion_config_table.sql`: only the header, the output table name and `'SC.OL'` / `'VR.SUB.10'`.
     The formula SQL is identical.
3. **DAG import** through recording Airflow stubs. Check:
   - the dag_id;
   - the tasks `generate_sql_query >> execute_compiled_sql`;
   - the config path, `dags/<PROJECT_SPACE>/corp_challenger_model_rolling_forecast_expense_line_item_all/config/…_config.json`;
   - the tables: CONFIG_TABLE is expense's, TRANSACTION_TABLE is the budget view, OUTPUT_TABLE is the new table.
4. **BigQuery fixture run** in `gcp-tutorial-test-336507`. It uses TEMP tables only. If bq auth has expired, I'll
   report it and skip this step.
   - One fixture runs through both the DAG's SQL and your two views. It holds:
     - a slice of the expense config: the GNT/CSS/CMO cost-centre split, the 4 WLS pairs and an Org Other sum;
     - a hierarchy;
     - plan rows for SC.OL / VR.SUB.10, plus decoys: SC.CV / VR.SUB.8 (the budget), VR.SUB.9, FY21 and FY27.
   - Expected results:
     - Only SC.OL / VR.SUB.10 rows in FY22–FY26 count. Both the DAG and the view leave out every decoy, FY27 included.
     - The base line items and the 4 non-WLS lines match your views. The only differences are the names and the 0
       rows the view drops.
     - Amount is NUMERIC.
5. **Memory:**
   - add an expense_rolling_forecast note;
   - update the budget_expense and expense notes: a change to `config_expense.csv` or to the expense DAG now affects 3
     DAGs, and all 3 `generated_sql/` folders need regenerating.

## After it lands (for you)
1. Load expense's config table with `expense/generated_sql/dml/corp_challenger_expense_formula_config.sql`, if you
   haven't yet.
2. Run `ddl/corp_challenger_rolling_forecast_expense_line_item_all_table.sql`.
3. Register and deploy the DAG:
   1. Replace `<next id>` and run `dml/ingestion_config_table.sql`.
   2. Add the `epm_dag_metadata` row.
   3. Deploy the DAG file.
4. Check the forecast's periods and years before the first run:
   ```sql
   SELECT year, period, COUNT(*) AS n FROM `oddo_project.od_published_epm.epm_p00exp_plan_bdgt_v`
   WHERE scenario = 'SC.OL' AND version = 'VR.SUB.10' GROUP BY 1, 2 ORDER BY 1, 2;
   ```
   - The DAG stops on a period it can't turn into a month, such as 13 or BegBalance. Put any such period in
     `EXCLUDED_PERIODS`.
   - The window is FY22–FY26, as in your view. If the forecast reaches into FY27, both leave those months out. The fix
     is one line: `ACTUALS_YEARS_BACK = None` reads every year of that one version.
5. For each new forecast version (e.g. VR.SUB.11), edit `BUDGET_FILTERS` and redeploy. If the values match no rows,
   the run empties the output table without an error.

## Not in this change
- `revenue_rolling_forecast/`: its views differ from budget_revenue's in the same way. It would be budget_revenue's
  files with the same edits, as a separate request.
- These stay as they are: the two view files, the expense and budget_expense files, and `config_expense.csv`.
- The optional `ASSERT` that rows exist before the TRUNCATE. It was offered for budget and not built.
- No commit unless you ask.
