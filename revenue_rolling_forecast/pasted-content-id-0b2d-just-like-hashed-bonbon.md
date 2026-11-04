# Plan: revenue_rolling_forecast DAG, JSON and generator, on revenue's config table

## Context
`revenue_rolling_forecast/` holds only your two hand-written views. They differ from budget_revenue's views only in:
- the `rolling_forecast_revenue` names, and `line_item_all` reading `…_rolling_forecast_revenue_line_item_table`;
- `params` = `'SC.OL'` / `'VR.SUB.10'` instead of `'SC.CV'` / `'VR.SUB.8'`.

They share the source (`od_published_epm.epm_p00rev_plan_bdgt_v`), the config table, the hierarchy logic and the 2
Volume/Services formulas. A `git diff --no-index` against budget_revenue's views shows only the lines above. This is
the same relationship expense_rolling_forecast has with budget_expense.

This builds the budget_revenue chain for the rolling forecast, the way expense_rolling_forecast was built from
budget_expense:
- It uses revenue's config (`revenue/config_revenue.csv` / `corp_challenger_revenue_formula_config`). There is no new
  config CSV, config table, config DDL or config DML. You wrote "use the revenue_rolling_forecast one" for the config
  table. I read that as revenue's table, as your first paragraph says.
- The DAG's `base_values` temp table does the job of `…_line_item_table`, so no view-to-table DAG is needed.

## Already checked (read-only, nothing written)
- I ran budget_revenue's compiler in memory on `config_revenue.csv`, with the forecast values swapped in:
  - 30 base and 11 formula line items in 3 tiers (8, 2, 1), so 29 line items in the output. There is only the existing
    warning, `Calculated Corporate - FTR Only Total WLN Operating Revenue` unused.
  - 4 dimensions are filtered: ProfitCentre, Account, CompanyCode and Market. The forecast view's `budget` CTE selects
    all four.
  - The generated filter is the view's `params`:
    `WHERE T.scenario = 'SC.OL' AND T.version = 'VR.SUB.10' AND T.year IN (<FY26 … FY22>)`.
- Running `sed 's/expense/revenue/g; s/p00exp_/p00rev_/g'` on budget_expense's 3 files reproduces budget_revenue's 3
  files byte for byte.
- So the same sed on expense_rolling_forecast's 3 files gives budget_revenue's files plus the forecast edits.
  - Its predicted diff against budget_revenue shows only the names, the docstrings and the two filter values.
  - No "expense" is left over.
  - Line lengths are unchanged, because both words are 7 characters long.
- **So the DAG needs no logic change.** `BUDGET_FILTERS` already covers it.

## Names
| | budget_revenue | revenue_rolling_forecast |
|---|---|---|
| DAG (dag_id, `.py`, JSON `source_file_name`) | `corp_challenger_model_budget_revenue_line_item_all` | `corp_challenger_model_rolling_forecast_revenue_line_item_all` |
| Output table (`publish_bq_table`) | `corp_challenger_budget_revenue_line_item_all_table` | `corp_challenger_rolling_forecast_revenue_line_item_all_table` |
| Generator | `generate_budget_revenue_sql.py` | `generate_revenue_rolling_forecast_sql.py` |
| `BUDGET_FILTERS` | `SC.CV` / `VR.SUB.8` | `SC.OL` / `VR.SUB.10` |
| Source (`raw_bq_table`) | `epm_p00rev_plan_bdgt_v` | same |
| Config table (`curate_bq_table`) | `corp_challenger_revenue_formula_config` | same |

The DAG and table names follow your views' `rolling_forecast_revenue` order. The generator name follows the folder,
as in expense_rolling_forecast.

## Differences from your views, by design (the same choices as budget_revenue)
1. **Names:** the config's names (`Consumer - Volume/Services`), not the view's (`Consumer Volume/Services`).
2. **Formulas:** all 11 are calculated, giving the 29 output line items, not just the 2 Volume/Services lines.
3. **Hierarchies:** the 6 Consumer/Business VZ/FTR Fiber and WLN rows use `ALT_PC_VZ_FTR_Split`. The view reads
   `is_primary = TRUE` only, so its numbers for those lines can differ.
4. **Amounts are NUMERIC**, as in every DAG.
5. **Zero rows:** neither the DAG nor the view drops them.

## Changes (3 new files, made by the sed above from expense_rolling_forecast's files)
### 1. `revenue_rolling_forecast/corp_challenger_model_rolling_forecast_revenue_line_item_all.py`
This is budget_revenue's DAG with the edits expense_rolling_forecast made:
- the docstring: "on the rolling forecast", the same code as the budget_revenue DAG, only the scenario and version differ;
- `dag_configs`;
- `BUDGET_FILTERS = {"scenario": "SC.OL", "version": "VR.SUB.10"}`, with its comment citing
  `corp_challenger_rolling_forecast_revenue_line_item.sql`;
- the `# Raw:` comment, which names the rolling forecast's scenario / version.

### 2. `revenue_rolling_forecast/corp_challenger_model_rolling_forecast_revenue_line_item_all_config.json`
This is budget_revenue's JSON with 2 values changed, `source_file_name` and `publish_bq_table`.

### 3. `revenue_rolling_forecast/generate_revenue_rolling_forecast_sql.py`
This is budget_revenue's generator with the new `DAG_NAME` and `OUTPUT_TABLE`, and the new names in its docstring and
header. These stay as they are:
- `CONFIG_CSV` is `../revenue/config_revenue.csv`, read where it is;
- `CONFIG_TABLE` is revenue's;
- `BUDGET_TABLE` and `--budget-table` are unchanged;
- it writes no config DDL/DML.

### 4. Generated SQL
Running `venv\Scripts\python.exe revenue_rolling_forecast\generate_revenue_rolling_forecast_sql.py -v` writes these to
`revenue_rolling_forecast/generated_sql/`:
- `ddl/corp_challenger_rolling_forecast_revenue_line_item_all_table.sql`
- `dml/corp_challenger_rolling_forecast_revenue_line_item_all_table.sql`
- `dml/ingestion_config_table.sql`
- `run_all.sql`, `base.sql` and `hierarchy_check.sql`

## Verification
1. **Generator output:** `config_revenue.csv: 30 base line items, 11 formula line items in 3 tiers (8, 2, 1), 29 line
   items in the output`, with only the existing FTR Only warning.
2. **Diffs** (`git diff --no-index`):
   - Against budget_revenue:
     - DAG: only the names, the docstrings and the two filter values;
     - JSON: only the 2 values;
     - generator: only the names;
     - output DML and `ingestion_config_table.sql`: only the header, the output table name and `'SC.OL'` / `'VR.SUB.10'`.
   - Against sed(expense_rolling_forecast): identical. This proves parity with expense_rolling_forecast.
3. **DAG import** through recording Airflow stubs. Check:
   - the dag_id;
   - the tasks `generate_sql_query >> execute_compiled_sql`;
   - the config path `dags/<PROJECT_SPACE>/corp_challenger_model_rolling_forecast_revenue_line_item_all/config/…`;
   - the tables: CONFIG_TABLE is revenue's, TRANSACTION_TABLE is the plan view, OUTPUT_TABLE is the new table.
4. **BigQuery fixture run** in `gcp-tutorial-test-336507`, with TEMP tables only. If bq auth has expired, I'll report
   it and skip this step.
   - The DAG SQL and your two views run on one fixture:
     - a slice of the revenue config: the 2 Volume/Services formulas, their inputs and an `ALT_PC_VZ_FTR_Split` row;
     - a hierarchy;
     - plan rows for SC.OL / VR.SUB.10, plus decoys: SC.CV / VR.SUB.8, VR.SUB.9, FY21 and FY27.
   - Expected results:
     - Only the SC.OL / VR.SUB.10 rows in FY22–FY26 count.
     - The base line items and the 2 Volume/Services lines match your views, apart from the names and the alt-hierarchy
       rows.
     - Amount is NUMERIC.
5. **Memory:**
   - add a revenue_rolling_forecast note;
   - update the budget_revenue, expense_rolling_forecast and revenue-config notes and the index. A change to
     `config_revenue.csv` or the revenue DAG now affects 3 DAGs and 3 `generated_sql/` folders.

## After it lands (for you)
1. Load revenue's config table, if you haven't yet: `revenue/generated_sql/dml/corp_challenger_revenue_formula_config.sql`.
2. Run `ddl/corp_challenger_rolling_forecast_revenue_line_item_all_table.sql`.
3. Replace `<next id>` and run `dml/ingestion_config_table.sql`. Then add the `epm_dag_metadata` row and deploy the DAG
   file.
4. Check the forecast's periods and years before the first run:
   ```sql
   SELECT year, period, COUNT(*) AS n FROM `oddo_project.od_published_epm.epm_p00rev_plan_bdgt_v`
   WHERE scenario = 'SC.OL' AND version = 'VR.SUB.10' GROUP BY 1, 2 ORDER BY 1, 2;
   ```
   - The DAG stops on a period it can't turn into a month. Put any such period in `EXCLUDED_PERIODS`.
   - FY27 months are left out, as in your view. To include them, set `ACTUALS_YEARS_BACK = None`.
5. For each new forecast version, edit `BUDGET_FILTERS` and redeploy. If the values match no rows, the run empties the
   output table without an error.

## Not in this change
- These stay as they are: the two view files, the revenue and budget_revenue files, and `config_revenue.csv`.
- A CostCenter filter on a revenue row would need `Cost_Center`, which your view doesn't select.
- The optional `ASSERT` guard before the TRUNCATE is not built.
- No commit unless you ask.
