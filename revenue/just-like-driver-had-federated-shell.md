# Plan: revenue config, DAG, generator and SQL (mirroring driver)

## Context
Driver has a full chain:
1. `driver_config_raw.csv` holds plain-English formulas.
2. `config_driver.csv` holds the same formulas in the DAG's syntax (`Formula-Conversion-Guide.md`).
3. DAG `corp_challenger_model_driver_line_item_all` and its config JSON.
4. Local generator `generate_driver_sql.py` writes the DDL/DML.

Revenue so far has only `revenue/revenue_config_raw.csv` (79 rows). This builds the same chain for revenue, with
everything revenue-related in `revenue/`. Expense is out of scope.

**Already checked, read-only.** I loaded the driver DAG the way the generator does (Airflow stubbed) and ran its own
`prepare_plan` and every SQL builder on the revenue rows, formulas translated. Nothing failed, so **the DAG needs no
logic change for revenue**:
- 30 base line items. Three have several rows: Business and Consumer - Postpaid FWA (19 accounts each) and Consumer -
  Value prepaid (3). The base SQL already sums them per line item (`GROUP BY LineItem`).
- 11 formulas, plain arithmetic only, in 3 tiers (8 / 2 / 1). There is no ROLL_FORWARD or LAG, so no last-actual cutoff.
- Dimensions: ProfitCentre, Account and Market are required; CompanyCode is optional.
- `MLProcessing` becomes a STRING column. The DAG reads only the columns it knows.

## Your decisions
- The 11 formula rows are **outputs**: 18 base + 11 calculated = **29 line items** in the output table.
- `MLProcessing` **stays** in the config. The DAG ignores it.
- Hierarchy names come from `driver/hierarchy_name.csv`, on every base row (formula rows stay blank, as in
  `config_driver.csv`):
  - **ProfitCentreHierarchyName:** the file's mapping, else `PROFIT_CENTER`. That changes 9 rows, all blank before:
    47, 50, 53, 54, 57, 62, 63, 64, 68 (PC.0.30000000, PC.0.21000000, PC.20P3000000, PC.20P8888888, PC.20P0000000).
  - **AccountHierarchy:** the file's mapping, else `STATISTICS`. The file lists no revenue account, so all 68 base rows
    get `STATISTICS`.
  - **ServiceTypeHierarchy, ProductHierarchy, EquipmentHierachy, DistributionChannelHierarchy:** each has a single value
    in the file. All 68 base rows get `SERVICE_TYPE`, `PRODUCT`, `EQUIPMENT` and `DISTRIBUTION_CHANNEL`.
  - All of these were blank, so nothing overwrites a filled value. Planner counts are unchanged.
- No `Corporate - FTR WLN Less Fiber` row is added. The DAG keeps logging that `Calculated Corporate - FTR Only Total WLN
  Operating Revenue` is unused.
- Revenue SQL goes in `revenue/generated_sql/`. Driver's root `generated_sql/` moves to `driver/generated_sql/`.

## Changes
### 1. Move driver's generated SQL
- `git mv generated_sql driver/generated_sql`: 8 files, content unchanged, nothing regenerated.
- After the split, `generate_driver_sql.py` already writes there by default (`HERE/generated_sql`).

### 2. `revenue/config_revenue.csv` (new)
Made by a one-off conversion script in the scratchpad, not committed, as was done for `config_driver.csv`. It asserts
every step.
- **Columns:** the raw header (25 columns, including `MLProcessing`), with every row padded to 25 fields.
- **Rows:** Config_IDs 1–79, unchanged. No base row shares a name with a formula row, so none is dropped.
- **Formula:** only the `[names]` change; the arithmetic is already in the guide's `[A] + [B] - [C]` shape.
  - Each reference becomes the exact `LineItem` spelling. The script tries an exact match first, then a match ignoring
    ` - `, then one typo fix: `Volumne` → `Volume`.
  - The script fails on any reference it can't resolve, or resolves to more than one name.
  - e.g. `[Calculated Consumer Volume/Services] - [Consumer Postpaid FWA]` →
    `[Calculated Consumer - Volume/Services] - [Consumer - Postpaid FWA]`
- **FinalOutput:** `FALSE` → `False` on the 12 `Calculated …` base rows, spelled as in `config_driver.csv`. Blank on the
  11 formula rows.
- **The six hierarchy-name columns:** the rules above, applied to base rows only.
  - The script reads the values from `hierarchy_name.csv` rather than hard-coding them.
  - It asserts that each non-profit-centre column has exactly one value there.
  - Of these columns, only ProfitCentre and Account change the SQL. ServiceType, Product, Equipment and
    DistributionChannel are `ALL` on every row, so the DAG doesn't join those dimensions at all.
- **Layout:** like `config_driver.csv`, with CRLF line endings as checked out and no trailing newline.
- `revenue_config_raw.csv` is not changed.

### 3. `revenue/corp_challenger_model_revenue_line_item_all.py` (new)
A copy of `driver/corp_challenger_model_driver_line_item_all.py`. The DAG file is self-contained, which suits the
one-file-per-DAG deployment. Changes:
- `dag_configs = ["corp_challenger_model_revenue_line_item_all"]`.
- Module docstring:
  - names the revenue DAG;
  - drops the driver view-chain wording and the "(the generated driver_line_item*.sql)" asides;
  - adds "other config columns (e.g. MLProcessing) are not read".
- The same driver-file references go from the docstrings of `build_base_sql` and `build_formula_sql`.
- Everything else, including the formula language, checks and SQL builders, stays byte-identical.

### 4. `revenue/corp_challenger_model_revenue_line_item_all_config.json` (new)
A copy of the driver JSON with three values changed. The actuals view, hierarchy view, datasets, schedule
(`30 2 * * *`), `bq_conn_id` and `"<next id>"` stay the same.

| Key | Value |
|---|---|
| `source_file_name` | `corp_challenger_model_revenue_line_item_all` |
| `curate_bq_table` | `corp_challenger_revenue_formula_config` |
| `publish_bq_table` | `corp_challenger_revenue_line_item_all_table` |

### 5. `revenue/generate_revenue_sql.py` (new)
A copy of `driver/generate_driver_sql.py`. Changes:
- `DAG_NAME`, `CONFIG_TABLE` and `OUTPUT_TABLE` get the revenue names from section 4.
- `--config` defaults to `config_revenue.csv`.
- The docstring and the "Generated by …" header name the revenue script and files.
- It loads the revenue DAG file, so its SQL is exactly what that DAG runs.

### 6. `revenue/generated_sql/` (output of step 5)
Run `venv\Scripts\python.exe revenue\generate_revenue_sql.py -v`. It writes:
- `ddl/corp_challenger_revenue_formula_config.sql`: `Config_ID` INT64, `FinalOutput` BOOL, every other column STRING,
  including `MLProcessing`.
- `ddl/corp_challenger_revenue_line_item_all_table.sql`: LineItem, year, month STRING; Amount FLOAT64.
- `dml/corp_challenger_revenue_formula_config.sql`: TRUNCATE, then INSERT of the 79 rows.
- `dml/corp_challenger_revenue_line_item_all_table.sql`: what the DAG runs.
- `dml/ingestion_config_table.sql`: from the JSON.
- Previews: `run_all.sql`, `base.sql`, `hierarchy_check.sql`.

## Verification
1. **Generator** prints `config_revenue.csv: 30 base line items, 11 formula line items in 3 tiers (8, 2, 1), 29 line
   items in the output`, plus the one warning about the unused Corporate FTR row.
2. **Offline checks** (scratchpad script, not committed):
   - `config_revenue.csv` against the raw file, field by field. The only differences may be:
     - Formula names and FinalOutput;
     - the 9 profit-centre hierarchy names;
     - the five other hierarchy-name columns on the 68 base rows.

     Padding adds empty fields only, and `MLProcessing` values are kept.
   - Every formula reference is an exact, case-sensitive `LineItem` name.
   - `git diff --no-index` of the DAG and generator copies against driver's shows only the lines listed in sections 3
     and 5.
   - The DAG imports through recording Airflow stubs:
     - dag_id `corp_challenger_model_revenue_line_item_all`;
     - tasks `generate_sql_query >> execute_compiled_sql`;
     - it reads `dags/<PROJECT_SPACE>/corp_challenger_model_revenue_line_item_all/config/corp_challenger_model_revenue_line_item_all_config.json`;
     - its config and output tables are the revenue ones.
3. **BigQuery fixture run** in `gcp-tutorial-test-336507`, as in earlier sessions. It uses TEMP tables only, so nothing
   permanent is written. The revenue DAG's base SQL and formula SQL run against fixture config, hierarchy and actuals
   tables. Checks:
   - the multi-row line items count each account once;
   - ALT_PC_VZ_FTR_Split rows map through the alternate hierarchy, PROFIT_CENTER rows through PROFIT_CENTER, and
     accounts through STATISTICS;
   - the CO.DW1000 / CO.VT1000 rows filter by company and `ALL` rows don't;
   - all 11 formulas equal the same arithmetic done in Python;
   - the output holds exactly the 29 output line items.

   If bq auth has expired, I'll report it and skip this step.
4. **Driver unaffected:**
   - `driver\generate_driver_sql.py --out-dir <scratchpad>` still prints `56 base, 42 formula in 4 tiers (16, 10, 8,
     8), 70 output`.
   - `git status` under `driver/` shows only the moved `generated_sql/` (100% renames).
5. Save the revenue conversion rules to memory: the hierarchy-name rules, FinalOutput, MLProcessing and the folder
   layout. Also correct the driver mapping memory: `hierarchy_name.csv` now says `PRODUCT`, not `Product`.

## After it lands (for you)
1. Run the two DDL files, then `dml/corp_challenger_revenue_formula_config.sql`.
2. Replace `<next id>` and run `dml/ingestion_config_table.sql`.
3. Add the `epm_dag_metadata` row the way you did for driver. Its config path is the one in Verification 2.
4. Deploy the DAG file.
5. Check that the selected hierarchies hold the revenue parents:
   - PC.0.30000000 and PC.0.21000000 now read `PROFIT_CENTER`.
   - Every account reads `STATISTICS`, including the summary accounts AC.4000 … AC.4200.

   A parent with no rows in its selected hierarchy maps only to itself, so its line item comes out as 0 without an
   error. The DAG's checks don't catch this when the value exists in another hierarchy. This query shows which
   hierarchies hold them:
   ```sql
   SELECT dimension_name, parent, hierarchy_name, is_primary, COUNT(DISTINCT node) AS nodes
   FROM `oddo_project.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v`
   WHERE (dimension_name = 'Profit_Center' AND parent IN
           ('PC.0.30000000', 'PC.0.21000000', 'PC.20P3000000', 'PC.20P8888888', 'PC.20P0000000'))
      OR (dimension_name = 'Account' AND parent IN
           ('AC.4000', 'AC.4100', 'AC.4110', 'AC.4110.01', 'AC.4110.02', 'AC.4120', 'AC.4200',
            'AC.41101112', 'AC.411012', 'AC.41102', 'AC.4110211'))
   GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3;
   ```

## Not changed
- Driver code, CSVs and DAG.
- `revenue_config_raw.csv`.
- `expense/`.
