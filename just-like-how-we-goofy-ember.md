# Plan: Alternate-hierarchy profit centres: fix the view, and anchor fiber Connections on EPM's EOP

## Context
`hierarchy_name.csv` puts the four fiber profit centres in `ALT_PC_VZ_FTR_Split`: PC.0.50000000, PC.0.23000000,
PC.0.40000000 and PC.0.22000000. `alt_hierarchy_pc.csv` shows all 303 rows of that hierarchy have `is_primary = false`.
The hierarchy is a closure table: each `parent` lists every descendant, plus itself at lvl 1.
`driver_line_item.sql` (line 32) keeps only `is_primary = TRUE`, so these profit centres map to nothing.

Comparison with EPM (`driver_data.csv`), FY26 May / Jun:

| Line item | EPM | View `line_item_all` | DAG table |
|---|---|---|---|
| Consumer - Fios Fiber Gross Adds / Disconnects / Net Adds | 101,342 / 115,505 · 89,099 / 101,209 · 12,243 / 14,296 | missing, missing, 0 | same as EPM |
| Consumer - Fios Fiber Connections | 7,379,735 / 7,394,031 | 0 | 456,456 / 470,752 (−6,923,279) |
| Consumer - Fios Fiber Churn % | 0.012083 / 0.013701 | 0 | 0.1979 / 0.2183 |
| Business - Fios Fiber Gross Adds / Disconnects / Net Adds | 5,439 / 5,657 · 4,671 / 4,919 · 768 / 738 | missing, missing, 0 | same as EPM |
| Business - Fios Fiber Connections | 436,592 / 437,330 | 0 | 33,516 / 34,254 (−403,076) |
| Business - Fios Fiber Churn % | 0.010708 / 0.011257 | 0 | 0.1410 / 0.1452 |

- **View: wrong.** No fiber actuals reach it, because of the primary-only filter.
- **DAG table:** its alternate-hierarchy mapping is right, since the base items are exact. But Connections roll forward
  from 0.
  - Your query found no fiber opening-balance account in EPM. In FY26 there is nothing besides `STC.CU.1400` (EOP Lines
    in Service).
  - So the four `… Fiber Connections Beginning` rows (`STC.CU.1400.i`) return nothing, and Churn is wrong as a result.
- **Your decision:** anchor the fiber roll forwards on EPM's EOP (`STC.CU.1400`) in every month that has actuals.

## 1. `driver_line_item.sql`: take the profit centre's hierarchy from the config
- **Config CTE:** add `NULLIF(TRIM(ProfitCentreHierarchyName), '') AS ProfitCentreHierarchyName`.
- **New CTE `pc_hierarchy`:** every Profit_Center hierarchy, primary and alternate, with `hierarchy_name` and
  `is_primary`.
- **`pc_mapping`:** join `pc_hierarchy` using the DAG's own rule (`generated_sql/base.sql` line 131):
  `CASE WHEN C.ProfitCentreHierarchyName IS NULL THEN H.is_primary = TRUE ELSE H.hierarchy_name = C.ProfitCentreHierarchyName END`.
- **Nothing else changes:** `hierarchy_flat` stays primary-only for the other 12 dimensions. Rows with a blank
  hierarchy name (Phone, FWA, SafeLink, …) map exactly as today.

## 2. Fiber anchor on EPM's EOP (config; no DAG code)
- **Config rows (`config_driver.csv` IDs 41–44, `driver_config_raw.csv` IDs 43–46):** the four fiber Beginning rows
  become `Calculated <segment> Fiber Connections EOP`, with Account `STC.CU.1400`.
  - Every other value stays as it is: profit centre and `ALT_PC_VZ_FTR_Split`, `PR.5251`, `DC.DISTCHAN`, and
    FinalOutput false.
  - The IDs stay the same, so nothing is renumbered.
  - The `Calculated` prefix follows the Adj rows, so the view also leaves these rows out of its output.
- **Formulas:**
  - The four fiber ROLL_FORWARD formulas in `config_driver.csv` (`<segment> Fiber Connections`) get
    `actual=[Calculated <segment> Fiber Connections EOP]`. The DAG already supports this: in a month where the actual
    row has a value, that value is shown and the roll forward restarts from it.
  - The same four rows in `driver_config_raw.csv` get the rule in plain English, without commas:
    `Current month (Calculated … EOP) if EPM has it; otherwise [Prior month (…) + …]`.
- **The FWA Beginning rows stay as they are.** Check (a) below confirms whether `STC.CU.1006.i` holds values for FWA.
- **`generated_sql/dml/corp_challenger_driver_formula_config.sql`:** regenerated again, and only that file, as last
  time.

## 3. `driver_line_item_all.sql`: the same anchor for the four fiber segments
- **`connection_segments`:** gets a 4th field, `actual_item`. It is `NULL` for Phone/FWA and
  `Calculated <segment> Fiber Connections EOP` for fiber.
- **`connections_inputs`:**
  - Add the four EOP names to the `monthly_summary` list, and `s.actual_item` to the join.
  - Change `actual_connections` to `COALESCE(<EOP row>, <row or beginning seed named like the roll forward>)`. The EOP
    wins, as in the DAG. Phone/FWA see no change because their `actual_item` is NULL.

## 4. Outside the repo (BigQuery, for you to run, in this order)
1. **The view's config table (`corp_challenger_driver_config_table`):**
   - Add the four `Calculated … Fiber Connections EOP` rows from `config_driver.csv`. Add the other fiber base rows too
     if they aren't there yet.
   - Then add and fill the hierarchy column:
   ```sql
   ALTER TABLE `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
     ADD COLUMN IF NOT EXISTS ProfitCentreHierarchyName STRING;
   UPDATE `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
   SET ProfitCentreHierarchyName = 'ALT_PC_VZ_FTR_Split'
   WHERE ProfitCentre IN ('PC.0.50000000', 'PC.0.23000000', 'PC.0.40000000', 'PC.0.22000000');
   ```
2. **Recreate the two views** from `driver_line_item.sql` and `driver_line_item_all.sql`. Then run the view_to_table DAG.
3. **Reload the DAG's config table** with the regenerated DML, then rerun the DAG.

## Verification
1. **Locally:** run the DAG planner on the edited `config_driver.csv`, in memory. Expect:
   - 4 Beginning balances (Phone and FWA) and no warnings
   - the four fiber roll forwards showing `actual_item = 'Calculated … EOP'` in the generated SQL
   - still 70 output line items
   - every `Fiber` name in `driver_line_item_all.sql` existing in `config_driver.csv`
2. **BigQuery, after step 4:** fiber FY26 May / Jun should equal EPM in both the DAG table and `line_item_all`. For
   Consumer Fios:
   - Connections 7,379,735 / 7,394,031
   - Average 7,373,613.5 / 7,386,883
   - Churn 0.012083 / 0.013701

   Business Fios: Connections 436,592 / 437,330, Churn 0.010708 / 0.011257. Gross Adds, Disconnects and Net Adds as in
   the table above.
3. **Check (a), DAG table:** Phone, FWA and Frontier Connections, May / Jun, against EPM:

   | Line item | May | Jun |
   |---|---|---|
   | Consumer Phone | 74,868,957 | 75,000,838 |
   | Business Phone | 19,047,719 | 19,097,326 |
   | Consumer FWA | 3,479,314 | 3,505,040 |
   | Business FWA | 2,272,111 | 2,305,362 |
   | Consumer Frontier | 2,901,345 | 2,936,859 |
   | Business Frontier | 142,880 | 144,408 |

   If FWA is still off by a constant, its `STC.CU.1006.i` rows are empty too, and the same EOP anchor would apply.
   ```sql
   SELECT LineItem, month, Amount
   FROM `oddo_project.od_enriched_epmmlops.corp_challenger_driver_line_item_all_table`
   WHERE year = 'FY26' AND month IN ('May', 'Jun')
     AND LineItem IN ('Consumer - Phone Connections', 'Business - Phone Connections',
                      'Consumer - FWA Connections', 'Business - FWA Connections',
                      'Consumer - Frontier Fiber Connections', 'Business - Frontier Fiber Connections')
   ORDER BY LineItem, month;
   ```

## Not changed
- **`corp_challenger_model_driver_line_item_all.py`:** its alternate-hierarchy handling and `actual=` already work.
- **`Formula-Conversion-Guide.md`:** it doesn't cover `actual=` yet. Optional follow-up.
