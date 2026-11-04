# Plan: Make the view's config table match config_driver.csv for fiber

## Context
- After the last changes, the DAG table (`corp_challenger_driver_line_item_all_table`) shows correct fiber values. The
  view chain (`corp_challenger_driver_line_item` → view_to_table DAG → `corp_challenger_driver_line_item_all`) still
  shows 0 for every Fios line item.
- The view chain reads `corp_challenger_driver_config_table` (your export: `corp_challenger_driver_config_table.csv`).
  `config_driver.csv` is what the DAG reads, and it now gives the right numbers. Compared field by field with it, the
  view table's 12 fiber rows (IDs 19–30) differ in four ways:
  1. **ServiceType is `SERVICE_TYPE`** on all 12 rows; `config_driver.csv` has `ALL` (your rule: SERVICE_TYPE → ALL).
     `driver_line_item.sql` treats only `'ALL'` as "no filter". Any other value must match the primary Service_Type
     hierarchy, so here it drops every fiber row. That's why even Consumer, whose names are right, shows 0.
  2. **The six Business rows are named `Business- Fios …` / `Business- Frontier …`.** `driver_line_item_all.sql` reads
     `Business - …`.
  3. **The four Beginning rows use `STC.CU.1400.i`,** which EPM doesn't have. `config_driver.csv` now has
     `Calculated … Fiber Connections EOP` rows on `STC.CU.1400` instead.
  4. **The four `Calculated … Fiber Connections Adj` rows (`STC.CU.1016`) are missing.**
- **The fix is to the data only:** four statements on that table. No change to the SQL files, the DAG or
  `config_driver.csv`.

## The fix (BigQuery, for you to run; any order)
```sql
-- 1. SERVICE_TYPE means all service types: ALL, as in config_driver.csv
UPDATE `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
SET ServiceType = 'ALL'
WHERE ServiceType = 'SERVICE_TYPE';

-- 2. Names as driver_line_item_all.sql reads them: 'Business - ', not 'Business- '
UPDATE `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
SET LineItem = REPLACE(LineItem, 'Business- ', 'Business - ')
WHERE LineItem LIKE '%Business- %';

-- 3. EPM has no STC.CU.1400.i: the four Beginning rows become the EOP anchors on STC.CU.1400
UPDATE `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
SET LineItem = CONCAT('Calculated ', REPLACE(REPLACE(LineItem, 'Business- ', 'Business - '), ' Beginning', ' EOP')),
    Account = 'STC.CU.1400'
WHERE LineItem LIKE '% Fiber Connections Beginning';

-- 4. The four fiber Adj rows, as in config_driver.csv (FinalOutput left NULL: neither view reads it)
INSERT INTO `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
  (Config_ID, LineItem, ProfitCentre, ProfitCentreHierarchyName, CompanyCode, Account, ModelName, ServiceType,
   Platform, CostCenter, GeoRegion, Market, ContractTerm, Product, Equipment, DistributionChannel, Source)
VALUES
  (47, 'Calculated Consumer - Fios Fiber Connections Adj', 'PC.0.50000000', 'ALT_PC_VZ_FTR_Split', 'ALL', 'STC.CU.1016',
   'Challenger Model', 'ALL', 'ALL', 'ALL', 'ALL', 'MKT.PRIM_MKT', 'ALL', 'PR.5251', 'ALL', 'DC.DISTCHAN', 'ALL'),
  (48, 'Calculated Consumer - Frontier Fiber Connections Adj', 'PC.0.23000000', 'ALT_PC_VZ_FTR_Split', 'ALL', 'STC.CU.1016',
   'Challenger Model', 'ALL', 'ALL', 'ALL', 'ALL', 'MKT.PRIM_MKT', 'ALL', 'PR.5251', 'ALL', 'DC.DISTCHAN', 'ALL'),
  (49, 'Calculated Business - Fios Fiber Connections Adj', 'PC.0.40000000', 'ALT_PC_VZ_FTR_Split', 'ALL', 'STC.CU.1016',
   'Challenger Model', 'ALL', 'ALL', 'ALL', 'ALL', 'MKT.PRIM_MKT', 'ALL', 'PR.5251', 'ALL', 'DC.DISTCHAN', 'ALL'),
  (50, 'Calculated Business - Frontier Fiber Connections Adj', 'PC.0.22000000', 'ALT_PC_VZ_FTR_Split', 'ALL', 'STC.CU.1016',
   'Challenger Model', 'ALL', 'ALL', 'ALL', 'ALL', 'MKT.PRIM_MKT', 'ALL', 'PR.5251', 'ALL', 'DC.DISTCHAN', 'ALL');
```
Then run the view_to_table DAG, and query `corp_challenger_driver_line_item_all` for fiber.

## Verification
1. **Done already, in memory:** the four statements applied to `corp_challenger_driver_config_table.csv`, in two
   different orders, give the same result.
   - The table then has exactly the 16 fiber base rows of `config_driver.csv`: same names, no dimension differences.
   - All 16 fiber names `driver_line_item_all.sql` reads are present.
   - The 50 Config_IDs are unique, and no `SERVICE_TYPE` is left.
2. **After the fix and the view_to_table run,** the view should match EPM and the DAG table. FY26 May / Jun:

   | Line item | Gross Adds | Disconnects | Net Adds | Connections | Churn % |
   |---|---|---|---|---|---|
   | Consumer - Fios Fiber | 101,342 / 115,505 | 89,099 / 101,209 | 12,243 / 14,296 | 7,379,735 / 7,394,031 | 0.012083 / 0.013701 |
   | Business - Fios Fiber | 5,439 / 5,657 | 4,671 / 4,919 | 768 / 738 | 436,592 / 437,330 | 0.010708 / 0.011257 |

   Frontier Connections: Consumer 2,901,345 / 2,936,859, Business 142,880 / 144,408. The Adj and EOP rows stay out of
   the view's output because of the `Calculated` prefix.
3. **If Consumer fiber is still 0,** another value isn't found in the primary hierarchy. The view then drops the row,
   where the DAG would use the value itself. This lists the ones that are found; any missing one is the culprit:
   ```sql
   SELECT dimension_name, parent, COUNT(DISTINCT node) AS nodes
   FROM `oddo_project.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v`
   WHERE is_primary = TRUE AND node IS NOT NULL
     AND ((dimension_name = 'Account' AND parent IN ('STC.CU.1301', 'STC.CU.1302', 'STC.CU.1016', 'STC.CU.1400'))
       OR (dimension_name = 'Product' AND parent = 'PR.5251'))
   GROUP BY 1, 2 ORDER BY 1, 2;
   ```
   The follow-up would then be to give `driver_line_item.sql` the DAG's fallback: use the value itself when it has no
   children.

## Not changed
- `driver_line_item.sql`, `driver_line_item_all.sql`, the DAG, and `config_driver.csv`.
- `corp_challenger_driver_config_table.csv` stays as your export of the table before the fix.
