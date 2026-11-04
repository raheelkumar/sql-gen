CREATE OR REPLACE VIEW `oddo_project.od_enriched_epmmlops.corp_challenger_rolling_forecast_expense_line_item` AS
WITH
  params AS (
    -- Change scenario/version HERE only.
    SELECT 'SC.OL' AS scenario, 'VR.SUB.10' AS version
  ),

  Config AS (
    SELECT DISTINCT
      Config_ID,
      LineItem,
      ProfitCentre,
      Account,
      CompanyCode,
      CostCenter,
      GeoRegion,
      Market,
      DistributionChannel,
      Source
    FROM
      `oddo_project.od_enriched_epmmlops.corp_challenger_expense_config_table`
  ),

  -- Single unified hierarchy CTE
  hierarchy_flat AS (
    SELECT DISTINCT
      dimension_name,
      parent AS filter_value,
      node AS granular_node
    FROM
      `oddo_project.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v`
    WHERE is_primary = TRUE AND node IS NOT NULL
  ),

  profit_center_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS profit_center_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.ProfitCentre AND H.dimension_name = 'Profit_Center'
    WHERE C.ProfitCentre <> 'ALL'
  ),
  Account_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Account_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Account AND H.dimension_name = 'Account'
    WHERE C.Account <> 'ALL'
  ),
  Company_Code_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Company_Code_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.CompanyCode AND H.dimension_name = 'Company_Code'
    WHERE C.CompanyCode <> 'ALL'
  ),
  Cost_Center_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Cost_Center_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.CostCenter AND H.dimension_name = 'Cost_Center'
    WHERE C.CostCenter <> 'ALL'
  ),
  Geo_Region_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Geo_Region_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.GeoRegion AND H.dimension_name = 'Geo_Region'
    WHERE C.GeoRegion <> 'ALL'
  ),
  Market_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Market_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Market AND H.dimension_name = 'Market'
    WHERE C.Market <> 'ALL'
  ),
  Distribution_Channel_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Distribution_Channel_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.DistributionChannel AND H.dimension_name = 'Distribution_Channel'
    WHERE C.DistributionChannel <> 'ALL'
  ),
  Source_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Source_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Source AND H.dimension_name = 'Source'
    WHERE C.Source <> 'ALL'
  ),

  -- Pre-aggregate budget before line item mapping to reduce row count
  budget AS (
    SELECT
      year,
      period,
      profit_center,
      Account,
      Company_Code,
      Cost_Center,
      Geo_Region,
      Market,
      Distribution_Channel,
      Source,
      SUM(amount) AS amount
    FROM `oddo_project.od_published_epm.epm_p00exp_plan_bdgt_v`
    WHERE scenario = (SELECT scenario FROM params) AND version = (SELECT version FROM params)
      AND year IN (
        SELECT
          CONCAT(
            'FY', FORMAT_DATE('%y', DATE_SUB(CURRENT_DATE(), INTERVAL i YEAR)))
        FROM UNNEST(GENERATE_ARRAY(0, 4)) AS i
      )
    GROUP BY
      year,
      period,
      profit_center,
      Account,
      Company_Code,
      Cost_Center,
      Geo_Region,
      Market,
      Distribution_Channel,
      Source
  )

SELECT
  C.LineItem,
  A.year,
  A.period AS month,
  SUM(A.amount) AS amount
FROM budget A
CROSS JOIN Config C
LEFT JOIN profit_center_mapping m0
  ON m0.Config_ID = C.Config_ID AND m0.profit_center_granular = A.profit_center
LEFT JOIN Account_mapping m1
  ON m1.Config_ID = C.Config_ID AND m1.Account_granular = A.Account
LEFT JOIN Company_Code_mapping m2
  ON m2.Config_ID = C.Config_ID AND m2.Company_Code_granular = A.Company_Code
LEFT JOIN Cost_Center_mapping m3
  ON m3.Config_ID = C.Config_ID AND m3.Cost_Center_granular = A.Cost_Center
LEFT JOIN Geo_Region_mapping m4
  ON m4.Config_ID = C.Config_ID AND m4.Geo_Region_granular = A.Geo_Region
LEFT JOIN Market_mapping m5
  ON m5.Config_ID = C.Config_ID AND m5.Market_granular = A.Market
LEFT JOIN Distribution_Channel_mapping m6
  ON m6.Config_ID = C.Config_ID AND m6.Distribution_Channel_granular = A.Distribution_Channel
LEFT JOIN Source_mapping m7
  ON m7.Config_ID = C.Config_ID AND m7.Source_granular = A.Source
WHERE
  (C.ProfitCentre = 'ALL' OR m0.Config_ID IS NOT NULL)
  AND (C.Account = 'ALL' OR m1.Config_ID IS NOT NULL)
  AND (C.CompanyCode = 'ALL' OR m2.Config_ID IS NOT NULL)
  AND (C.CostCenter = 'ALL' OR m3.Config_ID IS NOT NULL)
  AND (C.GeoRegion = 'ALL' OR m4.Config_ID IS NOT NULL)
  AND (C.Market = 'ALL' OR m5.Config_ID IS NOT NULL)
  AND (C.DistributionChannel = 'ALL' OR m6.Config_ID IS NOT NULL)
  AND (C.Source = 'ALL' OR m7.Config_ID IS NOT NULL)
GROUP BY
  C.LineItem,
  A.year,
  A.period;