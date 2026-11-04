CREATE OR REPLACE VIEW `oddo_project.od_enriched_epmmlops.corp_challenger_rolling_forecast_revenue_line_item` AS
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
      ServiceType,
      Platform,
      ContractTerm,
      Product,
      Equipment,
      GeoRegion,
      Market,
      DistributionChannel,
      Source
    FROM
      `oddo_project.od_enriched_epmmlops.corp_challenger_revenue_config_table`
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
  Service_Type_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Service_Type_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.ServiceType AND H.dimension_name = 'Service_Type'
    WHERE C.ServiceType <> 'ALL'
  ),
  Platform_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Platform_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Platform AND H.dimension_name = 'Platform'
    WHERE C.Platform <> 'ALL'
  ),
  Contract_Term_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Contract_Term_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.ContractTerm AND H.dimension_name = 'Contract_Term'
    WHERE C.ContractTerm <> 'ALL'
  ),
  Product_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Product_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Product AND H.dimension_name = 'Product'
    WHERE C.Product <> 'ALL'
  ),
  Equipment_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS Equipment_granular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Equipment AND H.dimension_name = 'Equipment'
    WHERE C.Equipment <> 'ALL'
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
      Service_Type,
      Platform,
      Contract_Term,
      Product,
      Equipment,
      Geo_Region,
      Market,
      Distribution_Channel,
      Source,
      SUM(amount) AS amount
    FROM `oddo_project.od_published_epm.epm_p00rev_plan_bdgt_v`
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
      Service_Type,
      Platform,
      Contract_Term,
      Product,
      Equipment,
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
LEFT JOIN Service_Type_mapping m3
  ON m3.Config_ID = C.Config_ID AND m3.Service_Type_granular = A.Service_Type
LEFT JOIN Platform_mapping m4
  ON m4.Config_ID = C.Config_ID AND m4.Platform_granular = A.Platform
LEFT JOIN Contract_Term_mapping m5
  ON m5.Config_ID = C.Config_ID AND m5.Contract_Term_granular = A.Contract_Term
LEFT JOIN Product_mapping m6
  ON m6.Config_ID = C.Config_ID AND m6.Product_granular = A.Product
LEFT JOIN Equipment_mapping m7
  ON m7.Config_ID = C.Config_ID AND m7.Equipment_granular = A.Equipment
LEFT JOIN Geo_Region_mapping m8
  ON m8.Config_ID = C.Config_ID AND m8.Geo_Region_granular = A.Geo_Region
LEFT JOIN Market_mapping m9
  ON m9.Config_ID = C.Config_ID AND m9.Market_granular = A.Market
LEFT JOIN Distribution_Channel_mapping m10
  ON m10.Config_ID = C.Config_ID AND m10.Distribution_Channel_granular = A.Distribution_Channel
LEFT JOIN Source_mapping m11
  ON m11.Config_ID = C.Config_ID AND m11.Source_granular = A.Source
WHERE
  (C.ProfitCentre = 'ALL' OR m0.Config_ID IS NOT NULL)
  AND (C.Account = 'ALL' OR m1.Config_ID IS NOT NULL)
  AND (C.CompanyCode = 'ALL' OR m2.Config_ID IS NOT NULL)
  AND (C.ServiceType = 'ALL' OR m3.Config_ID IS NOT NULL)
  AND (C.Platform = 'ALL' OR m4.Config_ID IS NOT NULL)
  AND (C.ContractTerm = 'ALL' OR m5.Config_ID IS NOT NULL)
  AND (C.Product = 'ALL' OR m6.Config_ID IS NOT NULL)
  AND (C.Equipment = 'ALL' OR m7.Config_ID IS NOT NULL)
  AND (C.GeoRegion = 'ALL' OR m8.Config_ID IS NOT NULL)
  AND (C.Market = 'ALL' OR m9.Config_ID IS NOT NULL)
  AND (C.DistributionChannel = 'ALL' OR m10.Config_ID IS NOT NULL)
  AND (C.Source = 'ALL' OR m11.Config_ID IS NOT NULL)
GROUP BY
  C.LineItem,
  A.year,
  A.period;