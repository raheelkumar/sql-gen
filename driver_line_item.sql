WITH
  Config AS (
    SELECT
      Config_ID,
      LineItem,
      ProfitCentre,
      Account,
      CompanyCode,
      ServiceType,
      Platform,
      CostCenter,
      GeoRegion,
      Market,
      ContractTerm,
      Product,
      Equipment,
      DistributionChannel,
      Source
    FROM
      `oddo_project.od_enriched_epmmlops.corp_challenger_driver_config_table`
  ),

  -- Single unified hierarchy CTE replacing 28 individual view scans
  hierarchy_flat AS (
    SELECT DISTINCT
      dimension_name,
      parent AS filter_value,
      node AS granular_node
    FROM
      `oddo_project.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v`
    WHERE is_primary = TRUE AND node IS NOT NULL
  ),

  -- Streamlined mapping CTEs leveraging hierarchy_flat
  pc_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS ProfitCenterGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.ProfitCentre AND H.dimension_name = 'Profit_Center'
    WHERE C.ProfitCentre <> 'ALL'
  ),
  account_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS AccountGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Account AND H.dimension_name = 'Account'
    WHERE C.Account <> 'ALL'
  ),
  company_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS CompanyCodeGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.CompanyCode AND H.dimension_name = 'Company_Code'
    WHERE C.CompanyCode <> 'ALL'
  ),
  servicetype_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS ServiceTypeGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.ServiceType AND H.dimension_name = 'Service_Type'
    WHERE C.ServiceType <> 'ALL'
  ),
  platform_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS PlatformGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Platform AND H.dimension_name = 'Platform'
    WHERE C.Platform <> 'ALL'
  ),
  costcenter_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS CostCenterGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.CostCenter AND H.dimension_name = 'Cost_Center'
    WHERE C.CostCenter <> 'ALL'
  ),
  georegion_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS GeoRegionGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.GeoRegion AND H.dimension_name = 'Geo_Region'
    WHERE C.GeoRegion <> 'ALL'
  ),
  market_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS MarketGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Market AND H.dimension_name = 'Market'
    WHERE C.Market <> 'ALL'
  ),
  contractterm_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS ContractTermGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.ContractTerm AND H.dimension_name = 'Contract_Term'
    WHERE C.ContractTerm <> 'ALL'
  ),
  product_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS ProductGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Product AND H.dimension_name = 'Product'
    WHERE C.Product <> 'ALL'
  ),
  equipment_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS EquipmentGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Equipment AND H.dimension_name = 'Equipment'
    WHERE C.Equipment <> 'ALL'
  ),
  distributionchannel_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS DistributionChannelGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON
        H.filter_value = C.DistributionChannel
        AND H.dimension_name = 'Distribution_Channel'
    WHERE C.DistributionChannel <> 'ALL'
  ),
  source_mapping AS (
    SELECT DISTINCT C.Config_ID, H.granular_node AS SourceGranular
    FROM Config C
    INNER JOIN hierarchy_flat H
      ON H.filter_value = C.Source AND H.dimension_name = 'Source'
    WHERE C.Source <> 'ALL'
  ),

  -- Pre-aggregate actuals before line item mapping to reduce row count
  actuals AS (
    SELECT
      year,
      period,
      profit_center,
      Account,
      Company_Code,
      Service_Type,
      Platform,
      Cost_Center,
      Geo_Region,
      Market,
      Contract_Term,
      Product,
      Equipment,
      Distribution_Channel,
      Source,
      SUM(amount) AS amount
    FROM `oddo_project.od_published_epm.epm_p00all_actuals_fullhist_v`
    WHERE
      year IN (
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
      Cost_Center,
      Geo_Region,
      Market,
      Contract_Term,
      Product,
      Equipment,
      Distribution_Channel,
      Source
  )

-- Efficient join using parallel hash matching
SELECT
  C.LineItem,
  A.year,
  A.period AS month,
  SUM(A.amount) AS amount
FROM actuals A
CROSS JOIN Config C
LEFT JOIN pc_mapping PC
  ON PC.Config_ID = C.Config_ID AND PC.ProfitCenterGranular = A.profit_center
LEFT JOIN account_mapping AC
  ON AC.Config_ID = C.Config_ID AND AC.AccountGranular = A.Account
LEFT JOIN company_mapping CC
  ON CC.Config_ID = C.Config_ID AND CC.CompanyCodeGranular = A.Company_Code
LEFT JOIN servicetype_mapping ST
  ON ST.Config_ID = C.Config_ID AND ST.ServiceTypeGranular = A.Service_Type
LEFT JOIN platform_mapping P
  ON P.Config_ID = C.Config_ID AND P.PlatformGranular = A.Platform
LEFT JOIN costcenter_mapping CO
  ON CO.Config_ID = C.Config_ID AND CO.CostCenterGranular = A.Cost_Center
LEFT JOIN georegion_mapping GR
  ON GR.Config_ID = C.Config_ID AND GR.GeoRegionGranular = A.Geo_Region
LEFT JOIN market_mapping M
  ON M.Config_ID = C.Config_ID AND M.MarketGranular = A.Market
LEFT JOIN contractterm_mapping CT
  ON CT.Config_ID = C.Config_ID AND CT.ContractTermGranular = A.Contract_Term
LEFT JOIN product_mapping PR
  ON PR.Config_ID = C.Config_ID AND PR.ProductGranular = A.Product
LEFT JOIN equipment_mapping E
  ON E.Config_ID = C.Config_ID AND E.EquipmentGranular = A.Equipment
LEFT JOIN distributionchannel_mapping DC
  ON
    DC.Config_ID = C.Config_ID
    AND DC.DistributionChannelGranular = A.Distribution_Channel
LEFT JOIN source_mapping S
  ON S.Config_ID = C.Config_ID AND S.SourceGranular = A.Source
WHERE
  (C.ProfitCentre = 'ALL' OR PC.Config_ID IS NOT NULL)
  AND (C.Account = 'ALL' OR AC.Config_ID IS NOT NULL)
  AND (C.CompanyCode = 'ALL' OR CC.Config_ID IS NOT NULL)
  AND (C.ServiceType = 'ALL' OR ST.Config_ID IS NOT NULL)
  AND (C.Platform = 'ALL' OR P.Config_ID IS NOT NULL)
  AND (C.CostCenter = 'ALL' OR CO.Config_ID IS NOT NULL)
  AND (C.GeoRegion = 'ALL' OR GR.Config_ID IS NOT NULL)
  AND (C.Market = 'ALL' OR M.Config_ID IS NOT NULL)
  AND (C.ContractTerm = 'ALL' OR CT.Config_ID IS NOT NULL)
  AND (C.Product = 'ALL' OR PR.Config_ID IS NOT NULL)
  AND (C.Equipment = 'ALL' OR E.Config_ID IS NOT NULL)
  AND (C.DistributionChannel = 'ALL' OR DC.Config_ID IS NOT NULL)
  AND (C.Source = 'ALL' OR S.Config_ID IS NOT NULL)
GROUP BY
  C.LineItem,
  A.year,
  A.period