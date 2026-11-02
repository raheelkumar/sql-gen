CREATE OR REPLACE VIEW `oddo_project.od_enriched_epmmlops.corp_challenger_driver_line_item_all` AS

-- Tier 0: Monthly summary with chronological date parsing in a single step
WITH monthly_summary AS (
  SELECT
    year,
    month,
    LineItem,
    COALESCE(SUM(amount), 0) AS Amount,
    PARSE_DATE('%Y-%m',
      CONCAT(
        IF(STARTS_WITH(CAST(year AS STRING), 'FY'),
          CONCAT('20', SUBSTR(CAST(year AS STRING), 3)),
          CAST(year AS STRING)),
        '-',
        LPAD(CAST(
          CASE LOWER(CAST(month AS STRING))
            WHEN 'jan' THEN 1 WHEN 'january' THEN 1 WHEN '1' THEN 1
            WHEN 'feb' THEN 2 WHEN 'february' THEN 2 WHEN '2' THEN 2
            WHEN 'mar' THEN 3 WHEN 'march' THEN 3 WHEN '3' THEN 3
            WHEN 'apr' THEN 4 WHEN 'april' THEN 4 WHEN '4' THEN 4
            WHEN 'may' THEN 5 WHEN '5' THEN 5
            WHEN 'jun' THEN 6 WHEN 'june' THEN 6 WHEN '6' THEN 6
            WHEN 'jul' THEN 7 WHEN 'july' THEN 7 WHEN '7' THEN 7
            WHEN 'aug' THEN 8 WHEN 'august' THEN 8 WHEN '8' THEN 8
            WHEN 'sep' THEN 9 WHEN 'september' THEN 9 WHEN '9' THEN 9
            WHEN 'oct' THEN 10 WHEN 'october' THEN 10 WHEN '10' THEN 10
            WHEN 'nov' THEN 11 WHEN 'november' THEN 11 WHEN '11' THEN 11
            WHEN 'dec' THEN 12 WHEN 'december' THEN 12 WHEN '12' THEN 12
            ELSE SAFE_CAST(month AS INT64)
          END AS STRING), 2, '0')
      )
    ) AS month_date
  FROM `oddo_project.od_enriched_epmmlops.corp_challenger_driver_line_item_table`
  GROUP BY year, month, LineItem
),

-- Tier 1: Net Adds Calculations
level1_net_adds AS (
  SELECT
    item.LineItem,
    year,
    month,
    month_date,
    item.Amount
  FROM (
    SELECT
      year, month, month_date,
      [
        STRUCT('Calculated Consumer Prepaid Phone Net Adds' AS LineItem,
          SUM(CASE WHEN LineItem = 'Calculated Consumer Prepaid Phone SafeLink Gross Adds' THEN Amount WHEN LineItem = 'Calculated Consumer Prepaid Phone SafeLink Disconnects' THEN -Amount ELSE 0 END) AS Amount),
        STRUCT('Consumer Prepaid Phone Net Adds',
          SUM(CASE WHEN LineItem = 'Calculated Consumer Prepaid Phone Equip. Gross Adds' THEN Amount WHEN LineItem = 'Calculated Consumer Prepaid Phone Equip. Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Calculated Consumer Prepaid Net Adds',
          SUM(CASE WHEN LineItem = 'Calculated Consumer Prepaid SafeLink Gross Adds' THEN Amount WHEN LineItem = 'Calculated Consumer Prepaid SafeLink Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Consumer Prepaid Net Adds',
          SUM(CASE WHEN LineItem = 'Calculated Consumer Prepaid Equip. Gross Adds' THEN Amount WHEN LineItem = 'Calculated Consumer Prepaid Equip. Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Consumer Phone Net Adds',
          SUM(CASE WHEN LineItem = 'Consumer Phone Gross Adds' THEN Amount WHEN LineItem = 'Consumer Phone Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Business Phone Net Adds',
          SUM(CASE WHEN LineItem = 'Business Phone Gross Adds' THEN Amount WHEN LineItem = 'Business Phone Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Consumer FWA Net Adds',
          SUM(CASE WHEN LineItem = 'Consumer FWA Gross Adds' THEN Amount WHEN LineItem = 'Consumer FWA Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Business FWA Net Adds',
          SUM(CASE WHEN LineItem = 'Business FWA Gross Adds' THEN Amount WHEN LineItem = 'Business FWA Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Consumer PostPaid Net Adds',
          SUM(CASE WHEN LineItem = 'Consumer PostPaid Gross Adds' THEN Amount WHEN LineItem = 'Consumer PostPaid Disconnects' THEN -Amount ELSE 0 END)),
        STRUCT('Business PostPaid Net Adds',
          SUM(CASE WHEN LineItem = 'Business PostPaid Gross Adds' THEN Amount WHEN LineItem = 'Business PostPaid Disconnects' THEN -Amount ELSE 0 END))
      ] AS items
    FROM monthly_summary
    GROUP BY year, month, month_date
  ), UNNEST(items) AS item
),

-- Tier 2a: Prepaid Net Adds excl. Safelink
level2_excl_safelink AS (
  SELECT
    IF(LineItem LIKE '%Phone%',
      'Consumer Prepaid Phone Net Adds excl. Safelink',
      'Consumer Prepaid Net Adds excl. Safelink') AS LineItem,
    year, month, month_date,
    SUM(IF(LineItem LIKE 'Calculated%', -Amount, Amount)) AS Amount
  FROM level1_net_adds
  WHERE LineItem IN (
    'Consumer Prepaid Phone Net Adds', 'Calculated Consumer Prepaid Phone Net Adds',
    'Consumer Prepaid Net Adds', 'Calculated Consumer Prepaid Net Adds'
  )
  GROUP BY 1, year, month, month_date
),

-- Tier 2b: Connections Roll-Forward Calculations
connection_segments AS (
  SELECT *
  FROM UNNEST(ARRAY<STRUCT<connections_item STRING, net_adds_item STRING, adj_item STRING>>[
    ('Consumer Phone Connections', 'Consumer Phone Net Adds', 'Consumer Phone Connections Adj'),
    ('Business Phone Connections', 'Business Phone Net Adds', 'Business Phone Connections Adj'),
    ('Consumer FWA Connections',   'Consumer FWA Net Adds',   'Consumer FWA Connections Adj'),
    ('Business FWA Connections',   'Business FWA Net Adds',   'Business FWA Connections Adj')
  ])
),

-- Beginning balances for every year, shifted from Jan back to Dec of previous year,
-- so each year re-anchors to the EPM opening balance
beginning_seeds AS (
  SELECT
    REPLACE(LineItem, ' Beginning', '') AS LineItem,
    IF(STARTS_WITH(CAST(year AS STRING), 'FY'),
      FORMAT_DATE('FY%y', DATE_SUB(month_date, INTERVAL 1 MONTH)),
      FORMAT_DATE('%Y', DATE_SUB(month_date, INTERVAL 1 MONTH))
    ) AS year,
    'Dec' AS month,
    DATE_SUB(month_date, INTERVAL 1 MONTH) AS month_date,
    Amount
  FROM monthly_summary
  WHERE LineItem LIKE '%Connections Beginning'
),

-- Latest month with closed actuals; later months (e.g. open-period Adj plugs) are excluded
last_actual_month AS (
  SELECT MAX(month_date) AS max_month_date
  FROM monthly_summary
  WHERE LineItem IN ('Consumer Phone Gross Adds', 'Consumer Phone Disconnects')
    AND Amount <> 0
),

connections_inputs AS (
  SELECT
    s.connections_item AS LineItem,
    c.year, c.month, c.month_date,
    NULLIF(SUM(IF(c.LineItem = s.connections_item, c.Amount, NULL)), 0) AS actual_connections,
    COALESCE(SUM(IF(c.LineItem IN (s.net_adds_item, s.adj_item), c.Amount, NULL)), 0) AS movement
  FROM (
    SELECT year, month, month_date, LineItem, Amount
    FROM monthly_summary
    WHERE LineItem IN (
      'Consumer Phone Connections', 'Consumer Phone Connections Adj',
      'Business Phone Connections', 'Business Phone Connections Adj',
      'Consumer FWA Connections',   'Consumer FWA Connections Adj',
      'Business FWA Connections',   'Business FWA Connections Adj'
    )
    UNION ALL
    SELECT year, month, month_date, LineItem, Amount
    FROM level1_net_adds
    WHERE LineItem IN (
      'Consumer Phone Net Adds', 'Business Phone Net Adds',
      'Consumer FWA Net Adds',   'Business FWA Net Adds'
    )
    UNION ALL
    -- Injects the Dec previous year seeds as actual anchors
    SELECT year, month, month_date, LineItem, Amount
    FROM beginning_seeds
  ) AS c
  JOIN connection_segments AS s
    ON c.LineItem IN (s.connections_item, s.net_adds_item, s.adj_item)
  CROSS JOIN last_actual_month AS l
  WHERE c.month_date IS NOT NULL
    AND c.month_date <= l.max_month_date
  GROUP BY s.connections_item, c.year, c.month, c.month_date
),

connections_running AS (
  SELECT
    *,
    SUM(movement) OVER (
      PARTITION BY LineItem ORDER BY month_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_movement
  FROM connections_inputs
),

level2_connections AS (
  SELECT
    LineItem, year, month, month_date,
    COALESCE(
      actual_connections,
      LAST_VALUE(actual_connections IGNORE NULLS) OVER w_prior
        + cum_movement
        - LAST_VALUE(IF(actual_connections IS NOT NULL, cum_movement, NULL) IGNORE NULLS) OVER w_prior,
      cum_movement
    ) AS Amount
  FROM connections_running
  WINDOW w_prior AS (
    PARTITION BY LineItem ORDER BY month_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )
),

-- Tier 3: 2-Month Rolling Average Connections
level3_avg_connections AS (
  SELECT
    CASE LineItem
      WHEN 'Consumer Phone Connections' THEN 'Consumer Average Phone Connections'
      WHEN 'Business Phone Connections' THEN 'Business Average Phone Connections'
      WHEN 'Consumer FWA Connections' THEN 'Consumer FWA Average Connections'
      WHEN 'Business FWA Connections' THEN 'Business FWA Average Connections'
      WHEN 'Consumer - Fios Fibre Connections' THEN 'Consumer - Fios Average Fibre Connections'
      WHEN 'Business - Fios Fibre Connections' THEN 'Business - Fios Average Fibre Connections'
    END AS LineItem,
    year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM (
    SELECT LineItem, year, month, month_date, Amount FROM level2_connections
    UNION ALL
    SELECT LineItem, year, month, month_date, Amount FROM monthly_summary
    WHERE LineItem IN ('Consumer - Fios Fibre Connections', 'Business - Fios Fibre Connections')
  )
),

-- Tier 4: Churn % Calculations
level4_churn AS (
  SELECT
    CASE
      WHEN LineItem IN ('Consumer Phone Disconnects', 'Consumer Average Phone Connections') THEN 'Consumer Phone Churn %'
      WHEN LineItem IN ('Business Phone Disconnects', 'Business Average Phone Connections') THEN 'Business Phone Churn %'
      WHEN LineItem IN ('Consumer FWA Disconnects', 'Consumer FWA Average Connections') THEN 'Consumer FWA Churn %'
      WHEN LineItem IN ('Business FWA Disconnects', 'Business FWA Average Connections') THEN 'Business FWA Churn %'
    END AS LineItem,
    year, month, month_date,
    COALESCE(SAFE_DIVIDE(
      SUM(IF(ENDS_WITH(LineItem, 'Disconnects'), Amount, 0)),
      SUM(IF(LineItem LIKE '%Average%', Amount, 0))
    ), 0) AS Amount
  FROM (
    SELECT LineItem, year, month, month_date, Amount FROM monthly_summary
    WHERE LineItem IN ('Consumer Phone Disconnects', 'Business Phone Disconnects', 'Consumer FWA Disconnects', 'Business FWA Disconnects')
    UNION ALL
    SELECT LineItem, year, month, month_date, Amount FROM level3_avg_connections
    WHERE LineItem IN ('Consumer Average Phone Connections', 'Business Average Phone Connections', 'Consumer FWA Average Connections', 'Business FWA Average Connections')
  )
  GROUP BY 1, year, month, month_date
),

-- Consolidated calculated line items table
calculated_line_item AS (
  SELECT LineItem, year, month, month_date, Amount FROM level1_net_adds
  UNION ALL
  SELECT LineItem, year, month, month_date, Amount FROM level2_excl_safelink
  UNION ALL
  SELECT LineItem, year, month, month_date, Amount FROM level2_connections
  UNION ALL
  SELECT LineItem, year, month, month_date, Amount FROM level3_avg_connections
  UNION ALL
  SELECT LineItem, year, month, month_date, Amount FROM level4_churn
)

-- Combined output: raw base items + calculated items ordered chronologically
SELECT LineItem, year, month, Amount
FROM (
  SELECT m.LineItem, m.year, m.month, m.month_date, COALESCE(m.Amount, 0) AS Amount
  FROM monthly_summary AS m
  LEFT JOIN (SELECT DISTINCT LineItem FROM calculated_line_item) AS calc
    ON m.LineItem = calc.LineItem
  WHERE m.LineItem NOT LIKE '%Calculated%'
    AND calc.LineItem IS NULL
    AND m.LineItem NOT LIKE '%Beginning%'   -- Drops all raw beginning balance setup rows

  UNION ALL

  SELECT LineItem, year, month, month_date, COALESCE(Amount, 0) AS Amount
  FROM calculated_line_item
  WHERE LineItem NOT LIKE '%Calculated%'
)
ORDER BY LineItem, month_date;