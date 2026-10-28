WITH monthly_summary AS (
  SELECT
    year,
    month,
    LineItem,
    COALESCE(SUM(amount), 0) AS Amount
  FROM `oddo_project.od_enriched_epmmlops.corp_challenger_driver_line_item_table`
  GROUP BY year, month, LineItem
),

-- Tier 0 Helper: Parse chronological order across year and month for lag/rolling calculations
monthly_summary_dated AS (
  SELECT
    year,
    month,
    LineItem,
    Amount,
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
  FROM monthly_summary
),

-- Tier 1: Net Adds Calculations (Level 1 Derived Line Items)
level1_net_adds AS (
  -- 1. Calculated Prepaid Phone Net Add
  SELECT
    'Calculated Consumer Prepaid Phone Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Calculated Consumer Prepaid Phone SafeLink Gross Adds' THEN Amount
      WHEN LineItem = 'Calculated Consumer Prepaid Phone SafeLink Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 2. Consumer Prepaid Phone Net Adds
  SELECT
    'Consumer Prepaid Phone Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Calculated Consumer Prepaid Phone Equip. Gross Adds' THEN Amount
      WHEN LineItem = 'Calculated Consumer Prepaid Phone Equip. Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 3. Calculated Prepaid Net Adds
  SELECT
    'Calculated Consumer Prepaid Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Calculated Consumer Prepaid SafeLink Gross Adds' THEN Amount
      WHEN LineItem = 'Calculated Consumer Prepaid SafeLink Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 4. Consumer Prepaid Net Adds
  SELECT
    'Consumer Prepaid Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Calculated Consumer Prepaid Equip. Gross Adds' THEN Amount
      WHEN LineItem = 'Calculated Consumer Prepaid Equip. Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 3. Consumer Phone Net Adds
  SELECT
    'Consumer Phone Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Consumer Phone Gross Adds' THEN Amount
      WHEN LineItem = 'Consumer Phone Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 4. Business Phone Net Adds
  SELECT
    'Business Phone Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Business Phone Gross Adds' THEN Amount
      WHEN LineItem = 'Business Phone Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 11. Consumer FWA Net Adds
  SELECT
    'Consumer FWA Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Consumer FWA Gross Adds' THEN Amount
      WHEN LineItem = 'Consumer FWA Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date

  UNION ALL

  -- 12. Business FWA Net Adds
  SELECT
    'Business FWA Net Adds' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Business FWA Gross Adds' THEN Amount
      WHEN LineItem = 'Business FWA Disconnects' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM monthly_summary_dated
  GROUP BY year, month, month_date
),

-- Source rows + Level 1 items. If the source ever carries a line item with the
-- same name as a Level 1 item, the calculated one wins (prevents double counting).
combined_level1 AS (
  SELECT d.year, d.month, d.month_date, d.LineItem, d.Amount
  FROM monthly_summary_dated AS d
  LEFT JOIN (SELECT DISTINCT LineItem FROM level1_net_adds) AS l1
    ON d.LineItem = l1.LineItem
  WHERE l1.LineItem IS NULL   -- anti-join: keep source rows with no Level 1 match
  UNION ALL
  SELECT year, month, month_date, LineItem, Amount FROM level1_net_adds
),

-- Tier 2: Connections Roll-Forward Calculations (Level 2 Derived Line Items)
--
-- Connections(t) = Opening(t) + Net Adds(t) + Connections Adj(t)
-- Opening(t)     = actual Connections(t-1) if it exists, otherwise calculated Connections(t-1)
--
-- The recursion is resolved without a loop: anchor on the latest month before t
-- that has an actual, then add every month's movement since that anchor:
--   Connections(t) = Actual(anchor) + SUM(Net Adds + Adj) for months in (anchor, t]

-- Segment config: one row per roll-forward (5, 6, 13, 14)
connection_segments AS (
  SELECT *
  FROM UNNEST(ARRAY<STRUCT<connections_item STRING, net_adds_item STRING, adj_item STRING>>[
    ('Consumer Phone Connections', 'Consumer Phone Net Adds', 'Consumer Phone Connections Adj'),
    ('Business Phone Connections', 'Business Phone Net Adds', 'Business Phone Connections Adj'),
    ('Consumer FWA Connections',   'Consumer FWA Net Adds',   'Consumer FWA Connections Adj'),
    ('Business FWA Connections',   'Business FWA Net Adds',   'Business FWA Connections Adj')
  ])
),

-- One row per segment per month: actual connections (NULL if none) and the month's movement
connections_inputs AS (
  SELECT
    s.connections_item AS LineItem,
    c.year, c.month, c.month_date,
    -- NULL = no actual for this month. Zeros are treated as "no actual" too,
    -- in case forecast months are stored as 0 rather than left blank.
    -- (This NULL is an internal marker only; it never reaches the output.)
    NULLIF(SUM(IF(c.LineItem = s.connections_item, c.Amount, NULL)), 0) AS actual_connections,
    COALESCE(SUM(IF(c.LineItem IN (s.net_adds_item, s.adj_item), c.Amount, NULL)), 0) AS movement
  FROM combined_level1 AS c
  JOIN connection_segments AS s
    ON c.LineItem IN (s.connections_item, s.net_adds_item, s.adj_item)
  WHERE c.month_date IS NOT NULL
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
      -- latest prior actual + movement accumulated since that month
      LAST_VALUE(actual_connections IGNORE NULLS) OVER w_prior
        + cum_movement
        - LAST_VALUE(IF(actual_connections IS NOT NULL, cum_movement, NULL) IGNORE NULLS) OVER w_prior,
      -- first month with an actual (nothing prior to roll from): use its own actual
      actual_connections,
      -- no actual yet at all: roll forward from an opening balance of 0
      cum_movement
    ) AS Amount
  FROM connections_running
  WINDOW w_prior AS (
    PARTITION BY LineItem ORDER BY month_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )

  UNION ALL
  -- 1. Consumer Prepaid Phone Net Adds excl. Safelink
  SELECT
    'Consumer Prepaid Phone Net Adds excl. Safelink' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Consumer Prepaid Phone Net Adds' THEN Amount
      WHEN LineItem = 'Calculated Consumer Prepaid Phone Net Adds' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM level1_net_adds
  GROUP BY year, month, month_date

  UNION ALL

  -- 2. Consumer Prepaid Net Adds excl. Safelink
  -- TODO: confirm the subtracted item. This is the same one item 1 uses
  --       ('Calculated Consumer Prepaid Phone Net Adds'); possibly meant to be
  --       'Calculated Consumer Prepaid Net Adds'.
  SELECT
    'Consumer Prepaid Net Adds excl. Safelink' AS LineItem,
    year, month, month_date,
    SUM(CASE
      WHEN LineItem = 'Consumer Prepaid Net Adds' THEN Amount
      WHEN LineItem = 'Calculated Consumer Prepaid Net Adds' THEN -Amount
      ELSE 0
    END) AS Amount
  FROM level1_net_adds
  GROUP BY year, month, month_date
),

-- Calculated connections replace the source connections rows (one row per month)
combined_level2 AS (
  SELECT c.year, c.month, c.month_date, c.LineItem, c.Amount
  FROM combined_level1 AS c
  LEFT JOIN connection_segments AS s
    ON c.LineItem = s.connections_item
  WHERE s.connections_item IS NULL   -- anti-join: drop source connections rows
  UNION ALL
  SELECT year, month, month_date, LineItem, Amount FROM level2_connections
),

-- Tier 3: 2-Month Rolling Average Connections (Level 3 Derived Line Items)
-- The first month has no prior month for LAG. Instead of 0 (which would halve
-- the average and double churn), the prior month falls back to the current
-- month, so the first month's average equals its own connections.
level3_avg_connections AS (
  -- 7. Consumer Average Phone Connections
  SELECT
    'Consumer Average Phone Connections' AS LineItem, year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM combined_level2
  WHERE LineItem = 'Consumer Phone Connections'

  UNION ALL

  -- 8. Business Average Phone Connections
  SELECT
    'Business Average Phone Connections' AS LineItem, year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM combined_level2
  WHERE LineItem = 'Business Phone Connections'

  UNION ALL

  -- 15. Consumer FWA Average Connections
  SELECT
    'Consumer FWA Average Connections' AS LineItem, year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM combined_level2
  WHERE LineItem = 'Consumer FWA Connections'

  UNION ALL

  -- 16. Business FWA Average Connections
  SELECT
    'Business FWA Average Connections' AS LineItem, year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM combined_level2
  WHERE LineItem = 'Business FWA Connections'

  UNION ALL

  -- 19. Consumer - Fios Average Fibre Connections
  SELECT
    'Consumer - Fios Average Fibre Connections' AS LineItem, year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM combined_level2
  WHERE LineItem = 'Consumer - Fios Fibre Connections'

  UNION ALL

  -- 20. Business - Fios Average Fibre Connections
  SELECT
    'Business - Fios Average Fibre Connections' AS LineItem, year, month, month_date,
    (Amount + COALESCE(LAG(Amount) OVER (PARTITION BY LineItem ORDER BY month_date), Amount)) / 2.0 AS Amount
  FROM combined_level2
  WHERE LineItem = 'Business - Fios Fibre Connections'
),

combined_level3 AS (
  SELECT year, month, month_date, LineItem, Amount FROM combined_level2
  UNION ALL
  SELECT year, month, month_date, LineItem, Amount FROM level3_avg_connections
),

-- Tier 4: Churn % Calculations (Level 4 Derived Line Items)
-- SAFE_DIVIDE returns NULL when average connections is 0; that becomes 0% churn.
level4_churn AS (
  -- 9. Consumer Phone Churn %
  SELECT
    'Consumer Phone Churn %' AS LineItem, year, month, month_date,
    COALESCE(SAFE_DIVIDE(
      SUM(CASE WHEN LineItem = 'Consumer Phone Disconnects' THEN Amount ELSE 0 END),
      SUM(CASE WHEN LineItem = 'Consumer Average Phone Connections' THEN Amount ELSE 0 END)
    ), 0) AS Amount
  FROM combined_level3
  GROUP BY year, month, month_date

  UNION ALL

  -- 10. Business Phone Churn %
  SELECT
    'Business Phone Churn %' AS LineItem, year, month, month_date,
    COALESCE(SAFE_DIVIDE(
      SUM(CASE WHEN LineItem = 'Business Phone Disconnects' THEN Amount ELSE 0 END),
      SUM(CASE WHEN LineItem = 'Business Average Phone Connections' THEN Amount ELSE 0 END)
    ), 0) AS Amount
  FROM combined_level3
  GROUP BY year, month, month_date

  UNION ALL

  -- 17. Consumer FWA Churn %
  SELECT
    'Consumer FWA Churn %' AS LineItem, year, month, month_date,
    COALESCE(SAFE_DIVIDE(
      SUM(CASE WHEN LineItem = 'Consumer FWA Disconnects' THEN Amount ELSE 0 END),
      SUM(CASE WHEN LineItem = 'Consumer FWA Average Connections' THEN Amount ELSE 0 END)
    ), 0) AS Amount
  FROM combined_level3
  GROUP BY year, month, month_date

  UNION ALL

  -- 18. Business FWA Churn %
  SELECT
    'Business FWA Churn %' AS LineItem, year, month, month_date,
    COALESCE(SAFE_DIVIDE(
      SUM(CASE WHEN LineItem = 'Business FWA Disconnects' THEN Amount ELSE 0 END),
      SUM(CASE WHEN LineItem = 'Business FWA Average Connections' THEN Amount ELSE 0 END)
    ), 0) AS Amount
  FROM combined_level3
  GROUP BY year, month, month_date
),

-- Consolidated calculated line items table
calculated_line_item AS (
  SELECT LineItem, year, month, Amount FROM level1_net_adds
  UNION ALL
  SELECT LineItem, year, month, Amount FROM level2_connections
  UNION ALL
  SELECT LineItem, year, month, Amount FROM level3_avg_connections
  UNION ALL
  SELECT LineItem, year, month, Amount FROM level4_churn
)

-- Combined output: raw base items + calculated items
-- (base items that share a name with a calculated item are dropped, so each
--  LineItem appears once per month; the final COALESCE is a last safety net)
SELECT m.LineItem, m.year, m.month, COALESCE(m.Amount, 0) AS Amount
FROM monthly_summary AS m
LEFT JOIN (SELECT DISTINCT LineItem FROM calculated_line_item) AS calc
  ON m.LineItem = calc.LineItem
WHERE m.LineItem NOT LIKE '%Calculated%'
  AND calc.LineItem IS NULL   -- anti-join: drop base rows superseded by a calculated item

UNION ALL

SELECT LineItem, year, month, COALESCE(Amount, 0) AS Amount
FROM calculated_line_item
WHERE LineItem NOT LIKE '%Calculated%'
ORDER BY LineItem, year, month