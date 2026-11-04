CREATE OR REPLACE VIEW `oddo_project.od_enriched_epmmlops.corp_challenger_budget_revenue_line_item_all`
AS
WITH
  date_params AS (
    SELECT
      CONCAT('FY', FORMAT_DATE('%y', CURRENT_DATE())) AS curr_fy,
      FORMAT_DATE('%b', CURRENT_DATE()) AS curr_month,
      FORMAT_DATE('%b', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        AS prev_month
  ),

  -- Intermediate lightweight summary table to break CTE expansion tree
  monthly_summary AS (
    SELECT
      year,
      month,
      LineItem,
      SUM(amount) AS Amount
    FROM
      `oddo_project.od_enriched_epmmlops.corp_challenger_budget_revenue_line_item_table`
    GROUP BY year, month, LineItem
  ),
  calculated_line_item AS (
    SELECT
    'Consumer Volume/Services' AS LineItem,
    year,
    month,
    SUM(
      CASE
        WHEN LineItem = 'Calculated Consumer Volume/Services' THEN Amount
        WHEN LineItem = 'Consumer Postpaid FWA' THEN -Amount
        WHEN LineItem = 'Consumer Promo Amortization' THEN -Amount
        WHEN LineItem = 'Consumer Value prepaid' THEN -Amount
        WHEN LineItem = 'Consumer Reseller' THEN -Amount
        ELSE 0
      END
    ) AS Amount
  FROM monthly_summary
  WHERE LineItem IN ('Calculated Consumer Volume/Services', 'Consumer Postpaid FWA', 'Consumer Promo Amortization', 'Consumer Value prepaid', 'Consumer Reseller')
  GROUP BY year, month

  UNION ALL

  SELECT
    'Business Volume/Services' AS LineItem,
    year,
    month,
    SUM(
      CASE
        WHEN LineItem = 'Calculated Business Volume/Services' THEN Amount
        WHEN LineItem = 'Business Postpaid FWA' THEN -Amount
        WHEN LineItem = 'Business Promo Amortization' THEN -Amount
        ELSE 0
      END
    ) AS Amount
  FROM monthly_summary
  WHERE LineItem IN ('Calculated Business Volume/Services', 'Business Postpaid FWA', 'Business Promo Amortization')
  GROUP BY year, month
  )
SELECT LineItem, year, month, Amount
FROM monthly_summary
WHERE LineItem NOT LIKE '%Calculated%'
UNION ALL
SELECT LineItem, year, month, Amount
FROM calculated_line_item
ORDER BY LineItem, year, month;
