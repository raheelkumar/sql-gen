CREATE OR REPLACE VIEW `oddo_project.od_enriched_epmmlops.corp_challenger_budget_expense_line_item_all`
AS
WITH
  -- Intermediate lightweight summary table to aggregate source line items by year and month
  monthly_summary AS (
    SELECT
      year,
      month,
      LineItem,
      SUM(amount) AS Amount
    FROM
      `oddo_project.od_enriched_epmmlops.corp_challenger_budget_expense_line_item_table`
    GROUP BY year, month, LineItem
  ),

  -- Derived line items using standard CTE / UNION ALL logic
  calculated_line_item AS (
    -- 1. Consumer Devices non-WLS
    SELECT
      'Consumer Devices non-WLS' AS LineItem,
      year,
      month,
      SUM(
        CASE
          WHEN LineItem = 'Calculated Consumer Devices WLS' THEN Amount
          WHEN LineItem = 'Consumer Devices WLS' THEN -Amount
          ELSE 0
        END
      ) AS Amount
    FROM monthly_summary
    GROUP BY year, month
    HAVING Amount <> 0

    UNION ALL

    -- 2. Consumer Transportation and Logistics Direct Costs non-WLS
    SELECT
      'Consumer Transportation and Logistics Direct Costs non-WLS' AS LineItem,
      year,
      month,
      SUM(
        CASE
          WHEN LineItem = 'Calculated Consumer Transportation and Logistics Direct Costs WLS' THEN Amount
          WHEN LineItem = 'Consumer Transportation and Logistics Direct Costs WLS' THEN -Amount
          ELSE 0
        END
      ) AS Amount
    FROM monthly_summary
    GROUP BY year, month
    HAVING Amount <> 0

    UNION ALL

    -- 3. Business Devices non-WLS
    SELECT
      'Business Devices non-WLS' AS LineItem,
      year,
      month,
      SUM(
        CASE
          WHEN LineItem = 'Calculated Business Devices WLS' THEN Amount
          WHEN LineItem = 'Business Devices WLS' THEN -Amount
          ELSE 0
        END
      ) AS Amount
    FROM monthly_summary
    GROUP BY year, month
    HAVING Amount <> 0

    UNION ALL

    -- 4. Business Transportation and Logistics Direct Costs non-WLS
    SELECT
      'Business Transportation and Logistics Direct Costs non-WLS' AS LineItem,
      year,
      month,
      SUM(
        CASE
          WHEN LineItem = 'Calculated Business Transportation and Logistics Direct Costs WLS' THEN Amount
          WHEN LineItem = 'Business Transportation and Logistics Direct Costs WLS' THEN -Amount
          ELSE 0
        END
      ) AS Amount
    FROM monthly_summary
    GROUP BY year, month
    HAVING Amount <> 0
  )

-- Final output combining base raw line items and calculated line items

SELECT LineItem, year, month, Amount
FROM monthly_summary
WHERE LineItem NOT LIKE '%Calculated%'

UNION ALL

SELECT LineItem, year, month, Amount
FROM calculated_line_item
-- ORDER BY LineItem, year, month;