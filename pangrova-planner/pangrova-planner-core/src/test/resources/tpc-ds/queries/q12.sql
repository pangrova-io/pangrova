-- TPC-DS Q12: Web Sales by Item Category and Class
-- The pre-aggregating CTE (ws_agg) avoids the standard TPC-DS pattern
-- SUM(SUM(x)) OVER (PARTITION BY y), which Calcite's StandardConvertletTable
-- does not support. Semantically equivalent: first GROUP BY to compute per-item
-- revenue, then a window function sums those across the i_class partition.
WITH ws_agg AS (
    SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price,
        SUM(ws_ext_sales_price) AS itemrevenue
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE i_category IN ('Sports', 'Books', 'Home')
        AND d_date BETWEEN DATE '1999-02-22' AND DATE '1999-03-24'
    GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue,
    itemrevenue * 100.0 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio
FROM ws_agg
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
