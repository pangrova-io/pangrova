-- TPC-DS Q58: Cross-channel item revenue
WITH ss_items AS (
    SELECT i_item_id, SUM(ss_ext_sales_price) AS revenue
    FROM store_sales
    JOIN item ON ss_item_sk = i_item_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 1998
        AND d_moy BETWEEN 2 AND 2 + 2
    GROUP BY i_item_id
),
cs_items AS (
    SELECT i_item_id, SUM(cs_ext_sales_price) AS revenue
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year = 1998
        AND d_moy BETWEEN 2 AND 2 + 2
    GROUP BY i_item_id
),
ws_items AS (
    SELECT i_item_id, SUM(ws_ext_sales_price) AS revenue
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year = 1998
        AND d_moy BETWEEN 2 AND 2 + 2
    GROUP BY i_item_id
)
SELECT ss_items.i_item_id,
    ss_items.revenue AS ss_revenue,
    cs_items.revenue AS cs_revenue,
    ws_items.revenue AS ws_revenue
FROM ss_items
JOIN cs_items ON ss_items.i_item_id = cs_items.i_item_id
JOIN ws_items ON ss_items.i_item_id = ws_items.i_item_id
ORDER BY ss_items.i_item_id
FETCH FIRST 100 ROWS ONLY
