-- TPC-DS Q60: Monthly revenue specific items
WITH ss AS (
    SELECT i_item_id, SUM(ss_ext_sales_price) AS total_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN item ON ss_item_sk = i_item_sk
    WHERE i_category IN ('Music')
        AND d_year = 1998
        AND d_moy BETWEEN 9 AND 9 + 2
    GROUP BY i_item_id
),
cs AS (
    SELECT i_item_id, SUM(cs_ext_sales_price) AS total_sales
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN item ON cs_item_sk = i_item_sk
    WHERE i_category IN ('Music')
        AND d_year = 1998
        AND d_moy BETWEEN 9 AND 9 + 2
    GROUP BY i_item_id
),
ws AS (
    SELECT i_item_id, SUM(ws_ext_sales_price) AS total_sales
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    JOIN item ON ws_item_sk = i_item_sk
    WHERE i_category IN ('Music')
        AND d_year = 1998
        AND d_moy BETWEEN 9 AND 9 + 2
    GROUP BY i_item_id
)
SELECT ss.i_item_id,
    ss.total_sales + COALESCE(cs.total_sales, 0) + COALESCE(ws.total_sales, 0) AS total_sales
FROM ss
LEFT JOIN cs ON ss.i_item_id = cs.i_item_id
LEFT JOIN ws ON ss.i_item_id = ws.i_item_id
ORDER BY total_sales DESC
FETCH FIRST 100 ROWS ONLY
