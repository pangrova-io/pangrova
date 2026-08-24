-- TPC-DS Q64: Store sales with item detail
WITH cs_ui AS (
    SELECT cs_item_sk, SUM(cs_ext_sales_price) AS total_sales
    FROM catalog_sales
    GROUP BY cs_item_sk
),
avg_sales AS (
    SELECT AVG(total_sales) AS avg_sales
    FROM cs_ui
)
SELECT s_store_name, s_company_id, street_number, street_name, street_type,
    suite_number, city, county, state, zip,
    SUM(ss_ext_sales_price) AS total_sales
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = (
    SELECT i_manager_id
    FROM item
    WHERE i_category = 'Music'
    ORDER BY SUM(cs_ext_sales_price) DESC
    FETCH FIRST 1 ROW ONLY
)
AND d_year = 2001
AND d_moy BETWEEN 8 AND 8 + 3
GROUP BY s_store_name, s_company_id, street_number, street_name, street_type,
    suite_number, city, county, state, zip
ORDER BY total_sales DESC
FETCH FIRST 100 ROWS ONLY
