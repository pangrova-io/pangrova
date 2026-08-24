-- TPC-DS Q55: Top brand revenue
SELECT i_brand_id AS brand_id, i_brand AS brand, SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 36
    AND d_moy = 12
    AND d_year = 2001
GROUP BY i_brand_id, i_brand
ORDER BY ext_price DESC
FETCH FIRST 100 ROWS ONLY
