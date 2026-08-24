-- TPC-DS Q55: Brand Revenue by Month
-- Report the total extended price for items of a specific brand
-- sold in stores in a specific month and year.
SELECT
    i_brand_id AS brand_id,
    i_brand AS brand,
    SUM(ss_ext_sales_price) AS ext_price
FROM date_dim
JOIN store_sales ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 28
AND d_moy = 11
AND d_year = 1999
GROUP BY i_brand_id, i_brand
ORDER BY ext_price DESC, brand_id
FETCH FIRST 100 ROWS ONLY
