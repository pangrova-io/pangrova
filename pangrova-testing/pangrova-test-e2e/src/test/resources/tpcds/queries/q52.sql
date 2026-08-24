-- TPC-DS Q52: Web Sales by Department
-- Report the total extended price for items in a specific department
-- sold via store in a specific year and month.
SELECT
    dt.d_year,
    item.i_brand_id AS brand_id,
    item.i_brand AS brand,
    SUM(ss_ext_sales_price) AS ext_price
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 1
AND dt.d_moy = 11
AND dt.d_year = 2000
GROUP BY dt.d_year, item.i_brand_id, item.i_brand
ORDER BY dt.d_year, ext_price DESC, brand_id
FETCH FIRST 100 ROWS ONLY
