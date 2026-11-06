-- TPC-DS Q52: Web sales by dept/year
SELECT dt.d_year, item.i_brand_id AS brand_id, item.i_brand AS brand,
    SUM(ws_ext_sales_price) AS ext_price
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim dt ON ws_sold_date_sk = d_date_sk
WHERE item.i_manager_id = 1
    AND dt.d_moy = 11
    AND dt.d_year = 2000
GROUP BY dt.d_year, item.i_brand_id, item.i_brand
ORDER BY ext_price DESC, brand_id
FETCH FIRST 100 ROWS ONLY
