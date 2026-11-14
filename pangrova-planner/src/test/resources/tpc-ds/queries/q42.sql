-- TPC-DS Q42: Item sales growth year/month
SELECT d_year, i_item_id, i_category, i_class, i_current_price,
    SUM(cs_ext_sales_price) AS itemrevenue,
    SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class ORDER BY d_year, i_item_id) AS cum_revenue
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE d_year IN (1998, 1999, 2000)
    AND i_category IN ('Sports', 'Books', 'Home')
GROUP BY d_year, i_item_id, i_category, i_class, i_current_price
ORDER BY d_year, i_item_id, i_category, i_class, i_current_price
FETCH FIRST 100 ROWS ONLY
