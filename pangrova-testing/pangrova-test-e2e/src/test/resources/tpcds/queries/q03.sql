-- TPC-DS Q3: Item Revenue by Brand
-- Report the total extended sales price per item brand of a specific
-- manufacturer for all sales in a specific month of the year.
SELECT
    dt.d_year,
    item.i_brand_id AS brand_id,
    item.i_brand AS brand,
    SUM(ss_ext_sales_price) AS sum_agg
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manufact_id = 128
AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand_id, item.i_brand
ORDER BY dt.d_year, sum_agg DESC, brand_id
FETCH FIRST 100 ROWS ONLY
