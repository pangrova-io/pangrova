-- TPC-DS Q43: Weekly store sales for year
SELECT s_store_name, s_store_id,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price ELSE 0 END) AS sun_sales,
    SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price ELSE 0 END) AS mon_sales,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price ELSE 0 END) AS tue_sales,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price ELSE 0 END) AS wed_sales,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price ELSE 0 END) AS thu_sales,
    SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price ELSE 0 END) AS fri_sales,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price ELSE 0 END) AS sat_sales
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE d_year = 1998
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name, s_store_id
FETCH FIRST 100 ROWS ONLY
