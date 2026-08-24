-- TPC-DS Q96: Store Sales by Household Demographics
-- Count the number of store sales transactions with specific
-- household demographics and time constraints.
SELECT COUNT(*) AS cnt
FROM store_sales
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN time_dim ON ss_sold_time_sk = t_time_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE (hd_dep_count = 7 OR hd_vehicle_count > 0)
AND t_hour = 20
AND t_minute >= 30
AND s_store_name = 'ese'
ORDER BY cnt
FETCH FIRST 100 ROWS ONLY
