-- TPC-DS Q66: Sales by Carrier and Warehouse
WITH wscs AS (
    SELECT sold_date_sk, sales_price
    FROM (
        SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price
        FROM web_sales
        UNION ALL
        SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price
        FROM catalog_sales
    ) x
),
wswscs AS (
    SELECT d_week_seq,
           SUM(CASE WHEN sm_carrier = 'DHL' THEN sales_price ELSE 0 END) AS dhl_sales,
           SUM(CASE WHEN sm_carrier = 'UPS' THEN sales_price ELSE 0 END) AS ups_sales,
           SUM(CASE WHEN sm_carrier = 'FedEx' THEN sales_price ELSE 0 END) AS fedex_sales
    FROM wscs
    JOIN date_dim ON sold_date_sk = d_date_sk
    JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
    GROUP BY d_week_seq
)
SELECT w1.d_week_seq AS week1,
       w1.dhl_sales AS dhl_week1,
       w2.dhl_sales AS dhl_week2,
       w1.ups_sales AS ups_week1,
       w2.ups_sales AS ups_week2,
       w1.fedex_sales AS fedex_week1,
       w2.fedex_sales AS fedex_week2
FROM wswscs w1
JOIN wswscs w2 ON w1.d_week_seq = w2.d_week_seq + 1
ORDER BY w1.d_week_seq
FETCH FIRST 100 ROWS ONLY
