-- TPC-DS Q2: Websales and Catalog Sales by Day of Week
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
        SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) AS sun_sales,
        SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) AS mon_sales,
        SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) AS tue_sales,
        SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) AS wed_sales,
        SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) AS thu_sales,
        SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) AS fri_sales,
        SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) AS sat_sales
    FROM wscs
    JOIN date_dim ON sold_date_sk = d_date_sk
    GROUP BY d_week_seq
)
SELECT d1.d_week_seq,
    ROUND(w1.sun_sales / w2.sun_sales, 2) AS sun_ratio,
    ROUND(w1.mon_sales / w2.mon_sales, 2) AS mon_ratio,
    ROUND(w1.tue_sales / w2.tue_sales, 2) AS tue_ratio,
    ROUND(w1.wed_sales / w2.wed_sales, 2) AS wed_ratio,
    ROUND(w1.thu_sales / w2.thu_sales, 2) AS thu_ratio,
    ROUND(w1.fri_sales / w2.fri_sales, 2) AS fri_ratio,
    ROUND(w1.sat_sales / w2.sat_sales, 2) AS sat_ratio
FROM wswscs w1
JOIN date_dim d1 ON d1.d_week_seq = w1.d_week_seq AND d1.d_year = 2001
JOIN wswscs w2 ON w1.d_week_seq = w2.d_week_seq + 53
ORDER BY d1.d_week_seq
