-- TPC-DS Q90: AM vs PM Web Sales Ratio
SELECT CAST(amc AS DECIMAL(17, 2)) / CAST(pmc AS DECIMAL(17, 2)) AS am_pm_ratio
FROM (
    SELECT COUNT(*) AS amc
    FROM web_sales
    JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
    JOIN time_dim ON ws_sold_time_sk = t_time_sk
    JOIN web_page ON ws_web_page_sk = wp_web_page_sk
    WHERE t_hour BETWEEN 8 AND 12
      AND hd_dep_count = 5
      AND wp_web_page_name = 'YourCompany'
) amt,
(
    SELECT COUNT(*) AS pmc
    FROM web_sales
    JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
    JOIN time_dim ON ws_sold_time_sk = t_time_sk
    JOIN web_page ON ws_web_page_sk = wp_web_page_sk
    WHERE t_hour BETWEEN 13 AND 17
      AND hd_dep_count = 5
      AND wp_web_page_name = 'YourCompany'
) pmt
