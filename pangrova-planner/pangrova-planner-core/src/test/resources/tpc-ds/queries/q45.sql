-- TPC-DS Q45: Web sales for customers in geo areas
SELECT ca_zip, ca_city, SUM(ws_sales_price) AS total_sales
FROM web_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE d_year = 2001
    AND d_qoy = 2
    AND (SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
        OR ca_state IN ('CA', 'WA', 'GA')
        OR ws_sales_price > 500)
GROUP BY ca_zip, ca_city
ORDER BY ca_zip, ca_city
FETCH FIRST 100 ROWS ONLY
