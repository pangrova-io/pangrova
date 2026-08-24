-- TPC-DS Q50: Store returns by reason
SELECT s_store_name, s_company_id, street_number, street_name, street_type,
    suite_number, city, county, state, zip,
    SUM(CASE WHEN sr_returned_date_sk - ss_sold_date_sk <= 30 THEN 1 ELSE 0 END) AS days_30,
    SUM(CASE WHEN sr_returned_date_sk - ss_sold_date_sk > 30 AND sr_returned_date_sk - ss_sold_date_sk <= 60 THEN 1 ELSE 0 END) AS days_31_60,
    SUM(CASE WHEN sr_returned_date_sk - ss_sold_date_sk > 60 AND sr_returned_date_sk - ss_sold_date_sk <= 90 THEN 1 ELSE 0 END) AS days_61_90,
    SUM(CASE WHEN sr_returned_date_sk - ss_sold_date_sk > 90 AND sr_returned_date_sk - ss_sold_date_sk <= 120 THEN 1 ELSE 0 END) AS days_91_120,
    SUM(CASE WHEN sr_returned_date_sk - ss_sold_date_sk > 120 THEN 1 ELSE 0 END) AS days_over_120
FROM store_sales
JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE d1.d_year = 2001
GROUP BY s_store_name, s_company_id, street_number, street_name, street_type,
    suite_number, city, county, state, zip
ORDER BY s_store_name, s_company_id, street_number, street_name, street_type,
    suite_number, city, county, state, zip
FETCH FIRST 100 ROWS ONLY
