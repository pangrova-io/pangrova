-- TPC-DS Q84: Customer with Income Band
SELECT c_customer_id AS customer_id,
       c_last_name || ', ' || c_first_name AS customername
FROM customer
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN income_band ON ib_income_band_sk = cd_income_band_sk
JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
WHERE ca_city = 'Edgewood'
ORDER BY c_customer_id
FETCH FIRST 100 ROWS ONLY
