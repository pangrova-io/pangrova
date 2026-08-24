-- TPC-DS Q73: Store Customers by Ticket Count
SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag,
       ss_ticket_number, cnt
FROM (
    SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt
    FROM store_sales
    WHERE ss_store_sk IN (4, 11, 68, 93)
      AND ss_sold_date_sk BETWEEN 2451179 AND 2451269
    GROUP BY ss_ticket_number, ss_customer_sk
    HAVING COUNT(*) >= 4
) sales
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY cnt DESC, c_last_name, c_first_name
FETCH FIRST 100 ROWS ONLY
