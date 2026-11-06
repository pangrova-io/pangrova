-- TPC-DS Q34: Store sales with specific demographics
SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag,
    ss_ticket_number, cnt
FROM (
    SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
    WHERE (ss_sold_date_sk BETWEEN 2451180 AND 2451182
        OR ss_sold_date_sk BETWEEN 2451190 AND 2451192
        OR ss_sold_date_sk BETWEEN 2451200 AND 2451202)
    AND s_store_id IN ('TEN', 'TWO')
    AND hd_buy_potential > 10000
    AND hd_dep_count > 0
    GROUP BY ss_ticket_number, ss_customer_sk
) sales
JOIN customer ON sales.ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, c_salutation, cnt DESC
FETCH FIRST 100 ROWS ONLY
