-- TPC-DS Q68: Customer Purchase in Stores
SELECT c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt, profit
FROM (
    SELECT ss_ticket_number,
           ss_customer_sk,
           ca_city AS bought_city,
           SUM(ss_coupon_amt) AS amt,
           SUM(ss_net_profit) AS profit
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE s_city IN ('Midway', 'Fairview', 'Fairview', 'Fairview', 'Fairview')
      AND d_dom BETWEEN 1 AND 3
      AND (d_year = 2000 OR d_year = 2001)
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
) a
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address current_addr ON c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> bought_city
ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
FETCH FIRST 100 ROWS ONLY
