-- TPC-DS Q8: Store Sales in Select Markets
SELECT s_store_name, SUM(ss_net_profit) AS total_profit
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN customer_address ON s_zip = ca_zip
WHERE d_qoy = 2
    AND d_year = 1998
    AND ca_address_sk IN (
        SELECT ca_address_sk
        FROM customer_address
        WHERE SUBSTRING(ca_zip, 1, 5) IN (
            '24128', '76232', '65084', '87816', '83926',
            '77556', '20548', '26231', '43848', '15126',
            '91137', '61265', '98294', '25782', '17920',
            '18103', '23790', '57855', '67449', '32213'
        )
    )
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100
