-- TPC-DS Q48: Store sales demographic constraints
SELECT SUM(ss_quantity) AS total_quantity
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
WHERE d_year = 2000
    AND (
        (hd_dep_count = 4 AND hd_vehicle_count <= 2)
        OR (hd_dep_count = 2 AND hd_vehicle_count <= 2)
        OR (hd_dep_count = 0 AND hd_vehicle_count <= 1)
    )
    AND ca_state IN ('OH', 'TX', 'CA')
