-- TPC-DS Q35: Customer demographics across channels
SELECT ca_state, cd_gender, cd_marital_status, COUNT(*) AS cnt1,
    MIN(cd_dep_count) AS min_dep_count, AVG(cd_dep_count) AS avg_dep_count,
    MAX(cd_dep_count) AS max_dep_count, MIN(cd_dep_employed_count) AS min_dep_employed_count,
    AVG(cd_dep_employed_count) AS avg_dep_employed_count, MAX(cd_dep_employed_count) AS max_dep_employed_count,
    MIN(cd_dep_college_count) AS min_dep_college_count, AVG(cd_dep_college_count) AS avg_dep_college_count,
    MAX(cd_dep_college_count) AS max_dep_college_count
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
WHERE cd_marital_status = 'M'
    AND cd_education_status = '4 yr Degree'
    AND (
        EXISTS (
            SELECT 1 FROM store_sales ss
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE ss.ss_customer_sk = c.c_customer_sk
                AND d.d_year = 2002 AND d.d_qoy <= 3
        )
        OR EXISTS (
            SELECT 1 FROM web_sales ws
            JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
            WHERE ws.ws_bill_customer_sk = c.c_customer_sk
                AND d.d_year = 2002 AND d.d_qoy <= 3
        )
        OR EXISTS (
            SELECT 1 FROM catalog_sales cs
            JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
            WHERE cs.cs_ship_customer_sk = c.c_customer_sk
                AND d.d_year = 2002 AND d.d_qoy <= 3
        )
    )
GROUP BY ca_state, cd_gender, cd_marital_status
ORDER BY ca_state, cd_gender, cd_marital_status
FETCH FIRST 100 ROWS ONLY
