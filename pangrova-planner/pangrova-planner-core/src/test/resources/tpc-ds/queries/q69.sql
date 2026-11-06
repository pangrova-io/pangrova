-- TPC-DS Q69: Customer Purchase Across Channels
SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1,
       cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3,
       cd_dep_count, COUNT(*) AS cnt4, cd_dep_employed_count, COUNT(*) AS cnt5,
       cd_dep_college_count, COUNT(*) AS cnt6
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
WHERE ca.ca_state IN ('KY', 'GA', 'NM')
  AND cd.cd_marital_status = 'M'
  AND cd.cd_education_status = '4 yr Degree'
  AND EXISTS (
      SELECT * FROM store_sales WHERE ss_customer_sk = c.c_customer_sk
  )
  AND EXISTS (
      SELECT * FROM web_sales WHERE ws_bill_customer_sk = c.c_customer_sk
  )
  AND EXISTS (
      SELECT * FROM catalog_sales WHERE cs_bill_customer_sk = c.c_customer_sk
  )
GROUP BY cd_gender, cd_marital_status, cd_education_status,
         cd_purchase_estimate, cd_credit_rating,
         cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender, cd_marital_status, cd_education_status,
         cd_purchase_estimate, cd_credit_rating,
         cd_dep_count, cd_dep_employed_count, cd_dep_college_count
FETCH FIRST 100 ROWS ONLY
