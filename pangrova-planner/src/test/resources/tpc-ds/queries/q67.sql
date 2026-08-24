-- TPC-DS Q67: Store Sales by Rollup Dimensions
SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy,
       SUM(ss_sales_price) AS sum_sales,
       AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) AS avg_monthly_sales
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE d_year IN (2000, 2001)
  AND ((i_category IN ('Books', 'Children', 'Electronics') AND
        i_class IN ('personal', 'portable', 'reference', 'self-help') AND
        s_store_name IN ('ese', 'anti')) OR
       (i_category IN ('Men', 'Jewelry', 'Sports') AND
        i_class IN ('shirts', 'birdal', 'football', 'baseball') AND
        s_store_name IN ('ese', 'anti')))
GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
ORDER BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
FETCH FIRST 100 ROWS ONLY
