-- TPC-DS Q36: Gross profit margin by store
SELECT SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
    i_category, i_class,
    GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
    i_category, i_class
FROM store_sales
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE d1.d_year = 2001
    AND s_state IN ('TN', 'TN')
GROUP BY ROLLUP(i_category, i_class)
ORDER BY gross_margin, lochierarchy DESC, i_category, i_class
FETCH FIRST 100 ROWS ONLY
