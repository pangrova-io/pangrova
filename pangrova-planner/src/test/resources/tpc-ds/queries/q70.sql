-- TPC-DS Q70: Store Sales by State Rollup
SELECT SUM(ss_net_profit) AS total_sum, s_state, s_county,
       GROUPING(s_state) + GROUPING(s_county) AS lochierarchy,
       RANK() OVER (PARTITION BY GROUPING(s_state) + GROUPING(s_county),
                    CASE WHEN GROUPING(s_county) = 0 THEN s_state END
                    ORDER BY SUM(ss_net_profit) DESC) AS rank_within_parent
FROM store_sales
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE d1.d_month_seq BETWEEN 1200 AND 1200 + 11
  AND s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN')
GROUP BY ROLLUP(s_state, s_county)
ORDER BY lochierarchy DESC,
         CASE WHEN lochierarchy = 0 THEN s_state END,
         rank_within_parent
FETCH FIRST 100 ROWS ONLY
