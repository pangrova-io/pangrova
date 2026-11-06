-- TPC-DS Q93: Store Returns with Reason
SELECT ss_customer_sk, SUM(act_sales) AS sumsales
FROM (
    SELECT ss_item_sk, ss_ticket_number, ss_customer_sk,
           CASE WHEN sr_return_quantity IS NOT NULL
                THEN (ss_quantity - sr_return_quantity) * ss_sales_price
                ELSE ss_quantity * ss_sales_price END AS act_sales
    FROM store_sales
    LEFT JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
    JOIN reason ON sr_reason_sk = r_reason_sk
    WHERE r_reason_desc = 'reason 1'
) t
GROUP BY ss_customer_sk
ORDER BY sumsales DESC, ss_customer_sk
FETCH FIRST 100 ROWS ONLY
