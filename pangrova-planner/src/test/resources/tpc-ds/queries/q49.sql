-- TPC-DS Q49: Returns analysis across channels
SELECT channel, item, return_ratio, return_rank, currency_rank
FROM (
    SELECT 'web' AS channel, i_item_id AS item,
        (SUM(wr_refunded_amt) / SUM(ws_sales_price)) AS return_ratio,
        RANK() OVER (ORDER BY (SUM(wr_refunded_amt) / SUM(ws_sales_price)) DESC) AS return_rank,
        RANK() OVER (ORDER BY SUM(ws_sales_price) DESC) AS currency_rank
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN web_returns ON wr_order_number = ws_order_number AND wr_item_sk = ws_item_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year = 2001
    GROUP BY i_item_id
    UNION ALL
    SELECT 'catalog' AS channel, i_item_id AS item,
        (SUM(cr_refunded_amt) / SUM(cs_sales_price)) AS return_ratio,
        RANK() OVER (ORDER BY (SUM(cr_refunded_amt) / SUM(cs_sales_price)) DESC) AS return_rank,
        RANK() OVER (ORDER BY SUM(cs_sales_price) DESC) AS currency_rank
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN catalog_returns ON cr_order_number = cs_order_number AND cr_item_sk = cs_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year = 2001
    GROUP BY i_item_id
) sub
WHERE return_rank <= 10 OR currency_rank <= 10
ORDER BY channel, return_rank, currency_rank
FETCH FIRST 100 ROWS ONLY
