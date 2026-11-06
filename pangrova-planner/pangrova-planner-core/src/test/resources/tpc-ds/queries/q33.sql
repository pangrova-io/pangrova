-- TPC-DS Q33: Revenue from items by manufacturer in categories
WITH ss AS (
    SELECT i_manufact_id, SUM(ss_ext_sales_price) AS total_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    JOIN item ON ss_item_sk = i_item_sk
    WHERE i_manufact_id IN (
        SELECT i_manufact_id
        FROM item
        WHERE i_category IN ('Electronics')
    )
    AND d_year = 1998
    AND ca_gmt_offset = -5
    GROUP BY i_manufact_id
)
SELECT i_manufact_id, SUM(total_sales) AS total_sales
FROM ss
GROUP BY i_manufact_id
ORDER BY total_sales
FETCH FIRST 100 ROWS ONLY
