-- TPC-DS Q83: Returns Analysis with Items
WITH sr_items AS (
    SELECT i_item_id AS item_id, SUM(sr_return_quantity) AS sr_item_qty
    FROM store_returns
    JOIN item ON sr_item_sk = i_item_sk
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN customer ON sr_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE d_date IN (
        SELECT d_date FROM date_dim WHERE d_moy IN (6, 7, 8) AND d_year = 2000
    )
      AND ca_state IN ('IN', 'OH', 'NJ', 'WI', 'MO')
    GROUP BY i_item_id
),
cr_items AS (
    SELECT i_item_id AS item_id, SUM(cr_return_quantity) AS cr_item_qty
    FROM catalog_returns
    JOIN item ON cr_item_sk = i_item_sk
    JOIN date_dim ON cr_returned_date_sk = d_date_sk
    JOIN customer ON cr_returning_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE d_date IN (
        SELECT d_date FROM date_dim WHERE d_moy IN (6, 7, 8) AND d_year = 2000
    )
      AND ca_state IN ('IN', 'OH', 'NJ', 'WI', 'MO')
    GROUP BY i_item_id
),
wr_items AS (
    SELECT i_item_id AS item_id, SUM(wr_return_quantity) AS wr_item_qty
    FROM web_returns
    JOIN item ON wr_item_sk = i_item_sk
    JOIN date_dim ON wr_returned_date_sk = d_date_sk
    JOIN customer ON wr_returning_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE d_date IN (
        SELECT d_date FROM date_dim WHERE d_moy IN (6, 7, 8) AND d_year = 2000
    )
      AND ca_state IN ('IN', 'OH', 'NJ', 'WI', 'MO')
    GROUP BY i_item_id
)
SELECT sr_items.item_id, sr_items.sr_item_qty,
       cr_items.cr_item_qty, wr_items.wr_item_qty
FROM sr_items, cr_items, wr_items
WHERE sr_items.item_id = cr_items.item_id
  AND sr_items.item_id = wr_items.item_id
ORDER BY sr_items.item_id, sr_items.sr_item_qty
FETCH FIRST 100 ROWS ONLY
