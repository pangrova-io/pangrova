-- TPC-DS Q37: Inventory items with price range
SELECT i_item_id, i_item_desc, i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
WHERE i_current_price BETWEEN 68 AND 68 + 30
    AND i_manufact_id IN (677, 940, 694, 808)
    AND d_date BETWEEN DATE '2000-02-01' AND DATE '2000-04-01'
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
FETCH FIRST 100 ROWS ONLY
