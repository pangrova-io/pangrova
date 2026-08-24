-- TPC-DS Q21: Inventory Analysis
SELECT w_warehouse_name, i_item_id,
    SUM(CASE WHEN d_date < DATE '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
    SUM(CASE WHEN d_date >= DATE '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
FROM inventory
JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
JOIN item ON inv_item_sk = i_item_sk
JOIN date_dim ON inv_date_sk = d_date_sk
WHERE d_date BETWEEN DATE '2000-02-10' AND DATE '2000-04-10'
    AND i_current_price BETWEEN 0.99 AND 1.49
GROUP BY w_warehouse_name, i_item_id
HAVING SUM(CASE WHEN d_date < DATE '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) > 0
    AND (SUM(CASE WHEN d_date >= DATE '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) * 1.0
        / SUM(CASE WHEN d_date < DATE '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END)) BETWEEN 0.667 AND 1.5
ORDER BY w_warehouse_name, i_item_id
LIMIT 100
