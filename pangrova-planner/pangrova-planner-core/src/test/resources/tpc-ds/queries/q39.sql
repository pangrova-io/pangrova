-- TPC-DS Q39: Inventory variance analysis
WITH inv AS (
    SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
        stdev(inv_quantity_on_hand) AS stdev_inv, AVG(inv_quantity_on_hand) AS mean_inv
    FROM inventory
    JOIN date_dim ON inv_date_sk = d_date_sk
    JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
    JOIN item ON inv_item_sk = i_item_sk
    WHERE d_year = 2001
    GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
)
SELECT inv1.w_warehouse_name, inv1.i_item_sk, inv1.d_moy,
    inv1.mean_inv, inv1.stdev_inv, inv2.mean_inv, inv2.stdev_inv
FROM inv inv1
JOIN inv inv2 ON inv1.i_item_sk = inv2.i_item_sk
    AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
WHERE inv1.d_moy = 1
    AND inv2.d_moy = 2
    AND inv1.stdev_inv > 0
    AND inv1.mean_inv > 0
    AND inv2.stdev_inv / inv2.mean_inv > 1.5
ORDER BY inv1.w_warehouse_name, inv1.i_item_sk, inv1.d_moy
FETCH FIRST 100 ROWS ONLY
