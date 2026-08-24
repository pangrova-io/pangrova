-- TPC-H Q17: Small-Quantity-Order Revenue
-- Rewritten: use implicit cross-join style to match Q19 pattern
-- which successfully returns single row aggregates.
SELECT
    SUM(l1.l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l1
    INNER JOIN part ON p_partkey = l1.l_partkey
    INNER JOIN (
        SELECT l_partkey AS avg_pk, AVG(l_quantity) AS avg_qty
        FROM lineitem
        GROUP BY l_partkey
    ) AS avg_t ON p_partkey = avg_t.avg_pk
WHERE
    p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l1.l_quantity < 0.2 * avg_t.avg_qty
