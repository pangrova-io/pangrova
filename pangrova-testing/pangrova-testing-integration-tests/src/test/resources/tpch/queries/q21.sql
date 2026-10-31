-- TPC-H Q21: Suppliers Who Kept Orders Waiting
-- Rewritten: use unique alias columns in subqueries to avoid
-- Velox duplicate column name error on join sides
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier
    INNER JOIN nation ON s_nationkey = n_nationkey
    INNER JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
    INNER JOIN orders ON o_orderkey = l1.l_orderkey
    INNER JOIN (
        SELECT l_orderkey AS ms_orderkey
        FROM lineitem
        GROUP BY l_orderkey
        HAVING COUNT(DISTINCT l_suppkey) > 1
    ) AS multi_supp ON l1.l_orderkey = multi_supp.ms_orderkey
    LEFT JOIN (
        SELECT l_orderkey AS lo_orderkey, l_suppkey AS lo_suppkey
        FROM lineitem
        WHERE l_receiptdate > l_commitdate
    ) AS late_others ON l1.l_orderkey = late_others.lo_orderkey
        AND late_others.lo_suppkey <> l1.l_suppkey
WHERE
    n_name = 'SAUDI ARABIA'
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND late_others.lo_orderkey IS NULL
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100
