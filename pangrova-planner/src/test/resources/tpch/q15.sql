-- TPC-H Query 15: Top Supplier
-- Rewritten without CTE due to planner CTE type inference limitation (tracked in Task 12)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier, (
    SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
    GROUP BY l_suppkey
) AS revenue
WHERE s_suppkey = supplier_no
    AND total_revenue = (
        SELECT MAX(total_revenue2)
        FROM (
            SELECT SUM(l_extendedprice * (1 - l_discount)) AS total_revenue2
            FROM lineitem
            WHERE l_shipdate >= DATE '1996-01-01'
                AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
            GROUP BY l_suppkey
        ) AS revenue2
    )
ORDER BY s_suppkey
