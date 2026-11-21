-- TPC-H Q13: Customer Distribution
-- LEFT JOIN + GROUP BY + subquery for customer order distribution
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c_custkey,
            COUNT(o_orderkey) AS c_count
        FROM
            customer
            LEFT OUTER JOIN orders ON c_custkey = o_custkey
                AND NOT (o_comment LIKE '%special%requests%')
        GROUP BY
            c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
