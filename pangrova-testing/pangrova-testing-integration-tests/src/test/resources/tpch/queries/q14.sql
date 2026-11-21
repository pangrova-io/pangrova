-- TPC-H Q14: Promotion Effect
-- ELSE uses 0.0 (DOUBLE) to match the THEN branch type and avoid
-- Calcite REDUCE_EXPRESSIONS producing a DECIMAL literal with corrupted
-- precision/scale that causes a Velox SWITCH type mismatch.
SELECT
    100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0.0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem
    INNER JOIN part ON l_partkey = p_partkey
WHERE
    l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-10-01'
