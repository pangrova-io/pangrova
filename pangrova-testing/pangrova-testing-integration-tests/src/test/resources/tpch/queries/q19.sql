-- TPC-H Q19: Discounted Revenue
-- Rewritten from OR to UNION ALL to avoid Calcite AND type validation bug
SELECT
    SUM(revenue) AS revenue
FROM (
    SELECT l_extendedprice * (1 - l_discount) AS revenue
    FROM lineitem INNER JOIN part ON p_partkey = l_partkey
    WHERE p_brand = 'Brand#12'
      AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      AND l_quantity >= 1 AND l_quantity <= 11
      AND p_size >= 1 AND p_size <= 5
      AND l_shipmode IN ('AIR', 'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON'
    UNION ALL
    SELECT l_extendedprice * (1 - l_discount) AS revenue
    FROM lineitem INNER JOIN part ON p_partkey = l_partkey
    WHERE p_brand = 'Brand#23'
      AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      AND l_quantity >= 10 AND l_quantity <= 20
      AND p_size >= 1 AND p_size <= 10
      AND l_shipmode IN ('AIR', 'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON'
    UNION ALL
    SELECT l_extendedprice * (1 - l_discount) AS revenue
    FROM lineitem INNER JOIN part ON p_partkey = l_partkey
    WHERE p_brand = 'Brand#34'
      AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      AND l_quantity >= 20 AND l_quantity <= 30
      AND p_size >= 1 AND p_size <= 15
      AND l_shipmode IN ('AIR', 'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON'
) AS combined
