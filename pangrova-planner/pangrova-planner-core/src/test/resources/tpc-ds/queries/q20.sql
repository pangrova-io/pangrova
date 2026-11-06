-- TPC-DS Q20: Catalog Sales by Item
-- The pre-aggregating CTE (cs_agg) avoids SUM(SUM(x)) OVER (PARTITION BY y),
-- which Calcite's StandardConvertletTable does not support. See q12 for details.
WITH cs_agg AS (
    SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price,
        SUM(cs_ext_sales_price) AS itemrevenue
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE i_category IN ('Sports', 'Books', 'Home')
        AND d_date BETWEEN DATE '1999-02-22' AND DATE '1999-03-24'
    GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue,
    itemrevenue * 100.0 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio
FROM cs_agg
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
