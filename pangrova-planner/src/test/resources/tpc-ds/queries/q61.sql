-- TPC-DS Q61: Promotional sales effectiveness
SELECT promotions, total, CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4)) * 100 AS promo_pct
FROM (
    SELECT SUM(ss_ext_sales_price) AS promotions
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    JOIN promotion ON ss_promo_sk = p_promo_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE p_channel_email = 'N'
        OR p_channel_event = 'N'
        AND ca_gmt_offset = -5
        AND d_year = 1998
) promo_sales,
(
    SELECT SUM(ss_ext_sales_price) AS total
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE ca_gmt_offset = -5
        AND d_year = 1998
) total_sales
