-- TPC-DS Q91: Call Center Returns by Reason
SELECT cc_call_center_id AS call_center, cc_name, cc_manager, returns, reason
FROM (
    SELECT cr_call_center_sk, SUM(cr_return_amount) AS returns, r_reason_desc AS reason
    FROM catalog_returns
    JOIN date_dim ON cr_returned_date_sk = d_date_sk
    JOIN reason ON cr_reason_sk = r_reason_sk
    WHERE d_year = 2000
      AND r_reason_desc LIKE 'reason%'
    GROUP BY cr_call_center_sk, r_reason_desc
) returns_info
JOIN call_center ON cr_call_center_sk = cc_call_center_sk
ORDER BY returns DESC
FETCH FIRST 100 ROWS ONLY
