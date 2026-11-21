package org.pangrova.common.client.query;

import org.pangrova.common.client.enums.CancelOutcome;

import org.pangrova.common.client.result.ResultEndpoint;

import java.util.Map;

/** Query operations used by interactive and programmatic clients. The HTTP client owns its connections. */
public interface QueryClient {
    SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties);

    QueryStatusResponse getQueryStatus(String queryId);

    CancelOutcome cancelQuery(String queryId);

    /** Fetches the result within the caller's absolute deadline. */
    byte[] fetchResult(ResultEndpoint endpoint, long deadlineEpochMs);
}
