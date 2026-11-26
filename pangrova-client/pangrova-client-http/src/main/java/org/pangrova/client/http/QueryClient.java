package org.pangrova.client.http;

import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.transport.client.ResultCursor;

import java.util.Map;

/** Query operations used by interactive and programmatic clients. The HTTP client owns its connections. */
public interface QueryClient {
    QueryExecution beginQuery(String sql, Map<String, String> sessionProperties);

    /** Opens a single-consumer result within the caller's absolute deadline. The caller owns the cursor. */
    ResultCursor openResult(ResultEndpoint endpoint, long deadlineEpochMs);
}
