package org.pangrova.client.http;

import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.query.SubmitQueryResponse;

/** Owns controls for one submission, including signed receipt recovery after owner loss. */
public interface QueryExecution {
    SubmitQueryResponse submission();

    /** Waits for the requested owner event or a bounded observation window. One reader at a time. */
    QueryStatusResponse awaitStatus(QueryWaitTarget target, long deadlineEpochMillis);

    /** Requests cancellation of this exact submitted query. */
    CancelOutcome cancel();
}
