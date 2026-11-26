package org.pangrova.query.lifecycle;

import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.client.result.ResultEndpoint;

/** Closed control-plane outcome of one query dispatch. */
public record QueryDispatchOutcome(ResultDisposition resultDisposition, ResultEndpoint resultEndpoint) {
    public QueryDispatchOutcome {
        if (resultDisposition == null) {
            throw new IllegalArgumentException("resultDisposition must not be null");
        }
        resultDisposition.validateEndpoint(resultEndpoint);
    }

    public static QueryDispatchOutcome pending() {
        return new QueryDispatchOutcome(ResultDisposition.PENDING, null);
    }

    public static QueryDispatchOutcome none() {
        return new QueryDispatchOutcome(ResultDisposition.NONE, null);
    }

    public static QueryDispatchOutcome stream(ResultEndpoint resultEndpoint) {
        return new QueryDispatchOutcome(ResultDisposition.STREAM, resultEndpoint);
    }
}
