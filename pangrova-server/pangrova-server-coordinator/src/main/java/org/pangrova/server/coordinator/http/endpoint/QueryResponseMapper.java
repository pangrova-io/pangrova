package org.pangrova.server.coordinator.http.endpoint;

import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.submission.QuerySubmissionResult;

/** Stateless conversion from query observations to the existing REST wire DTOs. */
final class QueryResponseMapper {
    private QueryResponseMapper() { }

    static SubmitQueryResponse submission(QuerySubmissionResult result) {
        return new SubmitQueryResponse(result.queryId(), result.state().name(),
                result.resultEndpoint(), result.columnNames(), result.branchSelection());
    }

    static ErrorDetail failure(QueryFailure failure) {
        return failure == null ? null : new ErrorDetail(
                failure.errorCode(), failure.errorType(), failure.message(), failure.stackTrace());
    }
}
