package org.pangrova.common.client.query;

import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.enums.ResultDisposition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Submission receipt with an explicit pending, streaming, or resultless outcome. */
public record SubmitQueryResponse(
    @JsonProperty("query_id") String queryId,
    @JsonProperty("state") String state,
    @JsonProperty("result_disposition") ResultDisposition resultDisposition,
    @JsonProperty("result_endpoint") ResultEndpoint resultEndpoint,
    @JsonProperty("column_names") List<String> columnNames,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("branch_selection") BranchSelection branchSelection,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("owner_endpoint") QueryOwnerEndpoint ownerEndpoint
) {

    public SubmitQueryResponse(String queryId, String state, ResultDisposition resultDisposition,
            ResultEndpoint resultEndpoint, List<String> columnNames) {
        this(queryId, state, resultDisposition, resultEndpoint, columnNames, null, null);
    }

    public SubmitQueryResponse {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        state = QueryStatusResponse.requireCanonicalState(state);
        if (resultDisposition == null) {
            throw new IllegalArgumentException("resultDisposition must not be null");
        }
        if (resultDisposition == ResultDisposition.UNAVAILABLE) {
            throw new IllegalArgumentException("Submission cannot contain a recovered disposition");
        }
        resultDisposition.validateEndpoint(resultEndpoint);
        if (resultDisposition == ResultDisposition.PENDING && QueryStatusResponse.isTerminalState(state)) {
            throw new IllegalArgumentException("PENDING must not survive terminal completion");
        }
        columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
    }
}
