package org.pangrova.common.client.query;

import org.pangrova.common.client.result.ResultEndpoint;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response body after submitting a query via REST API.
 * Results are served via transport at {@code resultEndpoint}.
 */
public record SubmitQueryResponse(
    @JsonProperty("query_id") String queryId,
    @JsonProperty("state") String state,
    @JsonProperty("result_endpoint") ResultEndpoint resultEndpoint,
    @JsonProperty("column_names") List<String> columnNames,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("branch_selection") BranchSelection branchSelection
) {

    public SubmitQueryResponse(String queryId, String state, ResultEndpoint resultEndpoint, List<String> columnNames) {
        this(queryId, state, resultEndpoint, columnNames, null);
    }

    public SubmitQueryResponse {
        columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
    }
}
