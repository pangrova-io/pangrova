package org.pangrova.server.coordinator.http.endpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.common.client.result.ResultEndpoint;
import static org.assertj.core.api.Assertions.assertThat;

class QueryResponseMapperTest {
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void keepsExistingSubmitWireFieldsAndClientRoundTripForEveryObservedState() throws Exception {
        var endpoint = new ResultEndpoint("worker", 1234, "ticket", "flight");
        for (QueryState state : QueryState.values()) {
            var domain = new QuerySubmissionResult("q", state, endpoint, List.of("结果"));
            var encoded = json.writeValueAsString(QueryResponseMapper.submission(domain));
            assertThat(json.readTree(encoded)).isEqualTo(json.readTree(json.writeValueAsString(
                    new SubmitQueryResponse("q", state.name(), endpoint, List.of("结果")))));
            assertThat(json.readValue(encoded, SubmitQueryResponse.class).state()).isEqualTo(state.name());
            assertThat(json.readTree(encoded).fieldNames()).toIterable()
                    .containsExactlyInAnyOrder("query_id", "state", "result_endpoint", "column_names");
        }
    }

    @Test
    void preservesEndpointAbsenceAndEmptyColumnsForControlPlaneWrites() throws Exception {
        var encoded = json.readTree(json.writeValueAsString(QueryResponseMapper.submission(
                new QuerySubmissionResult("q", QueryState.RUNNING, null, null))));
        assertThat(encoded.get("result_endpoint").isNull()).isTrue();
        assertThat(encoded.get("column_names").isEmpty()).isTrue();
    }

    @Test
    void preservesAllDiagnosticsAndExistingNullOmission() throws Exception {
        for (QueryFailure failure : List.of(
                new QueryFailure("EXCHANGE-0001", "WorkerLost", "节点失败\nretry", "stack\nat source"),
                new QueryFailure(null, null, null, null),
                new QueryFailure("QUERY_TIMEOUT", "TIMEOUT", "elapsed"))) {
            ErrorDetail wire = QueryResponseMapper.failure(failure);
            assertThat(wire).isEqualTo(new ErrorDetail(
                    failure.errorCode(), failure.errorType(), failure.message(), failure.stackTrace()));
            String encoded = json.writeValueAsString(QueryStatusResponse.of("q", "FAILED", wire, null));
            assertThat(json.readValue(encoded, QueryStatusResponse.class).error()).isEqualTo(wire);
            if (failure.stackTrace() == null) assertThat(json.readTree(encoded).get("error").has("stack_trace")).isFalse();
        }
        assertThat(QueryResponseMapper.failure(null)).isNull();
        assertThat(json.readTree(json.writeValueAsString(QueryStatusResponse.of("q", "RUNNING", null, null)))
                .has("error")).isFalse();
    }
}
