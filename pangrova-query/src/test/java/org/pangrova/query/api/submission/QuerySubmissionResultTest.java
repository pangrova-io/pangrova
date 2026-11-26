package org.pangrova.query.api.submission;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.client.result.ResultEndpoint;
import static org.assertj.core.api.Assertions.*;

class QuerySubmissionResultTest {
    @Test
    void freezesColumnsWithoutTakingOwnershipOfTheResultEndpoint() {
        var columns = new ArrayList<>(List.of("a", "结果"));
        var endpoint = new ResultEndpoint("worker", 1234, "ticket", "grpc");
        var result = new QuerySubmissionResult(
                "q", QueryState.RUNNING, ResultDisposition.STREAM, endpoint, columns);
        columns.clear();
        assertThat(result.columnNames()).containsExactly("a", "结果");
        assertThatThrownBy(() -> result.columnNames().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.resultEndpoint()).isSameAs(endpoint);
        assertThat(result.state()).isEqualTo(QueryState.RUNNING);
    }

    @Test
    void supportsResultlessDispatchAndRejectsMissingIdentityOrState() {
        var result = new QuerySubmissionResult("q", QueryState.RUNNING, ResultDisposition.NONE, null, null);
        assertThat(result.resultDisposition()).isEqualTo(ResultDisposition.NONE);
        assertThat(result.resultEndpoint()).isNull();
        assertThat(result.columnNames()).isEmpty();
        assertThatThrownBy(() -> new QuerySubmissionResult(
                null, QueryState.RUNNING, ResultDisposition.NONE, null, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new QuerySubmissionResult(
                "q", null, ResultDisposition.NONE, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void dispositionMustAgreeWithEndpointAndTerminalState() {
        var endpoint = new ResultEndpoint("worker", 1234, "ticket", "grpc");
        assertThatThrownBy(() -> new QuerySubmissionResult(
                "q", QueryState.RUNNING, ResultDisposition.STREAM, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QuerySubmissionResult(
                "q", QueryState.RUNNING, ResultDisposition.PENDING, endpoint, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QuerySubmissionResult(
                "q", QueryState.FINISHED, ResultDisposition.PENDING, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
