package org.pangrova.query.api.submission;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.transport.api.result.ResultEndpoint;
import static org.assertj.core.api.Assertions.*;

class QuerySubmissionResultTest {
    @Test
    void freezesColumnsWithoutTakingOwnershipOfTheResultEndpoint() {
        var columns = new ArrayList<>(List.of("a", "结果"));
        var endpoint = new ResultEndpoint("worker", 1234, "ticket", "flight");
        var result = new QuerySubmissionResult("q", QueryState.RUNNING, endpoint, columns);
        columns.clear();
        assertThat(result.columnNames()).containsExactly("a", "结果");
        assertThatThrownBy(() -> result.columnNames().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.resultEndpoint()).isSameAs(endpoint);
        assertThat(result.state()).isEqualTo(QueryState.RUNNING);
    }

    @Test
    void supportsWriteDispatchWithoutAResultEndpointAndRejectsMissingIdentityOrState() {
        var result = new QuerySubmissionResult("q", QueryState.RUNNING, null, null);
        assertThat(result.resultEndpoint()).isNull();
        assertThat(result.columnNames()).isEmpty();
        assertThatThrownBy(() -> new QuerySubmissionResult(null, QueryState.RUNNING, null, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new QuerySubmissionResult("q", null, null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
