package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.enums.QueryWaitTarget;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Waits for real Native results and the query owner's physical release. */
final class ProcessQueryResults {
    private ProcessQueryResults() { }

    static ResultEndpoint awaitResult(QueryExecution query, long deadline) {
        var endpoint = query.submission().resultEndpoint();
        while (endpoint == null) {
            var status = query.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
            assertThat(status.state()).as("Query %s error %s", status.queryId(), status.error())
                    .isNotIn("FAILED", "CANCELLED");
            endpoint = status.resultEndpoint();
            if ("FINISHED".equals(status.state())) assertThat(endpoint).isNotNull();
        }
        assertThat(endpoint.protocol()).isEqualTo(ResultProtocol.GRPC_TLS.value());
        return endpoint;
    }

    static ResultTiming consumeAndDrain(PangrovaRestClient client, QueryExecution query, ResultEndpoint endpoint,
                                       List<Long> expected, long deadline) {
        var values = new ArrayList<Long>();
        long firstRow;
        try (var cursor = client.openResult(endpoint, deadline)) {
            assertThat(cursor.next()).isTrue();
            firstRow = System.nanoTime();
            do {
                values.add(((Number) cursor.value(0)).longValue());
            } while (cursor.next());
        }
        long eof = System.nanoTime();
        assertThat(values).containsExactlyElementsOf(expected);
        var status = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        while (!Boolean.TRUE.equals(status.drained())) {
            assertThat(status.state()).as("Query %s error %s", status.queryId(), status.error())
                    .isNotIn("FAILED", "CANCELLED");
            status = query.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        }
        assertThat(status.state()).isEqualTo("FINISHED");
        return new ResultTiming(firstRow, eof);
    }

    record ResultTiming(long firstRow, long eof) { }
}
