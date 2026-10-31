package org.pangrova.client.cli;

import org.pangrova.client.transport.http.PangrovaRestClient;
import org.pangrova.client.core.api.ResultBatchConsumer;
import org.pangrova.client.core.protocol.QueryStatusResponse;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.client.core.protocol.SubmitQueryResponse;

import java.util.Map;

/**
 * Abstraction over the query-related operations of the REST client,
 * enabling testability of the SQL shell without requiring a real HTTP connection.
 */
interface QueryClient {

    SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties);

    QueryStatusResponse getQueryStatus(String queryId);

    PangrovaRestClient.CancelOutcome cancelQuery(String queryId);

    void fetchResultStream(ResultEndpoint endpoint, ResultBatchConsumer consumer);

    /**
     * Fetches query result as a single Arrow IPC byte array.
     * Default implementation throws; override or use {@link #wrap(PangrovaRestClient)}.
     */
    default byte[] fetchResult(ResultEndpoint endpoint) {
        throw new UnsupportedOperationException("fetchResult not implemented");
    }

    byte[] fetchResult(ResultEndpoint endpoint, long deadlineEpochMs);

    /**
     * Adapter that delegates to a real {@link PangrovaRestClient}.
     */
    static QueryClient wrap(PangrovaRestClient client) {
        return new RestClientAdapter(client);
    }

    /** Delegates the shell's query operations to its borrowed REST client. */
    final class RestClientAdapter implements QueryClient {
        private final PangrovaRestClient client;

        private RestClientAdapter(PangrovaRestClient client) {
            this.client = client;
        }

        @Override
        public SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties) {
            return client.submitQuery(sql, sessionProperties);
        }

        @Override
        public QueryStatusResponse getQueryStatus(String queryId) {
            return client.getQueryStatus(queryId);
        }

        @Override
        public PangrovaRestClient.CancelOutcome cancelQuery(String queryId) {
            return client.cancelQuery(queryId);
        }

        @Override
        public void fetchResultStream(ResultEndpoint endpoint, ResultBatchConsumer consumer) {
            client.fetchResultStream(endpoint, consumer);
        }

        @Override
        public byte[] fetchResult(ResultEndpoint endpoint) {
            return client.fetchResult(endpoint);
        }

        @Override
        public byte[] fetchResult(ResultEndpoint endpoint, long deadlineEpochMs) {
            return client.fetchResult(endpoint, deadlineEpochMs);
        }
    }
}
