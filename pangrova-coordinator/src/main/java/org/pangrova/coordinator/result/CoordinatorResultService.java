package org.pangrova.coordinator.result;

import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.NativePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Facade for coordinator-local result dispatch.
 *
 * <p>Supports three result paths (all columnar, no row-based fallback):</p>
 * <ul>
 *   <li>Pre-serialized columnar IPC bytes → buffer</li>
 *   <li>DDL/DML TypedRow: columnar fill → IPC → buffer</li>
 *   <li>NativePage passthrough → buffer (deferred IPC access)</li>
 * </ul>
 *
 * <p>Encapsulates buffer + writer + transport endpoint, eliminating
 * duplicated state-transition + buffer-write + DispatchResult construction.</p>
 */
public final class CoordinatorResultService {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultService.class);

    /**
     * Callback to transition query state to FINISHED.
     * Breaks the cyclic dependency between result and query packages.
     */
    @FunctionalInterface
    public interface QueryFinisher {
        void transitionToFinished();
    }

    /**
     * Lightweight result descriptor produced by local dispatch.
     * Mirrors DispatchResult semantics without depending on the query.execution.dispatch package.
     */
    public record LocalDispatchResult(String workerHost, String taskId, ResultEndpoint resultEndpoint) {}

    private final CoordinatorResultBuffer buffer;
    private final CoordinatorResultWriter writer;
    private final ResultEndpoint transportEndpoint;

    public CoordinatorResultService(CoordinatorResultBuffer buffer,
                                    CoordinatorResultWriter writer,
                                    ResultEndpoint transportEndpoint) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.transportEndpoint = Objects.requireNonNull(transportEndpoint, "transportEndpoint");
    }

    /**
     * Buffers pre-serialized columnar IPC bytes and transitions the query to FINISHED.
     *
     * @return LocalDispatchResult pointing to the coordinator-local transport endpoint
     */
    public LocalDispatchResult dispatchLocal(String queryId, byte[] resultBytes, QueryFinisher finisher) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(resultBytes, "resultBytes");
        Objects.requireNonNull(finisher, "finisher");
        buffer.put(queryId, resultBytes);
        finisher.transitionToFinished();
        LOG.debug("event=local_dispatch_complete queryId={} bytes={}", queryId, resultBytes.length);
        return buildResult(queryId);
    }

    /**
     * Buffers an ArrowPage directly, deferring IPC serialization to retrieval time.
     */
    public LocalDispatchResult dispatchLocalFromNativePage(String queryId, NativePage nativePage,
                                                           QueryFinisher finisher) {
        Objects.requireNonNull(nativePage, "nativePage");
        buffer.putArrowPage(queryId, nativePage);
        finisher.transitionToFinished();
        LOG.debug("event=local_arrow_page_dispatch queryId={} rows={}", queryId, nativePage.getRowCount());
        return buildResult(queryId);
    }


    /**
     * Converts TypedRow list to columnar IPC via columnar fill, buffers, and dispatches.
     */
    public LocalDispatchResult dispatchLocalFromTypedRows(String queryId,
                                                          List<TypedRow> rows,
                                                          List<String> columnNames,
                                                          QueryFinisher finisher) {
        return dispatchLocal(queryId, writer.serializeTypedRows(rows, columnNames), finisher);
    }

    /**
     * Retrieves buffered result bytes for a query, or null if not available.
     */
    public byte[] getResultBytes(String queryId) {
        return buffer.get(queryId);
    }

    private LocalDispatchResult buildResult(String queryId) {
        ResultEndpoint endpoint = new ResultEndpoint(
                transportEndpoint.host(),
                transportEndpoint.port(),
                queryId,
                transportEndpoint.protocol());
        return new LocalDispatchResult("coordinator-local", queryId, endpoint);
    }
}
