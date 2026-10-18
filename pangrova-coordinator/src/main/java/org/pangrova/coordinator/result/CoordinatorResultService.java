package org.pangrova.coordinator.result;

import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.spi.connector.data.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Facade for small coordinator control-plane statement responses.
 *
 * <p>This service is deliberately not part of relational query execution.
 * SELECT, constant, LIMIT 0, cache and system-table query results must be
 * produced by a Worker/Native task and served from its direct endpoint.</p>
 */
public final class CoordinatorResultService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultService.class);

    /**
     * Lightweight result descriptor produced by bounded control-plane dispatch.
     * Mirrors DispatchResult semantics without depending on the query.execution.dispatch package.
     */
    public record ControlPlaneDispatchResult(
            String workerHost, String taskId, ResultEndpoint resultEndpoint) {}

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
     * Serializes a bounded DDL/SHOW/DML acknowledgement response and transitions
     * the control-plane statement to FINISHED.
     */
    public ControlPlaneDispatchResult dispatchControlPlaneRows(
            String queryId, List<TypedRow> rows, List<String> columnNames,
            Runnable finisher) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(rows, "rows");
        Objects.requireNonNull(columnNames, "columnNames");
        Objects.requireNonNull(finisher, "finisher");
        byte[] resultBytes = writer.serializeTypedRows(rows, columnNames);
        buffer.put(queryId, resultBytes);
        finisher.run();
        LOG.debug("event=control_plane_result_complete queryId={} bytes={}",
                queryId, resultBytes.length);
        return buildResult(queryId);
    }

    private ControlPlaneDispatchResult buildResult(String queryId) {
        ResultEndpoint endpoint = new ResultEndpoint(
                transportEndpoint.host(),
                transportEndpoint.port(),
                queryId,
                transportEndpoint.protocol());
        return new ControlPlaneDispatchResult("coordinator-control", queryId, endpoint);
    }

    /** Retires acknowledgement timers only after accepted query/publication owners drain. */
    public void stopExpirationScheduling() {
        buffer.stopExpirationScheduling();
    }

    /** Releases retained acknowledgement bytes after the serving transport exits. */
    @Override
    public void close() {
        buffer.close();
    }
}
