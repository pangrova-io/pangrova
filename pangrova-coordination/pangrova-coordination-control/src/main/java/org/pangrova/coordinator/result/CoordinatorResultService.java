package org.pangrova.coordinator.result;

import org.pangrova.execution.task.DispatchResult;

import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.types.value.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.pangrova.query.api.lifecycle.QueryOperation;
import org.pangrova.common.lifecycle.ResourceCloser;

/**
 * Facade for small coordinator control-plane statement responses.
 *
 * <p>This service is deliberately not part of relational query execution.
 * SELECT, constant, LIMIT 0, cache and system-table query results must be
 * produced by a Worker/Native task and served from its direct endpoint.</p>
 */
public final class CoordinatorResultService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultService.class);

    private final CoordinatorResultBuffer buffer;
    private final CoordinatorResultWriter writer;
    private final ResultEndpoint transportEndpoint;
    private final Function<String, QueryOperation> retainOperation;

    /** Borrows the transport, batch allocator, and query-operation admission. */
    public CoordinatorResultService(CoordinatorResultBuffer buffer, CoordinatorResultWriter writer,
            ResultEndpoint transportEndpoint, Function<String, QueryOperation> retainOperation) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.transportEndpoint = Objects.requireNonNull(transportEndpoint, "transportEndpoint");
        this.retainOperation = Objects.requireNonNull(retainOperation, "retainOperation");
    }

    /**
     * Publishes a bounded columnar response and transfers its physical query operation to the buffer.
     */
    public DispatchResult dispatchControlPlaneRows(
            String queryId, List<TypedRow> rows, List<String> columnNames) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(rows, "rows");
        Objects.requireNonNull(columnNames, "columnNames");
        var operation = retainOperation.apply(queryId);
        CoordinatorResultBatch batch;
        try {
            batch = writer.buildBatch(rows, columnNames);
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, operation::close);
            throw failure;
        }
        buffer.publish(queryId, batch, operation);
        LOG.debug("event=control_plane_result_complete queryId={} bytes={}", queryId, batch.bytes());
        return buildResult(queryId);
    }

    private DispatchResult buildResult(String queryId) {
        ResultEndpoint endpoint = new ResultEndpoint(
                transportEndpoint.host(),
                transportEndpoint.port(),
                queryId,
                transportEndpoint.protocol());
        return new DispatchResult("coordinator-control", queryId, endpoint);
    }

    /** Releases a failed query's unclaimed result. */
    public void discardUnopened(String queryId) {
        buffer.discardUnopened(queryId);
    }

    /** Releases retained acknowledgement bytes after the serving transport exits. */
    @Override
    public void close() {
        buffer.close();
    }
}
