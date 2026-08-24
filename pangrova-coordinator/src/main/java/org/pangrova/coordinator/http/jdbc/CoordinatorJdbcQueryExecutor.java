package org.pangrova.coordinator.http.jdbc;

import org.apache.calcite.avatica.ColumnMetaData;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executes SQL queries by delegating to the coordinator's {@link QueryManager}.
 * Converts Arrow-format results into Avatica-compatible row/column structures.
 */
public final class CoordinatorJdbcQueryExecutor implements JdbcQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorJdbcQueryExecutor.class);
    private static final long POLL_INTERVAL_MS = 100;
    private static final long MAX_POLL_INTERVAL_MS = 2000;

    private final QueryManager queryManager;
    private final CoordinatorResultBuffer resultBuffer;

    public CoordinatorJdbcQueryExecutor(QueryManager queryManager,
                                        CoordinatorResultBuffer resultBuffer) {
        this.queryManager = queryManager;
        this.resultBuffer = resultBuffer;
    }

    @Override
    public JdbcQueryResult execute(String sql, String catalog) {
        LOG.debug("event=jdbc_execute sql={} catalog={}", sql, catalog);

        String queryId = "jdbc-" + UUID.randomUUID();
        Map<String, String> sessionProps = catalog != null ? Map.of("catalog", catalog) : Map.of();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                queryId, sessionProps, null, null);

        SubmitQueryResponse submitResponse = queryManager.submitQuery(queryId, sql, session);
        awaitCompletion(queryId, submitResponse);

        byte[] arrowData = resultBuffer.get(queryId);
        if (arrowData == null) {
            return new JdbcQueryResult(List.of(), List.of());
        }

        return convertArrowToResult(arrowData);
    }

    private void awaitCompletion(String queryId, SubmitQueryResponse submitResponse) {
        if ("FINISHED".equals(submitResponse.state())) {
            return;
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "jdbc-query-poll-" + queryId);
            thread.setDaemon(true);
            return thread;
        });
        try {
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<JdbcExecutionException> failure = new AtomicReference<>();
            pollQueryState(scheduler, queryId, done, failure, POLL_INTERVAL_MS, true);
            done.await();
            JdbcExecutionException ex = failure.get();
            if (ex != null) {
                throw ex;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JdbcExecutionException("Interrupted while waiting for query completion", e);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private void pollQueryState(ScheduledExecutorService scheduler, String queryId,
                                CountDownLatch done, AtomicReference<JdbcExecutionException> failure,
                                long pollMs, boolean immediate) {
        Runnable poll = () -> {
            try {
                if (isQueryTerminal(queryId)) {
                    done.countDown();
                    return;
                }
                pollQueryState(scheduler, queryId, done, failure,
                        Math.min(pollMs * 2, MAX_POLL_INTERVAL_MS), false);
            } catch (JdbcExecutionException e) {
                failure.set(e);
                done.countDown();
            }
        };
        if (immediate) {
            poll.run();
        } else {
            scheduler.schedule(poll, pollMs, TimeUnit.MILLISECONDS);
        }
    }

    private boolean isQueryTerminal(String queryId) {
        Optional<QueryStateMachine.State> stateOpt = queryManager.getQueryState(queryId);
        if (stateOpt.isEmpty()) {
            throw new JdbcExecutionException("Query not found: queryId=" + queryId);
        }

        QueryStateMachine.State state = stateOpt.get();
        if (state == QueryStateMachine.State.FINISHED) {
            return true;
        }
        if (state == QueryStateMachine.State.FAILED) {
            Optional<ErrorDetail> error = queryManager.getQueryError(queryId);
            String errorMsg = error.map(e -> e.errorCode() + ": " + e.message())
                    .orElse("Unknown error");
            throw new JdbcExecutionException("Query failed: " + errorMsg + ", queryId=" + queryId);
        }
        if (state == QueryStateMachine.State.CANCELLED) {
            throw new JdbcExecutionException("Query was cancelled, queryId=" + queryId);
        }
        return false;
    }

    private JdbcQueryResult convertArrowToResult(byte[] arrowData) {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(
                     new ByteArrayInputStream(arrowData), allocator)) {

            List<ColumnMetaData> columns = null;
            List<List<Object>> allRows = new ArrayList<>();

            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                if (columns == null) {
                    columns = buildColumnMetadata(root.getSchema());
                }
                int rowCount = root.getRowCount();
                List<FieldVector> vectors = root.getFieldVectors();
                for (int row = 0; row < rowCount; row++) {
                    List<Object> rowData = new ArrayList<>(vectors.size());
                    for (FieldVector vector : vectors) {
                        rowData.add(extractValue(vector, row));
                    }
                    allRows.add(rowData);
                }
            }

            if (columns == null) {
                columns = buildColumnMetadata(reader.getVectorSchemaRoot().getSchema());
            }

            return new JdbcQueryResult(columns, allRows);
        } catch (IOException e) {
            throw new JdbcExecutionException("Failed to read Arrow result stream: " + e.getMessage(), e);
        }
    }

    private static List<ColumnMetaData> buildColumnMetadata(Schema schema) {
        List<ColumnMetaData> columns = new ArrayList<>();
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int sqlType = arrowTypeToSqlType(field);
            String typeName = field.getType().toString();
            ColumnMetaData col = new ColumnMetaData(
                    i,
                    false,
                    true,
                    false,
                    false,
                    field.isNullable() ? 1 : 0,
                    true,
                    -1,
                    field.getName(),
                    field.getName(),
                    null,
                    0,
                    0,
                    null,
                    null,
                    ColumnMetaData.scalar(sqlType, typeName, ColumnMetaData.Rep.OBJECT),
                    true,
                    false,
                    false,
                    ColumnMetaData.scalar(sqlType, typeName, ColumnMetaData.Rep.OBJECT).columnClassName());
            columns.add(col);
        }
        return columns;
    }

    private static int arrowTypeToSqlType(Field field) {
        return switch (field.getType().getTypeID()) {
            case Int -> Types.INTEGER;
            case FloatingPoint -> Types.DOUBLE;
            case Utf8, LargeUtf8 -> Types.VARCHAR;
            case Bool -> Types.BOOLEAN;
            case Date -> Types.DATE;
            case Time -> Types.TIME;
            case Timestamp -> Types.TIMESTAMP;
            case Decimal -> Types.DECIMAL;
            case Binary, LargeBinary -> Types.BINARY;
            default -> Types.OTHER;
        };
    }

    private static Object extractValue(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof IntVector iv) return iv.get(row);
        if (vector instanceof BigIntVector bv) return bv.get(row);
        if (vector instanceof Float4Vector fv) return fv.get(row);
        if (vector instanceof Float8Vector dv) return dv.get(row);
        if (vector instanceof VarCharVector sv) return new String(sv.get(row), StandardCharsets.UTF_8);
        if (vector instanceof BitVector bv) return bv.get(row) != 0;
        return vector.getObject(row);
    }
}
