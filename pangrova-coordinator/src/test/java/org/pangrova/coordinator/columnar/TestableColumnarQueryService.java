package org.pangrova.coordinator.columnar;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-only Flight SQL producer that bypasses QueryManager for protocol isolation.
 * Directly manages query state machines and result buffer, enabling tests to
 * control query lifecycle without the full execution pipeline.
 */
final class TestableColumnarQueryService implements FlightSqlProducer, AutoCloseable {

    private final CoordinatorResultBuffer resultBuffer;
    private final BufferAllocator allocator;
    private final ColumnarQueryServiceConfig config;
    private final ConcurrentHashMap<String, QueryStateMachine> stateMachines;
    private final ConcurrentHashMap<String, PreparedStatementHandle> preparedStatements =
            new ConcurrentHashMap<>();

    TestableColumnarQueryService(CoordinatorResultBuffer resultBuffer,
                                 BufferAllocator allocator,
                                 ColumnarQueryServiceConfig config,
                                 ConcurrentHashMap<String, QueryStateMachine> stateMachines) {
        this.resultBuffer = resultBuffer;
        this.allocator = allocator;
        this.config = config;
        this.stateMachines = stateMachines;
    }

    @Override
    public FlightInfo getFlightInfoStatement(
            FlightSql.CommandStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor) {
        String queryId = UUID.randomUUID().toString();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        stateMachines.put(queryId, sm);

        // Wrap queryId in a TicketStatementQuery protobuf so the Flight SQL
        // framework can decode it back in getStreamStatement.
        FlightSql.TicketStatementQuery ticketProto = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(queryId))
                .build();
        Ticket ticket = new Ticket(Any.pack(ticketProto).toByteArray());
        FlightEndpoint endpoint = new FlightEndpoint(ticket);
        Schema schema = new Schema(Collections.emptyList());
        return new FlightInfo(schema, descriptor, List.of(endpoint), -1, -1);
    }

    @Override
    public SchemaResult getSchemaStatement(
            FlightSql.CommandStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor) {
        return new SchemaResult(new Schema(Collections.emptyList()));
    }

    @Override
    public void getStreamStatement(
            FlightSql.TicketStatementQuery ticket,
            CallContext context,
            ServerStreamListener listener) {
        String queryId = ticket.getStatementHandle().toStringUtf8();
        streamResultsForQuery(queryId, listener);
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor) {
        String handleId = command.getPreparedStatementHandle().toStringUtf8();
        PreparedStatementHandle handle = preparedStatements.get(handleId);
        if (handle == null || handle.isExpired(config.preparedStatementTtlMs())) {
            preparedStatements.remove(handleId);
            throw CallStatus.NOT_FOUND
                    .withDescription("Prepared statement not found or expired: " + handleId)
                    .toRuntimeException();
        }

        String queryId = UUID.randomUUID().toString();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        stateMachines.put(queryId, sm);

        Ticket resultTicket = new Ticket(queryId.getBytes(StandardCharsets.UTF_8));
        FlightEndpoint endpoint = new FlightEndpoint(resultTicket);
        return new FlightInfo(new Schema(Collections.emptyList()),
                descriptor, List.of(endpoint), -1, -1);
    }

    @Override
    public SchemaResult getSchemaPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor) {
        return new SchemaResult(new Schema(Collections.emptyList()));
    }

    @Override
    public void getStreamPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            ServerStreamListener listener) {
        String handleId = command.getPreparedStatementHandle().toStringUtf8();
        PreparedStatementHandle handle = preparedStatements.get(handleId);
        if (handle == null || handle.isExpired(config.preparedStatementTtlMs())) {
            listener.error(CallStatus.NOT_FOUND
                    .withDescription("Prepared statement not found or expired")
                    .toRuntimeException());
            return;
        }
        String queryId = UUID.randomUUID().toString();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        stateMachines.put(queryId, sm);
        streamResultsForQuery(queryId, listener);
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return () -> {
            // Consume parameter binding stream per Flight SQL protocol contract
            while (flightStream.next()) {
                flightStream.getRoot();
            }
            ackStream.onCompleted();
        };
    }

    @Override
    public void createPreparedStatement(
            FlightSql.ActionCreatePreparedStatementRequest request,
            CallContext context,
            StreamListener<Result> listener) {
        String sql = request.getQuery();
        String handleId = UUID.randomUUID().toString();
        preparedStatements.put(handleId,
                new PreparedStatementHandle(handleId, sql, Instant.now()));

        FlightSql.ActionCreatePreparedStatementResult result =
                FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                        .setPreparedStatementHandle(ByteString.copyFromUtf8(handleId))
                        .build();
        listener.onNext(new Result(Any.pack(result).toByteArray()));
        listener.onCompleted();
    }

    @Override
    public void closePreparedStatement(
            FlightSql.ActionClosePreparedStatementRequest request,
            CallContext context,
            StreamListener<Result> listener) {
        preparedStatements.remove(request.getPreparedStatementHandle().toStringUtf8());
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "sql-info");
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "catalogs");
    }

    @Override
    public void getStreamCatalogs(CallContext ctx, ServerStreamListener l) { emptyStream(l); }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "schemas");
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "tables");
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "table-types");
    }

    @Override
    public void getStreamTableTypes(CallContext ctx, ServerStreamListener l) { emptyStream(l); }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "exported-keys");
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "imported-keys");
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "cross-ref");
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "primary-keys");
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo c, CallContext ctx, FlightDescriptor d) {
        return emptyFlightInfo(d, "type-info");
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo c, CallContext ctx, ServerStreamListener l) {
        emptyStream(l);
    }

    @Override
    public void listFlights(CallContext ctx, Criteria criteria, StreamListener<FlightInfo> l) {
        l.onCompleted();
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate c, CallContext ctx, FlightStream fs, StreamListener<PutResult> ack) {
        return () -> ack.onError(CallStatus.UNIMPLEMENTED
                .withDescription("DML via columnar protocol not supported; use REST endpoint")
                .toRuntimeException());
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate c, CallContext ctx, FlightStream fs, StreamListener<PutResult> ack) {
        return () -> ack.onError(CallStatus.UNIMPLEMENTED
                .withDescription("DML via columnar protocol not supported; use REST endpoint")
                .toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoSubstraitPlan(FlightSql.CommandStatementSubstraitPlan c, CallContext ctx, FlightDescriptor d) {
        throw CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException();
    }

    @Override
    public void createPreparedSubstraitPlan(FlightSql.ActionCreatePreparedSubstraitPlanRequest r, CallContext ctx, StreamListener<FlightSql.ActionCreatePreparedStatementResult> l) {
        l.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public void beginTransaction(FlightSql.ActionBeginTransactionRequest r, CallContext ctx, StreamListener<FlightSql.ActionBeginTransactionResult> l) {
        l.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public void endTransaction(FlightSql.ActionEndTransactionRequest r, CallContext ctx, StreamListener<Result> l) {
        l.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public void beginSavepoint(FlightSql.ActionBeginSavepointRequest r, CallContext ctx, StreamListener<FlightSql.ActionBeginSavepointResult> l) {
        l.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public void endSavepoint(FlightSql.ActionEndSavepointRequest r, CallContext ctx, StreamListener<Result> l) {
        l.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public void cancelFlightInfo(org.apache.arrow.flight.CancelFlightInfoRequest r, CallContext ctx, StreamListener<org.apache.arrow.flight.CancelStatus> l) {
        l.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(FlightSql.CommandStatementIngest c, CallContext ctx, org.apache.arrow.flight.FlightStream fs, StreamListener<PutResult> ack) {
        return () -> ack.onError(CallStatus.UNIMPLEMENTED.withDescription("Not supported").toRuntimeException());
    }

    @Override
    public void close() {
        preparedStatements.clear();
    }

    // ── Internal ──

    private void streamResultsForQuery(String queryId, ServerStreamListener listener) {
        QueryStateMachine sm = stateMachines.get(queryId);
        if (sm == null) {
            listener.error(CallStatus.NOT_FOUND
                    .withDescription("Query not found: " + queryId)
                    .toRuntimeException());
            return;
        }

        if (sm.getState() == QueryStateMachine.State.FAILED
                || sm.getState() == QueryStateMachine.State.CANCELLED) {
            listener.error(CallStatus.CANCELLED
                    .withDescription("Query failed or cancelled: " + queryId)
                    .toRuntimeException());
            return;
        }

        byte[] resultBytes = pollForResult(queryId);
        if (resultBytes == null) {
            listener.error(CallStatus.UNAVAILABLE
                    .withDescription("Query results not yet available: " + queryId)
                    .toRuntimeException());
            return;
        }

        try (var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new ByteArrayInputStream(resultBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            listener.start(root);
            while (reader.loadNextBatch()) {
                listener.putNext();
            }
            listener.completed();
        } catch (IOException e) {
            listener.error(CallStatus.UNKNOWN
                    .withDescription("Failed to stream results: " + e.getMessage())
                    .toRuntimeException());
        }
    }

    private byte[] pollForResult(String queryId) {
        byte[] result = resultBuffer.get(queryId);
        if (result != null) {
            return result;
        }
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            result = resultBuffer.get(queryId);
            if (result != null) {
                return result;
            }
            QueryStateMachine sm = stateMachines.get(queryId);
            if (sm != null && (sm.getState() == QueryStateMachine.State.FAILED
                    || sm.getState() == QueryStateMachine.State.CANCELLED)) {
                return null;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return resultBuffer.get(queryId);
    }

    private void emptyStream(ServerStreamListener listener) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(
                new Schema(Collections.emptyList()), allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    private FlightInfo emptyFlightInfo(FlightDescriptor descriptor, String ticketId) {
        Schema schema = new Schema(Collections.emptyList());
        Ticket ticket = new Ticket(ticketId.getBytes(StandardCharsets.UTF_8));
        return new FlightInfo(schema, descriptor, List.of(new FlightEndpoint(ticket)), -1, -1);
    }
}
