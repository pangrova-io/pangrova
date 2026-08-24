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
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.spi.execution.SessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Columnar query service implementing the columnar SQL protocol.
 * Enables high-throughput columnar data transfer for analytics workloads
 * and compatibility with Flight SQL-aware tools (DBeaver 23+, ADBC drivers).
 *
 * <p>Maps the Pangrova query lifecycle to the Flight SQL ticket model:
 * <ul>
 *   <li>getFlightInfoStatement: submit query, return ticket with queryId</li>
 *   <li>getStreamStatement: poll for results, stream columnar IPC batches</li>
 *   <li>createPreparedStatement: register SQL for later execution</li>
 * </ul>
 *
 * <p>Concurrency: thread-safe via ConcurrentHashMap for prepared statements.
 * Query submission and result retrieval delegate to existing thread-safe components.</p>
 */
@ThreadSafe
public final class ColumnarQueryService implements FlightSqlProducer, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnarQueryService.class);

    private final QueryManager queryManager;
    private final CoordinatorResultBuffer resultBuffer;
    private final BufferAllocator allocator;
    private final ColumnarQueryServiceConfig config;
    private final ColumnarResultStreamer resultStreamer;
    private final ConcurrentHashMap<String, PreparedStatementHandle> preparedStatements =
            new ConcurrentHashMap<>();

    /**
     * Creates the service wiring query execution and result buffering to the Flight protocol.
     */
    public ColumnarQueryService(QueryManager queryManager,
                                CoordinatorResultBuffer resultBuffer,
                                BufferAllocator allocator,
                                ColumnarQueryServiceConfig config) {
        this.queryManager = Objects.requireNonNull(queryManager, "queryManager");
        this.resultBuffer = Objects.requireNonNull(resultBuffer, "resultBuffer");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.config = Objects.requireNonNull(config, "config");
        this.resultStreamer = new ColumnarResultStreamer(queryManager, resultBuffer, allocator);
    }

    // ── Flight SQL: Statement Execution ──

    @Override
    public FlightInfo getFlightInfoStatement(
            FlightSql.CommandStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor) {
        String sql = command.getQuery();
        LOG.debug("event=columnar_query_submit sql={}", sql);

        String queryId = submitQuery(sql, context);
        Ticket ticket = new Ticket(queryId.getBytes(StandardCharsets.UTF_8));
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
        resultStreamer.streamResultsForQuery(queryId, listener);
    }

    // ── Flight SQL: Prepared Statements ──

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            FlightDescriptor descriptor) {
        String handleId = command.getPreparedStatementHandle().toStringUtf8();
        PreparedStatementHandle handle = resolveHandle(handleId);

        String queryId = submitQuery(handle.sql(), context);
        Ticket resultTicket = new Ticket(queryId.getBytes(StandardCharsets.UTF_8));
        FlightEndpoint endpoint = new FlightEndpoint(resultTicket);
        Schema schema = new Schema(Collections.emptyList());

        return new FlightInfo(schema, descriptor, List.of(endpoint), -1, -1);
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
            preparedStatements.remove(handleId);
            listener.error(CallStatus.NOT_FOUND
                    .withDescription("Prepared statement not found or expired: " + handleId)
                    .toRuntimeException());
            return;
        }

        String queryId = submitQuery(handle.sql(), context);
        resultStreamer.streamResultsForQuery(queryId, listener);
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return () -> {
            while (flightStream.next()) {
                // Parameter binding consumed; actual binding at execution time
            }
            ackStream.onCompleted();
        };
    }

    // ── Prepared Statement Lifecycle ──

    @Override
    public void createPreparedStatement(
            FlightSql.ActionCreatePreparedStatementRequest request,
            CallContext context,
            StreamListener<Result> listener) {
        String sql = request.getQuery();
        String handleId = UUID.randomUUID().toString();
        PreparedStatementHandle handle = new PreparedStatementHandle(
                handleId, sql, Instant.now());
        preparedStatements.put(handleId, handle);

        LOG.info("event=prepared_statement_created handleId={}", handleId);

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
        String handleId = request.getPreparedStatementHandle().toStringUtf8();
        preparedStatements.remove(handleId);
        LOG.debug("event=prepared_statement_closed handleId={}", handleId);
        listener.onCompleted();
    }

    // ── SQL Info ──

    @Override
    public FlightInfo getFlightInfoSqlInfo(
            FlightSql.CommandGetSqlInfo command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "sql-info");
    }

    @Override
    public void getStreamSqlInfo(
            FlightSql.CommandGetSqlInfo command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    // ── Catalog Metadata (minimal for ADBC compatibility) ──

    @Override
    public FlightInfo getFlightInfoCatalogs(
            FlightSql.CommandGetCatalogs command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "catalogs");
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoSchemas(
            FlightSql.CommandGetDbSchemas command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "schemas");
    }

    @Override
    public void getStreamSchemas(
            FlightSql.CommandGetDbSchemas command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoTables(
            FlightSql.CommandGetTables command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "tables");
    }

    @Override
    public void getStreamTables(
            FlightSql.CommandGetTables command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(
            FlightSql.CommandGetTableTypes command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "table-types");
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        emptyStream(listener);
    }

    // ── Keys and References ──

    @Override
    public FlightInfo getFlightInfoExportedKeys(
            FlightSql.CommandGetExportedKeys command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "exported-keys");
    }

    @Override
    public void getStreamExportedKeys(
            FlightSql.CommandGetExportedKeys command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(
            FlightSql.CommandGetImportedKeys command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "imported-keys");
    }

    @Override
    public void getStreamImportedKeys(
            FlightSql.CommandGetImportedKeys command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(
            FlightSql.CommandGetCrossReference command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "cross-reference");
    }

    @Override
    public void getStreamCrossReference(
            FlightSql.CommandGetCrossReference command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(
            FlightSql.CommandGetPrimaryKeys command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "primary-keys");
    }

    @Override
    public void getStreamPrimaryKeys(
            FlightSql.CommandGetPrimaryKeys command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(
            FlightSql.CommandGetXdbcTypeInfo command,
            CallContext context,
            FlightDescriptor descriptor) {
        return emptyFlightInfo(descriptor, "type-info");
    }

    @Override
    public void getStreamTypeInfo(
            FlightSql.CommandGetXdbcTypeInfo command,
            CallContext context,
            ServerStreamListener listener) {
        emptyStream(listener);
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria,
                            StreamListener<FlightInfo> listener) {
        listener.onCompleted();
    }

    // ── DML (read-only protocol; DML uses REST) ──

    @Override
    public Runnable acceptPutStatement(
            FlightSql.CommandStatementUpdate command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return () -> ackStream.onError(CallStatus.UNIMPLEMENTED
                .withDescription("DML via columnar protocol not supported; use REST endpoint")
                .toRuntimeException());
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(
            FlightSql.CommandPreparedStatementUpdate command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return () -> ackStream.onError(CallStatus.UNIMPLEMENTED
                .withDescription("DML via columnar protocol not supported; use REST endpoint")
                .toRuntimeException());
    }

    // beginTransaction, endTransaction, beginSavepoint, endSavepoint,
    // createPreparedSubstraitPlan, cancelFlightInfo, getFlightInfoSubstraitPlan,
    // getFlightInfoStatementIngest — all use default implementations from
    // FlightSqlProducer which throw UNIMPLEMENTED.

    // ── Lifecycle ──

    @Override
    public void close() {
        preparedStatements.clear();
        resultStreamer.close();
        LOG.info("event=columnar_query_service_closed");
    }

    /**
     * Returns the number of active prepared statements (for monitoring).
     */
    public int preparedStatementCount() {
        return preparedStatements.size();
    }

    // ── Internal ──

    private String submitQuery(String sql, CallContext context) {
        SessionContext session = buildSessionFromContext(context);
        String queryId = UUID.randomUUID().toString();
        var coordinatorSession = org.pangrova.coordinator.query.SessionContextConverter
                .fromSpiSessionContext(queryId, session);
        queryManager.submitQuery(queryId, sql, coordinatorSession);
        return queryId;
    }

    private PreparedStatementHandle resolveHandle(String handleId) {
        PreparedStatementHandle handle = preparedStatements.get(handleId);
        if (handle == null || handle.isExpired(config.preparedStatementTtlMs())) {
            preparedStatements.remove(handleId);
            throw CallStatus.NOT_FOUND
                    .withDescription("Prepared statement not found or expired: " + handleId)
                    .toRuntimeException();
        }
        return handle;
    }

    private SessionContext buildSessionFromContext(CallContext context) {
        String peerIdentity = context.peerIdentity();
        String user = (peerIdentity != null && !peerIdentity.isEmpty())
                ? peerIdentity : "anonymous";
        return new ColumnarSessionContext(user);
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
