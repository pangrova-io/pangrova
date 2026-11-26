package org.pangrova.coordinator.result;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;

import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.types.value.TypedRow;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the control-plane-only result service.
 */
class CoordinatorResultServiceTest {

    private static final String QUERY_ID = "svc-q1";
    private static final ResultEndpoint TRANSPORT_ENDPOINT =
            new ResultEndpoint("localhost", 9091, "unused", "grpc");

    private ScheduledExecutorService executor;
    private CoordinatorResultBuffer buffer;
    private CoordinatorResultWriter writer;
    private CoordinatorResultService service;
    private RootAllocator allocator;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        buffer = new CoordinatorResultBuffer(executor, 60_000L);
        allocator = new RootAllocator();
        writer = new CoordinatorResultWriter(allocator);
        service = new CoordinatorResultService(buffer, writer, TRANSPORT_ENDPOINT);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        service.close();
        buffer.close();
        executor.shutdownNow();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        allocator.close();
    }

    @Test
    void rejectedBufferAdmissionDoesNotPublishFinished() {
        buffer.close();
        buffer = new CoordinatorResultBuffer(executor, 60_000L, 1, 1);
        service = new CoordinatorResultService(buffer, writer, TRANSPORT_ENDPOINT);
        AtomicBoolean finished = new AtomicBoolean();

        assertThatThrownBy(() -> service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), () -> finished.set(true)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(finished).isFalse();
        assertThat(buffer.get(QUERY_ID)).isNull();
        assertThat(buffer.bufferedBytes()).isZero();
    }

    @Test
    void dispatchControlPlaneRowsReturnsCoordinatorEndpoint() {
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("coordinator-control");
        assertThat(result.taskId()).isEqualTo(QUERY_ID);
        assertThat(result.resultEndpoint().host()).isEqualTo("localhost");
        assertThat(result.resultEndpoint().port()).isEqualTo(9091);
        assertThat(result.resultEndpoint().ticket()).isEqualTo(QUERY_ID);
        assertThat(buffer.get(QUERY_ID)).isNotEmpty();
    }

    @Test
    void dispatchControlPlaneRowsTransitionsStateMachineToFinished() {
        QueryStateMachine sm = new QueryStateMachine();

        service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), sm.toFinisher());

        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void dispatchControlPlaneRowsConvertsAndBuffers() {
        List<TypedRow> rows = List.of(
                TypedRow.builder().putInt("result", 42).build());
        List<String> columnNames = List.of("result");
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                QUERY_ID, rows, columnNames, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("coordinator-control");
        assertThat(buffer.get(QUERY_ID)).isNotNull();
        assertThat(buffer.get(QUERY_ID).length).isGreaterThan(0);
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
    }

    @Test
    void dispatchControlPlaneRowsHandlesMultipleColumns() {
        TypedRow row = TypedRow.builder()
                .putInt("id", 1)
                .putString("name", "hello")
                .build();
        List<String> columnNames = List.of("id", "name");
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                "multi-col-q", List.of(row), columnNames, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(buffer.get("multi-col-q")).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
    }

    @Test
    void dispatchControlPlaneRowsHandlesEmptyRows() {
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                "empty-q", List.of(), List.of(), sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(buffer.get("empty-q")).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
    }

    @Test
    void dispatcherCompletesSchemaLessStatementWithoutResultStream() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        sm.markNoResult();
        StatementDispatcher dispatcher = dispatcher((stmt, ctx) -> new StatementResult(
                List.of(), List.of(), "catalog.schema.table", StatementCategory.DML));

        DispatchResult result = dispatcher.executeDirect(
                new DropTableStatement(Identifier.of("catalog", "schema", "table"), false),
                sm, QUERY_ID, session());

        assertThat(result).isNull();
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
        assertThat(sm.status().disposition()).isEqualTo(ResultDisposition.NONE);
        assertThat(sm.status().endpoint()).isNull();
        assertThat(buffer.get(QUERY_ID)).isNull();
    }

    @Test
    void dispatcherPublishesEmptyResultSetWithSchemaAsStream() {
        QueryStateMachine sm = new QueryStateMachine();
        StatementDispatcher dispatcher = dispatcher((stmt, ctx) -> new StatementResult(
                List.of(), List.of("result"), null, StatementCategory.DDL));

        DispatchResult result = dispatcher.executeDirect(
                new DropTableStatement(Identifier.of("catalog", "schema", "table"), false),
                sm, QUERY_ID, session());

        assertThat(result).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
        assertThat(sm.status().disposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(sm.status().endpoint()).isEqualTo(result.resultEndpoint());
        assertThat(buffer.get(QUERY_ID)).isNotEmpty();
    }

    @Test
    void dispatcherPublishesControlPlaneRowsAfterNativeWriteDispatch() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        sm.deferTerminalUntilControlPlaneResult();
        sm.markWriteDispatchReady();
        StatementDispatcher dispatcher = dispatcher((stmt, ctx) -> new StatementResult(
                List.of(TypedRow.builder().putString("mv_name", "mv").build()),
                List.of("mv_name"), null, StatementCategory.DDL));

        DispatchResult result = dispatcher.executeDirect(
                new DropTableStatement(Identifier.of("catalog", "schema", "table"), false),
                sm, QUERY_ID, session());

        assertThat(result).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
        assertThat(sm.status().disposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(sm.status().endpoint()).isEqualTo(result.resultEndpoint());
    }

    private StatementDispatcher dispatcher(MetadataTask<Statement> task) {
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(DropTableStatement.class, task);
        var factory = new MetadataExecutionFactory(tasks);
        return new StatementDispatcher(new StatementDispatcherContext(
                service, List.of(factory), List.of(), null));
    }

    private static QuerySession session() {
        return new QuerySession(QUERY_ID, Map.of(), null, null, java.util.Optional.empty(),
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    @Test
    void resultEndpointUsesQueryIdAsTicket() {
        String queryId = "ticket-check-q";
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                queryId, List.of(), List.of(), sm.toFinisher());

        assertThat(result.resultEndpoint().ticket()).isEqualTo(queryId);
    }

    @Test
    void resultEndpointPreservesProtocol() {
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), sm.toFinisher());

        assertThat(result.resultEndpoint().protocol()).isEqualTo("grpc");
    }

    @Test
    void dispatchControlPlaneTransitionsThroughAllStatesToFinished() {
        QueryStateMachine sm = new QueryStateMachine();

        DispatchResult result = service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void dispatchControlPlaneThrowsWhenQueryAlreadyFailed() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.FAILED);

        assertThatThrownBy(() -> service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), sm.toFinisher()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Cannot transition query to FINISHED")
                .hasMessageContaining("FAILED");
    }

    @Test
    void dispatchControlPlaneThrowsWhenQueryAlreadyCancelled() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.CANCELLED);

        assertThatThrownBy(() -> service.dispatchControlPlaneRows(
                QUERY_ID, List.of(), List.of(), sm.toFinisher()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Cannot transition query to FINISHED")
                .hasMessageContaining("CANCELLED");
    }
}
