package org.pangrova.coordinator.result;

import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.result.CoordinatorResultService.LocalDispatchResult;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.TypedRow;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CoordinatorResultService} — the unified local-dispatch facade.
 */
class CoordinatorResultServiceTest {

    private static final String QUERY_ID = "svc-q1";
    private static final ResultEndpoint TRANSPORT_ENDPOINT =
            new ResultEndpoint("localhost", 9091, "unused", "grpc");

    private ScheduledExecutorService executor;
    private CoordinatorResultBuffer buffer;
    private CoordinatorResultWriter writer;
    private CoordinatorResultService service;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        buffer = new CoordinatorResultBuffer(executor, 60_000L);
        writer = new CoordinatorResultWriter(new RootAllocator());
        service = new CoordinatorResultService(buffer, writer, TRANSPORT_ENDPOINT);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void dispatchLocalBuffersBytesAndReturnsCoordinatorEndpoint() {
        byte[] resultBytes = {1, 2, 3, 4};
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocal(QUERY_ID, resultBytes, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("coordinator-local");
        assertThat(result.taskId()).isEqualTo(QUERY_ID);
        assertThat(result.resultEndpoint().host()).isEqualTo("localhost");
        assertThat(result.resultEndpoint().port()).isEqualTo(9091);
        assertThat(result.resultEndpoint().ticket()).isEqualTo(QUERY_ID);
        assertThat(buffer.get(QUERY_ID)).isEqualTo(resultBytes);
    }

    @Test
    void dispatchLocalTransitionsStateMachineToFinished() {
        QueryStateMachine sm = new QueryStateMachine();

        service.dispatchLocal(QUERY_ID, new byte[]{1}, sm.toFinisher());

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void dispatchLocalFromTypedRowsConvertsAndBuffers() {
        List<TypedRow> rows = List.of(
                TypedRow.builder().putInt("result", 42).build());
        List<String> columnNames = List.of("result");
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocalFromTypedRows(
                QUERY_ID, rows, columnNames, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("coordinator-local");
        assertThat(buffer.get(QUERY_ID)).isNotNull();
        assertThat(buffer.get(QUERY_ID).length).isGreaterThan(0);
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void dispatchLocalFromTypedRowsHandlesMultipleColumns() {
        TypedRow row = TypedRow.builder()
                .putInt("id", 1)
                .putString("name", "hello")
                .build();
        List<String> columnNames = List.of("id", "name");
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocalFromTypedRows(
                "multi-col-q", List.of(row), columnNames, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(buffer.get("multi-col-q")).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void dispatchLocalFromTypedRowsHandlesEmptyRows() {
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocalFromTypedRows(
                "empty-q", List.of(), List.of(), sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(buffer.get("empty-q")).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void resultEndpointUsesQueryIdAsTicket() {
        String queryId = "ticket-check-q";
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocal(queryId, new byte[]{1}, sm.toFinisher());

        assertThat(result.resultEndpoint().ticket()).isEqualTo(queryId);
    }

    @Test
    void resultEndpointPreservesProtocol() {
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocal(QUERY_ID, new byte[]{1}, sm.toFinisher());

        assertThat(result.resultEndpoint().protocol()).isEqualTo("grpc");
    }

    @Test
    void dispatchLocalFromNativePageBuffersAndReturnsEndpoint() {
        byte[] pageData = {10, 20, 30};
        NativePage nativePage = new NativePage(pageData, 3, 2, BatchFormat.ARROW_IPC);
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocalFromNativePage(QUERY_ID, nativePage, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("coordinator-local");
        assertThat(buffer.get(QUERY_ID)).isEqualTo(pageData);
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void dispatchLocalFromNativePageTransitionsStateMachine() {
        NativePage nativePage = new NativePage(new byte[]{1}, 1, 1, BatchFormat.ARROW_IPC);
        QueryStateMachine sm = new QueryStateMachine();

        service.dispatchLocalFromNativePage("ap-q", nativePage, sm.toFinisher());

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void dispatchLocalTransitionsThroughAllStatesToFinished() {
        QueryStateMachine sm = new QueryStateMachine();

        LocalDispatchResult result = service.dispatchLocal(QUERY_ID, new byte[]{1}, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void dispatchLocalThrowsWhenQueryAlreadyFailed() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.FAILED);

        assertThatThrownBy(() -> service.dispatchLocal(QUERY_ID, new byte[]{1}, sm.toFinisher()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Cannot transition query to FINISHED")
                .hasMessageContaining("FAILED");
    }

    @Test
    void dispatchLocalThrowsWhenQueryAlreadyCancelled() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.CANCELLED);

        assertThatThrownBy(() -> service.dispatchLocal(QUERY_ID, new byte[]{1}, sm.toFinisher()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Cannot transition query to FINISHED")
                .hasMessageContaining("CANCELLED");
    }
}
