package org.pangrova.coordinator.task;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.QueryStateMachine;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Connector facades retain exact issued scopes rather than resolving a later query-ID incarnation. */
class ConnectorIssuedScopeTest {
    @Test
    void connectorOperationExitNotifiesTerminalCleanupAfterTheLastLease() {
        var tracker = new CoordinatorTaskTracker();
        var exits = new java.util.ArrayList<String>();
        tracker.addQueryOperationExitListener(exits::add);
        var authority = tracker.openQueryIssuance("connector-query");
        var first = authority.enterOperation();
        var second = authority.enterOperation();
        tracker.sealQueryIssuance("connector-query");
        first.close();
        assertThat(exits).isEmpty();
        second.close();
        assertThat(exits).containsExactly("connector-query");
        tracker.requireQueryExecutionDrained("connector-query");
        tracker.retireQueryIssuance("connector-query");
    }

    @Test
    void connectorCleanupClaimsTheSameIssuedTaskDrainEvidence() {
        var registry = new TaskIdentityRegistry();
        var state = new QueryStateMachine();
        var authority = state.bindConnectorAuthority(registry.openQuery("issued-task"));
        var request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task", "issued-task", 0, 0, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, new byte[]{1});
        registry.registerRequest("task", request);
        registry.bindTarget(request, org.pangrova.coordinator.worker.TestWorkerInfoBuilder.active(
                "worker", "original-host", 8001, 8002));
        registry.enterDispatch(request);
        state.tryTransition(QueryStateMachine.State.CANCELLED);
        registry.sealQuery(authority.queryId());
        assertThatThrownBy(authority::requireDrained).hasMessageContaining("lacks its exact");
        // This is retained canonical receipt validation, not a claim that a Native executor ran.
        registry.recordExecutionDrain(request, org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setAttempt(request.taskRequest().getAttempt())
                .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setExecutionDrainState(
                        org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
                .build());
        authority.requireDrained();
        registry.retireQuery(authority.queryId());
        authority.requireDrained();
        assertThatThrownBy(authority::enterOperation).hasMessageContaining("admission is closed");
    }

    @Test
    void oldFacadeCannotReviveAfterRetiredScopePurgeAndTextualIdReuse() {
        var clock = new AtomicLong(1);
        var registry = new TaskIdentityRegistry(clock::get);
        var oldState = new QueryStateMachine();
        var old = oldState.bindConnectorAuthority(registry.openQuery("same-text-id"));
        oldState.tryTransition(QueryStateMachine.State.CANCELLED);
        registry.sealQuery("same-text-id");
        old.requireDrained();
        registry.retireQuery("same-text-id");
        clock.incrementAndGet();
        var fresh = new QueryStateMachine().bindConnectorAuthority(registry.openQuery("same-text-id"));
        assertThat(fresh).isNotSameAs(old);
        assertThat(fresh.isClosed()).isFalse();
        assertThat(old.isClosed()).isTrue();
        old.requireDrained();
        assertThatThrownBy(old::enterOperation).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> old.beginMetadata("unused", () -> {
            throw new AssertionError("A retired facade must not perform metadata lookup");
        })).isInstanceOf(IllegalStateException.class);
        try (var actualOperation = fresh.enterOperation()) {
            assertThatThrownBy(fresh::requireDrained).isInstanceOf(IllegalStateException.class);
        }
        registry.sealQuery("same-text-id");
        fresh.requireDrained();
        registry.retireQuery("same-text-id");
    }
}
