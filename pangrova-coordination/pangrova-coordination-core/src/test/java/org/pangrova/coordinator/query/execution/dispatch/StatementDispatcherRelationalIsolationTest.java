package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.query.core.task.DispatchResult;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.planner.core.analysis.SqlCompiler;

class StatementDispatcherRelationalIsolationTest {

    @Test
    void connectorSystemTableSelectIsNotInterceptedByControlPlane() {
        var dispatcher = new StatementDispatcher(new StatementDispatcherContext(
                new SqlCompiler(), null, List.of(), List.of(), null));
        var session = new QuerySession(
                "system-table-query", Map.of(), null, "tenant-a", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        DispatchResult result = dispatcher.tryDispatch(
                new QueryStateMachine(),
                "SELECT * FROM lake.analytics.orders$snapshots",
                session.queryId(), session);

        assertThat(result).isNull();
    }
}
