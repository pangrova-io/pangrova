package org.pangrova.coordinator.query.execution.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.planner.SqlCompiler;

class StatementDispatcherRelationalIsolationTest {

    @Test
    void connectorSystemTableSelectIsNotInterceptedByControlPlane() {
        var dispatcher = new StatementDispatcher(new StatementDispatcherContext(
                new SqlCompiler(), null, List.of(), List.of(), null));
        var session = new CoordinatorSessionContext(
                "system-table-query", Map.of(), null, "tenant-a");

        DispatchResult result = dispatcher.tryDispatch(
                new QueryStateMachine(),
                "SELECT * FROM lake.analytics.orders$snapshots",
                session.queryId(), session);

        assertThat(result).isNull();
    }
}
