package org.pangrova.coordinator.metadata.read;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure authenticated-session selector contract; real SDK interpretation is verified separately. */
class PlanningSnapshotSelectorsTest {
    @Test
    void selectorsAreCaseNormalizedAndPreserveAdmittedIdentity() {
        var session = SessionContextConverter.toConnectorSession(new QuerySession("query",
                Map.of("catalog", "Cat", "schema", "Db", "user", "reader"), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
        var selected = PlanningSnapshotSelectors.bind(session, Map.of("Cat.Db.Table", 7L));
        assertThat(PlanningSnapshotSelectors.snapshot(selected, "cat", "db", "table")).hasValue(7);
        assertThat(selected.getProperties()).containsEntry("queryId", "query").containsEntry("tenant_id", "tenant");
        assertThat(selected.getUser()).isEqualTo("reader");
        assertThat(session.getProperties()).doesNotContainKey("pangrova.source_snapshot.cat.db.table");
    }

    @Test
    void contradictoryCaseAliasesAndNonpositiveVersionsAreRejected() {
        var session = SessionContextConverter.toConnectorSession(new QuerySession("query", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
        assertThatThrownBy(() -> PlanningSnapshotSelectors.bind(session, Map.of("cat.db.table", 0L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PlanningSnapshotSelectors.bind(session,
                Map.of("cat.db.table", 1L, "CAT.DB.TABLE", 2L)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Conflicting");
    }

    @Test
    void literalIdentifierDotsDoNotCollideWithQualification() {
        var session = SessionContextConverter.toConnectorSession(new QuerySession("query", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
        var selected = PlanningSnapshotSelectors.bind(session, Map.of(
                PlanningSnapshotSelectors.tableKey("cat", "a.b", "rows"), 2L,
                PlanningSnapshotSelectors.tableKey("cat", "a", "b.rows"), 3L));
        assertThat(PlanningSnapshotSelectors.snapshot(selected, "cat", "a.b", "rows")).hasValue(2);
        assertThat(PlanningSnapshotSelectors.snapshot(selected, "cat", "a", "b.rows")).hasValue(3);
    }

    @Test
    void replacingSelectorsDoesNotCarryAnEarlierSelectedSnapshotForward() {
        var session = SessionContextConverter.toConnectorSession(new QuerySession("query", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
        var selected = PlanningSnapshotSelectors.bind(session, Map.of("cat.db.first", 7L));
        var replaced = PlanningSnapshotSelectors.bind(selected, Map.of("cat.db.second", 9L));
        assertThat(PlanningSnapshotSelectors.snapshot(replaced, "cat", "db", "first")).isEmpty();
        assertThat(PlanningSnapshotSelectors.snapshot(replaced, "cat", "db", "second")).hasValue(9);
    }

    @Test
    void trustedKernelSnapshotsBindWhileClientSelectorsAreStripped() {
        var context = new QuerySession("query", Map.of(
                "catalog", "cat",
                "pangrova.source_snapshot.cat.db.forged", "99"), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())
                .withSourceSnapshots(Map.of(
                        PlanningSnapshotSelectors.tableKey("cat", "db", "orders"), 7L));

        var session = SessionContextConverter.toConnectorSession(context);

        assertThat(PlanningSnapshotSelectors.snapshot(session, "cat", "db", "orders")).hasValue(7);
        assertThat(session.getProperties())
                .doesNotContainKey("pangrova.source_snapshot.cat.db.forged");
    }
}
