package org.pangrova.coordinator.dynamictable;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.statement.StatementTestSessions;
import org.pangrova.metadata.dynamictable.DynamicTablePublication;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.CreateDynamicTableStatement;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamicTableMaterializationServiceTest {

    @Test
    void positiveTargetLagIsRejectedUntilScheduledRefreshExists() {
        var statement = create("PT5M", "SNAPSHOT");
        assertThatThrownBy(() -> DynamicTableMaterializationService.requireManualSnapshotMode(statement))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("TARGET_LAG = 'PT0S'");
    }

    @Test
    void manualFullRefreshModeIsAcceptedButCdcIsRejected() {
        assertThatCode(() -> DynamicTableMaterializationService.requireManualSnapshotMode(
                create("PT0S", "SNAPSHOT"))).doesNotThrowAnyException();
        assertThatThrownBy(() -> DynamicTableMaterializationService.requireManualSnapshotMode(
                create("PT0S", "CDC")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("CDC");
    }

    @Test
    void refreshRestoresPersistedCreationDefaultsWithoutChangingAuthority() {
        CoordinatorSessionContext original = StatementTestSessions.context();
        CoordinatorSessionContext refresher = new CoordinatorSessionContext(
                original.queryId(), Map.of("catalog", "wrong", "schema", "wrong"),
                original.userIdentity(), original.tenantId());
        StatementExecutionContext execution = new StatementExecutionContext(
                null, refresher.queryId(), refresher, null, null);
        DynamicTablePublication definition = new DynamicTablePublication(
                original.tenantId(), "lake.analytics.rollup", "11111111-1111-1111-1111-111111111111",
                "SELECT id FROM events", "lake", "raw", "lake.analytics.__pg_dt_first", 1,
                DynamicTablePublication.State.ACTIVE, Instant.EPOCH, Instant.EPOCH);

        StatementExecutionContext bound = DynamicTableMaterializationService
                .withDefinitionBindings(execution, definition);
        assertThat(bound.session().sessionProperties())
                .containsEntry("catalog", "lake").containsEntry("schema", "raw");
        assertThat(bound.session().userIdentity()).isSameAs(refresher.userIdentity());
        assertThat(bound.session().tenantId()).isEqualTo(refresher.tenantId());
        assertThat(bound.queryId()).isEqualTo(refresher.queryId());
    }

    private static CreateDynamicTableStatement create(String lag, String mode) {
        return (CreateDynamicTableStatement) new SqlParser().parse(
                "CREATE DYNAMIC TABLE lake.analytics.rollup TARGET_LAG = '" + lag
                        + "' REFRESH_MODE = " + mode + " AS SELECT 1 AS id");
    }
}
