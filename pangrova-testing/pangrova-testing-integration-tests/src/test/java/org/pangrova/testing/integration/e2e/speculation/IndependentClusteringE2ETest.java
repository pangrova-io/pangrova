package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@E2ETest
class IndependentClusteringE2ETest extends ContainerTestBase {
    private static final String TABLE = "spec_catalog.default.clustered_orders";
    private static final ResultProtocol FLIGHT = ResultProtocol.GRPC_TLS;

    @Test
    void recoversExactIcebergRewriteAfterTheJournalCannotRecordSuccess() throws Exception {
        try (var processes = new IndependentLocalProcesses(false)) {
            try {
                verifyRecovery(processes);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Clustering diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    private static void verifyRecovery(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        processes.query("CREATE SCHEMA IF NOT EXISTS spec_catalog.default", FLIGHT);
        processes.query("CREATE TABLE " + TABLE + " (id BIGINT, payload VARCHAR)", FLIGHT);
        processes.query("INSERT INTO " + TABLE + " VALUES (3, 'third'), (1, 'first'), (2, 'second')", FLIGHT);
        processes.query("ALTER TABLE " + TABLE + " SET CLUSTERING ORDER (id) MODE LINEAR", FLIGHT);
        assertThat(processes.query("SHOW CLUSTERING STATUS FOR TABLE " + TABLE, FLIGHT).output())
                .contains("LINEAR", "PENDING");
        rejectCompletion(processes);
        var failed = processes.executeSql("RECLUSTER TABLE " + TABLE, FLIGHT);
        assertThat(failed.exitCode()).isNotZero();
        var pending = latestRun(processes);
        assertThat(pending.state()).isEqualTo("RUNNING");
        var published = replacementSnapshot(processes);
        assertThat(published).isNotEqualTo(pending.base());
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_recluster_completion ON table_control_maintenance_run");
        }

        processes.restartQueryCoordinator();
        assertThat(processes.query("SHOW CLUSTERING STATUS FOR TABLE " + TABLE, FLIGHT).output())
                .contains("CLUSTERED", "SUCCEEDED");
        var recovered = latestRun(processes);
        assertThat(recovered.id()).isEqualTo(pending.id());
        assertThat(recovered.state()).isEqualTo("SUCCEEDED");
        assertThat(recovered.snapshot()).isEqualTo(published);
        assertThat(replacementSnapshot(processes)).isEqualTo(published);
        processes.query("ALTER TABLE " + TABLE + " SET CLUSTERING ORDER (id, payload) MODE INTERLEAVE", FLIGHT);
        assertThat(processes.query("SHOW CLUSTERING STATUS FOR TABLE " + TABLE, FLIGHT).output())
                .contains("INTERLEAVE", "PENDING");
        processes.query("RECLUSTER TABLE " + TABLE, FLIGHT);
        assertThat(processes.query("SHOW CLUSTERING STATUS FOR TABLE " + TABLE, FLIGHT).output())
                .contains("INTERLEAVE", "CLUSTERED", "SUCCEEDED");
        assertThat(processes.query("SELECT payload FROM " + TABLE + " ORDER BY id", FLIGHT).output())
                .contains("first", "second", "third", "3 rows");
    }

    private static void rejectCompletion(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION reject_recluster_completion() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        IF NEW.operation = 'RECLUSTER' AND NEW.state = 'SUCCEEDED' THEN
                            RAISE EXCEPTION 'injected maintenance journal failure';
                        END IF;
                        RETURN NEW;
                    END;
                    $$
                    """);
            statement.execute("""
                    CREATE TRIGGER reject_recluster_completion BEFORE UPDATE ON table_control_maintenance_run
                    FOR EACH ROW EXECUTE FUNCTION reject_recluster_completion()
                    """);
        }
    }

    private static Run latestRun(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT run_id, state, version_before, version_after
                        FROM table_control_maintenance_run WHERE operation = 'RECLUSTER'
                        ORDER BY started_at DESC
                        """)) {
            assertThat(rows.next()).isTrue();
            var run = new Run(rows.getString(1), rows.getString(2), rows.getString(3), rows.getString(4));
            assertThat(rows.next()).isFalse();
            return run;
        }
    }

    private static String replacementSnapshot(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.openJdbc();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT snapshot_id FROM " + TABLE
                        + "$snapshots WHERE operation = 'replace'")) {
            assertThat(rows.next()).isTrue();
            String snapshot = rows.getString(1);
            assertThat(rows.next()).isFalse();
            return snapshot;
        }
    }

    private record Run(String id, String state, String base, String snapshot) {}
}
