package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.common.enums.ResultDisposition;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@E2ETest
class IndependentRelationalWriteRecoveryE2ETest extends ContainerTestBase {

    @ParameterizedTest
    @EnumSource(Target.class)
    void survivingAttemptPublishesAfterOriginalWorkerExits(Target target) throws Exception {
        try (var processes = new IndependentSpeculationProcesses()) {
            try {
                processes.allowCatalog(target.catalog());
                processes.start();
                verifyWriteRecovery(processes, target);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Write recovery process diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Target.class)
    void restartedCoordinatorResolvesTheExactWriteIntent(Target target) throws Exception {
        try (var processes = new IndependentLocalProcesses(false)) {
            try {
                verifyPublishedWriteRecovery(processes, target);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Catalog publication diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    private static void verifyPublishedWriteRecovery(IndependentLocalProcesses processes, Target target)
            throws Exception {
        processes.allowCatalog(target.catalog());
        processes.startCoordinatorAndFirstWorker();
        String catalog = target.catalog();
        if (target == Target.ICEBERG) {
            processes.createSourceCatalog();
        } else {
            processes.executeWithoutRows(target.createCatalog(processes.warehouse()), ResultProtocol.GRPC_TLS);
        }
        processes.executeWithoutRows("CREATE SCHEMA IF NOT EXISTS " + catalog + ".default", ResultProtocol.GRPC_TLS);
        String table = catalog + ".default.recovery_after_publication";
        processes.executeWithoutRows("CREATE TABLE " + table + " (id BIGINT, payload VARCHAR)", ResultProtocol.GRPC_TLS);
        rejectCompletion(processes);
        var attempt = processes.executeSql("INSERT INTO " + table + " VALUES (42, 'exact-publication')",
                ResultProtocol.GRPC_TLS, Duration.ofSeconds(45));
        assertThat(attempt.exitCode()).isNotZero();
        var pending = storedIntent(processes);
        assertThat(pending.state()).isEqualTo("PENDING");
        // For Lance the pointer and journal share one PostgreSQL transaction.
        // The same fault leaves the staged data unpublished until recovery.
        requireRows(processes, table, target != Target.LANCE);
        if (target == Target.LANCE) {
            await("the running Coordinator encounters the same rollback during recovery")
                    .atMost(Duration.ofSeconds(45)).until(() -> rejectedCompletions(processes) >= 2);
        }

        processes.restartQueryCoordinator();
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_relational_completion ON write_commit_journal");
        }
        await("restarted Coordinator confirms the exact " + target + " write").atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    var recovered = storedIntent(processes);
                    assertThat(recovered.state()).isEqualTo("COMMITTED");
                    assertThat(recovered.queryId()).isEqualTo(pending.queryId());
                    assertThat(recovered.fingerprint()).isEqualTo(pending.fingerprint());
                });
        requireRows(processes, table, true);
    }

    private static void requireRows(IndependentLocalProcesses processes, String table, boolean published)
            throws Exception {
        try (var connection = processes.openJdbc();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT id, payload FROM " + table)) {
            assertThat(rows.next()).isEqualTo(published);
            if (!published) return;
            assertThat(rows.getLong(1)).isEqualTo(42L);
            assertThat(rows.getString(2)).isEqualTo("exact-publication");
            assertThat(rows.next()).isFalse();
        }
    }

    private static void rejectCompletion(IndependentLocalProcesses processes) throws java.sql.SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE SEQUENCE reject_relational_completion_attempts");
            statement.execute("""
                    CREATE FUNCTION reject_relational_completion() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        IF NEW.table_name = 'default.recovery_after_publication' AND NEW.state = 'COMMITTED' THEN
                            PERFORM nextval('reject_relational_completion_attempts');
                            RAISE EXCEPTION 'injected write-journal confirmation failure' USING ERRCODE = '40001';
                        END IF;
                        RETURN NEW;
                    END;
                    $$
                    """);
            statement.execute("""
                    CREATE TRIGGER reject_relational_completion BEFORE UPDATE ON write_commit_journal
                    FOR EACH ROW EXECUTE FUNCTION reject_relational_completion()
                    """);
        }
    }

    private static long rejectedCompletions(IndependentLocalProcesses processes) throws java.sql.SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT last_value FROM reject_relational_completion_attempts")) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private static Intent storedIntent(IndependentLocalProcesses processes) throws java.sql.SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT query_id, commit_fingerprint, state FROM write_commit_journal
                        WHERE table_name='default.recovery_after_publication'
                        """)) {
            assertThat(rows.next()).isTrue();
            var intent = new Intent(rows.getString(1), rows.getString(2), rows.getString(3));
            assertThat(rows.next()).isFalse();
            return intent;
        }
    }

    private record Intent(String queryId, String fingerprint, String state) {}

    private static void verifyWriteRecovery(IndependentSpeculationProcesses processes, Target target) throws Exception {
        String table = "recovered_output";
        String catalog = target.catalog();
        if (target != Target.ICEBERG) {
            execute(processes, target.createCatalog(processes.warehouse()));
            execute(processes, "CREATE SCHEMA IF NOT EXISTS " + catalog + ".default");
        }
        String destination = catalog + ".default." + table;
        execute(processes, "CREATE TABLE " + destination + " (id BIGINT, payload VARCHAR)");

        String sql = "INSERT INTO " + destination + " SELECT id, payload FROM spec_catalog.default.input";
        try (var scenario = new SpeculativeProcessScenario(
                processes, SpeculativeProcessScenario.Fault.NONE, true, sql, ResultDisposition.NONE)) {
            var original = scenario.original();
            var originalAttempt = original.request().taskRequest().getAttempt();
            var originalReport = scenario.report(originalAttempt.getTaskId(), "FINISHED");
            assertThat(originalReport.report().writeResult().commitMetadata()).isNotEmpty();
            processes.crashWorker(original.workerId());

            var copy = scenario.copy();
            var copyAttempt = copy.request().taskRequest().getAttempt();
            var copyReport = scenario.report(copyAttempt.getTaskId(), "FINISHED");
            assertThat(copyReport.report().writeResult().commitMetadata()).isNotEmpty();
            assertThat(publishedRows(processes, target, table)).isZero();
            scenario.releaseCopy();
            await("surviving Worker publishes before original receipt is delivered").atMost(Duration.ofMinutes(2))
                    .untilAsserted(() -> assertThat(publishedRows(processes, target, table))
                            .isEqualTo(ProcessInputSnapshot.ROW_COUNT));
            assertThat(originalReport.response().get()).isNull();
            if (target == Target.LANCE) {
                assertThat(preparedOnWinner(processes, copy.workerId(), copyAttempt.getQueryOrJobId())).isTrue();
            }
            scenario.complete(true);
            assertThat(publishedRows(processes, target, table)).isEqualTo(ProcessInputSnapshot.ROW_COUNT);
        }
    }

    private static boolean preparedOnWinner(IndependentSpeculationProcesses processes, String winnerId,
            String queryId) throws java.io.IOException {
        var winner = processes.workers().stream().filter(worker -> worker.workerId().equals(winnerId))
                .findFirst().orElseThrow();
        for (var transfer : winner.http().transfers()) {
            if (!transfer.path().getPath().equals("/v1/native-dataset/control")) continue;
            var command = RuntimeCommand.parseFrom(transfer.body());
            if (command.getCommandType() == RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE
                    && command.getPayload().unpack(PrepareDistributedWriteRequest.class).getExecutionId().equals(queryId)
                    && transfer.response().get() != null && transfer.response().get().statusCode() == 200) {
                return true;
            }
        }
        return false;
    }

    private static void execute(IndependentSpeculationProcesses processes, String sql) throws Exception {
        var result = processes.query(sql, ResultDisposition.NONE).get(2, TimeUnit.MINUTES);
        assertThat(result.succeeded()).as(result.output() + result.errors()).isTrue();
    }

    private static long publishedRows(IndependentSpeculationProcesses processes, Target target, String table)
            throws Exception {
        return switch (target) {
            case ICEBERG -> processes.input().committedRows(table);
            case PAIMON -> paimonRows(processes, table);
            case LANCE -> lanceRows(processes, table);
        };
    }

    private static long paimonRows(IndependentSpeculationProcesses processes, String name) throws Exception {
        var options = new Options();
        options.setString("warehouse", processes.warehouse().resolve("paimon").toUri().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create("default", name));
            var snapshot = table.latestSnapshot();
            if (snapshot.isEmpty()) return 0;
            var published = snapshot.orElseThrow();
            assertThat(published.id()).isEqualTo(1L);
            return published.totalRecordCount();
        }
    }

    private static long lanceRows(IndependentSpeculationProcesses processes, String table)
            throws java.sql.SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT visible_version, row_count FROM managed_source_heads
                        WHERE catalog_name=? AND tenant_id=? AND schema_name='default' AND table_name=?
                        """)) {
            statement.setString(1, Target.LANCE.catalog());
            statement.setString(2, ProcessSecurityAuthority.TENANT);
            statement.setString(3, table);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                long count = rows.getLong("row_count");
                assertThat(rows.getLong("visible_version")).isEqualTo(count == 0 ? 0 : 1);
                assertThat(rows.next()).isFalse();
                return count;
            }
        }
    }

    enum Target {
        ICEBERG, PAIMON, LANCE;

        String connector() { return name().toLowerCase(java.util.Locale.ROOT); }

        String catalog() { return this == ICEBERG ? "spec_catalog" : "recovery_" + connector(); }

        String createCatalog(java.nio.file.Path root) {
            String warehouse = root.resolve(connector()).toString().replace("'", "''");
            return "CREATE CATALOG " + catalog() + " USING " + connector()
                    + " WITH ('warehouse'='" + warehouse + "')";
        }
    }
}
