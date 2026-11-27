package org.pangrova.testing.integration.e2e.speculation;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.maintenance.ReclusterStageRequest;
import org.pangrova.protocol.rest.maintenance.ReclusterStageResult;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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

    @Test
    void survivingWorkerRestagesWhenTheOriginalExitsBeforeItsReceiptArrives() throws Exception {
        var firstTarget = new AtomicReference<URI>();
        var secondTarget = new AtomicReference<URI>();
        try (var first = new ProcessWorkerHttpRelay(firstTarget::get);
                var second = new ProcessWorkerHttpRelay(secondTarget::get);
                var processes = new IndependentLocalProcesses(false)) {
            processes.reports().mapWorkerHttpPorts(worker -> {
                boolean initial = worker.workerId().equals("local-worker-1");
                (initial ? firstTarget : secondTarget).set(
                        URI.create("http://" + worker.httpHost() + ":" + worker.httpPort()));
                return (initial ? first : second).port();
            });
            try {
                prepareTable(processes);
                await("the setup SQL releases its tenant slots").atMost(Duration.ofSeconds(45))
                        .until(() -> processes.activeTenantReservations() == 0);
                processes.startSecondWorker();
                assertUnauthenticatedStagingIsRejected(first);
                verifyStagingFailure(processes, first, second);
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "Worker recluster diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            }
        }
    }

    private static void assertUnauthenticatedStagingIsRejected(ProcessWorkerHttpRelay worker) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + worker.port()
                        + ReclusterStageRequest.PATH))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}")).build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isEqualTo(401);
    }

    private static void verifyStagingFailure(IndependentLocalProcesses processes,
            ProcessWorkerHttpRelay first, ProcessWorkerHttpRelay second) throws Exception {
        var dropped = new AtomicReference<InterruptedStage>();
        var workerExited = new CountDownLatch(1);
        first.disconnectAfterDelivery(transfer -> holdStagingReceipt("local-worker-1", transfer, dropped, workerExited));
        second.disconnectAfterDelivery(transfer -> holdStagingReceipt("local-worker-2", transfer, dropped, workerExited));
        var client = ManagedExecutorFactory.newSingleThreadExecutor("recluster-test-client");
        try {
            var query = client.submit(() -> processes.query("RECLUSTER TABLE " + TABLE, FLIGHT));
            await("the first Worker stages real files before publication").atMost(Duration.ofSeconds(40))
                    .until(() -> dropped.get() != null);
            var interrupted = dropped.get();
            var lost = receipt(interrupted.transfer());
            assertThat(lost.files()).isNotEmpty();
            var running = latestRun(processes);
            assertThat(running.state()).isEqualTo("RUNNING");
            assertThat(publishedRewrites(processes)).isZero();
            processes.crashWorker(interrupted.workerId());
            workerExited.countDown();

            assertThat(query.get(3, TimeUnit.MINUTES).output()).contains("RECLUSTER SUCCEEDED", running.id());
            var survivor = interrupted.workerId().equals("local-worker-1") ? second : first;
            var winning = receipt(survivor.transfers().stream().filter(IndependentClusteringE2ETest::isStage)
                    .filter(transfer -> transfer.response().get().statusCode() == 200).findFirst().orElseThrow());
            assertThat(winning.attemptId()).isNotEqualTo(lost.attemptId());
            assertThat(winning.files()).isNotEmpty();
            assertThat(publishedRewrites(processes)).isEqualTo(1);
            assertThat(latestRun(processes).id()).isEqualTo(running.id());
            verifyWinnerFiles(processes, lost, winning);
        } finally {
            workerExited.countDown();
            client.shutdownNow();
            assertThat(client.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static boolean holdStagingReceipt(String workerId, ProcessWorkerHttpRelay.Transfer transfer,
            AtomicReference<InterruptedStage> dropped, CountDownLatch workerExited) {
        if (!isStage(transfer) || transfer.response().get().statusCode() != 200
                || !dropped.compareAndSet(null, new InterruptedStage(workerId, transfer))) return false;
        try {
            if (!workerExited.await(35, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Worker did not exit before the staging receipt deadline");
            }
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Staging receipt barrier was interrupted", interrupted);
        }
    }

    private static boolean isStage(ProcessWorkerHttpRelay.Transfer transfer) {
        return ReclusterStageRequest.PATH.equals(transfer.path().getPath());
    }

    private static ReclusterStageResult receipt(ProcessWorkerHttpRelay.Transfer transfer) throws java.io.IOException {
        return JsonMapperFactory.shared().readValue(transfer.response().get().body(), ReclusterStageResult.class);
    }

    private static long publishedRewrites(IndependentLocalProcesses processes) throws java.io.IOException {
        try (var catalog = ProcessInputSnapshot.openCatalog(processes.warehouse(), processes.hostJdbcUrl())) {
            var table = catalog.loadTable(TableIdentifier.of("default", "clustered_orders"));
            return StreamSupport.stream(table.snapshots().spliterator(), false)
                    .filter(snapshot -> snapshot.operation().equals("replace")).count();
        }
    }

    private static void verifyWinnerFiles(IndependentLocalProcesses processes,
            ReclusterStageResult lost, ReclusterStageResult winning) throws java.io.IOException {
        try (var catalog = ProcessInputSnapshot.openCatalog(processes.warehouse(), processes.hostJdbcUrl())) {
            var table = catalog.loadTable(TableIdentifier.of("default", "clustered_orders"));
            var snapshot = table.currentSnapshot();
            assertThat(snapshot.addedDataFiles(table.io())).isNotEmpty().allSatisfy(file -> {
                assertThat(file.path().toString()).contains(winning.attemptId().toString())
                        .doesNotContain(lost.attemptId().toString());
            });
            var ids = new ArrayList<Long>();
            for (var file : snapshot.addedDataFiles(table.io())) {
                try (CloseableIterable<Record> records =
                        Parquet.read(table.io().newInputFile(file.path().toString()))
                                .project(table.schema())
                                .createReaderFunc(schema -> GenericParquetReaders.buildReader(table.schema(), schema))
                                .build()) {
                    records.forEach(row -> ids.add((Long) row.getField("id")));
                }
            }
            assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 3L);
        }
    }

    private static void prepareTable(IndependentLocalProcesses processes) throws Exception {
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        processes.executeWithoutRows("CREATE SCHEMA IF NOT EXISTS spec_catalog.default", FLIGHT);
        processes.executeWithoutRows("CREATE TABLE " + TABLE + " (id BIGINT, payload VARCHAR)", FLIGHT);
        processes.executeWithoutRows("INSERT INTO " + TABLE + " VALUES (3, 'third'), (1, 'first'), (2, 'second')", FLIGHT);
        processes.query("ALTER TABLE " + TABLE + " SET CLUSTERING ORDER (id) MODE LINEAR", FLIGHT);
        assertThat(processes.query("SHOW CLUSTERING STATUS FOR TABLE " + TABLE, FLIGHT).output())
                .contains("LINEAR", "PENDING");
    }

    private static void verifyRecovery(IndependentLocalProcesses processes) throws Exception {
        prepareTable(processes);
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

    private record InterruptedStage(String workerId, ProcessWorkerHttpRelay.Transfer transfer) {}

    private record Run(String id, String state, String base, String snapshot) {}
}
