package org.pangrova.testing.integration.e2e.speculation;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@E2ETest
class IndependentCdcProcessesE2ETest extends ContainerTestBase {

    private static final int ICEBERG_CATALOG_BARRIER = 716_241;

    private enum Scenario { COMBINED, MAINTENANCE, WORKER_EXIT, ICEBERG_WORKER_EXIT }

    @Test
    void combinedCoordinatorPublishesKafkaRangesThroughWorkerHttpAndPaimon() throws Exception {
        try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))) {
            kafka.start();
            String broker = kafka.getBootstrapServers();
            var firstTarget = new AtomicReference<URI>();
            var secondTarget = new AtomicReference<URI>();
            var issuedId = new AtomicReference<String>();
            try (var processes = IndependentLocalProcesses.cdc(broker);
                    var first = new ProcessWorkerHttpRelay(firstTarget::get);
                    var second = new ProcessWorkerHttpRelay(secondTarget::get)) {
                processes.reports().mapWorkerHttpPorts(worker -> {
                    boolean isFirst = worker.workerId().equals("local-worker-1");
                    (isFirst ? firstTarget : secondTarget).set(URI.create(
                            "http://" + worker.httpHost() + ":" + worker.httpPort()));
                    return (isFirst ? first : second).port();
                });
                recordCdcIssuanceOnDelivery(processes, first, issuedId);
                recordCdcIssuanceOnDelivery(processes, second, issuedId);
                try {
                    assertPublication(broker, processes, Scenario.COMBINED,
                            IndependentCdcProcessesE2ETest::verifyWorkerExit);
                    assertThat(issuedId.get()).isNotBlank();
                    await("successful CDC exits the task journal and releases its lease").atMost(Duration.ofSeconds(10))
                            .until(() -> executionReleased(processes, issuedId.get())
                                    && cdcIssuanceCleared(processes, issuedId.get()));
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "CDC process diagnostics retained at " + processes.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    @Test
    void streamingOnlyCoordinatorPublishesKafkaMutationsAfterTakingOverWithoutWorkers() throws Exception {
        verifyPublication(Scenario.MAINTENANCE);
    }

    @Test
    void survivingWorkerRestagesAnUnacknowledgedBatchAfterProcessExit() throws Exception {
        verifyPublication(Scenario.WORKER_EXIT);
    }

    @Test
    void survivingWorkerRestagesIcebergUpdateAfterProcessExit() throws Exception {
        verifyPublication(Scenario.ICEBERG_WORKER_EXIT);
    }

    @ParameterizedTest
    @EnumSource(value = Scenario.class, names = {"WORKER_EXIT", "ICEBERG_WORKER_EXIT"})
    void survivingWorkerStagesTheRangeAfterAnExitBeforeTaskDelivery(Scenario scenario) throws Exception {
        try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))) {
            kafka.start();
            verifyExitBeforeDelivery(kafka.getBootstrapServers(), scenario);
        }
    }

    @Test
    void icebergUpdateRecoversItsPublishedSnapshotAndMaintenanceStagesDelete() throws Exception {
        try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))) {
            kafka.start();
            String broker = kafka.getBootstrapServers();
            var workerTarget = new AtomicReference<URI>();
            try (var processes = IndependentLocalProcesses.cdc(broker);
                    var workerHttp = new ProcessWorkerHttpRelay(workerTarget::get)) {
                try {
                    processes.reports().mapWorkerHttpPorts(worker -> {
                        workerTarget.set(URI.create("http://" + worker.httpHost() + ":" + worker.httpPort()));
                        return workerHttp.port();
                    });
                    verifyIcebergMutations(broker, processes, workerHttp);
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "Iceberg CDC diagnostics retained at " + processes.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    private static void verifyIcebergMutations(String broker, IndependentLocalProcesses processes,
            ProcessWorkerHttpRelay workerHttp) throws Exception {
        String topic = "independent-cdc-orders";
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        processes.startCoordinatorAndFirstWorker();
        processes.createSourceCatalog();
        processes.command("CREATE SCHEMA IF NOT EXISTS spec_catalog.default", ResultProtocol.GRPC_TLS);
        processes.command("CREATE TABLE spec_catalog.default.orders (id BIGINT, payload VARCHAR)", ResultProtocol.GRPC_TLS);
        processes.command("INSERT INTO spec_catalog.default.orders VALUES (99, 'unrelated')", ResultProtocol.GRPC_TLS);
        processes.query("CREATE STREAM orders_stream FROM KAFKA ("
                + "'bootstrap.servers'='" + broker + "', 'topic'='" + topic + "',"
                + "'group.id'='independent-cdc-orders') FORMAT 'debezium-json' INTO spec_catalog.default.orders"
                + " WITH ('cdc.write.privileges'='INSERT,UPDATE,DELETE')", ResultProtocol.GRPC_TLS);
        try (var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", broker,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            verifyIcebergCommitInterruption(processes, producer, topic);
            assertThat(payload(processes)).contains("cdc-visible");
            long before = icebergSnapshotCount(processes);

            rejectJournalCompletion(processes);
            producer.send(new ProducerRecord<>(topic, null, "{\"op\":\"u\","
                    + "\"before\":{\"id\":42,\"payload\":\"cdc-visible\"},"
                    + "\"after\":{\"id\":42,\"payload\":\"cdc-updated\"}}")).get(30, TimeUnit.SECONDS);
            await("Iceberg snapshot published with the write journal still pending").atMost(Duration.ofSeconds(60))
                    .until(() -> journalFingerprint(processes, "PENDING", 1) != null
                            && icebergSnapshotCount(processes) == before + 1);
            String fingerprint = journalFingerprint(processes, "PENDING", 1);
            var read = processes.query("SELECT payload FROM spec_catalog.default.orders WHERE id = 42",
                    ResultProtocol.GRPC_TLS);
            assertThat(hasPositionDeleteRead(workerHttp, read.queryId()))
                    .as("the real Worker read was bound to Iceberg delete files").isTrue();
            assertThat(read.output()).contains("cdc-updated").doesNotContain("cdc-visible");
            assertThat(processes.reports().deliveries()).anyMatch(delivery ->
                    "CDC_INGEST".equals(delivery.report().taskType()) && "FINISHED".equals(delivery.report().state())
                            && delivery.report().writeResult() != null
                            && !delivery.report().writeResult().commitFragments().isEmpty());
            processes.switchToMaintenanceOnly();
            dropCompletionTrigger(processes);
            awaitPublication(processes, 2);
            assertThat(journalFingerprint(processes, "COMMITTED", 2)).isEqualTo(fingerprint);

            rejectOffsetAcknowledgement(processes);
            producer.send(new ProducerRecord<>(topic, null,
                    "{\"op\":\"d\",\"before\":{\"id\":42,\"payload\":\"cdc-updated\"}}")).get(30, TimeUnit.SECONDS);
            verifyOffsetRecovery(processes, fingerprint);
        }
        processes.restartQueryCoordinator();
        assertThat(payload(processes)).contains("0 rows");
        assertThat(icebergSnapshotCount(processes)).isEqualTo(4);
        assertThat(processes.query("SELECT payload FROM spec_catalog.default.orders ORDER BY id",
                ResultProtocol.GRPC_TLS).output()).contains("unrelated").doesNotContain("cdc-");
    }

    private static void verifyIcebergCommitInterruption(IndependentLocalProcesses processes,
            KafkaProducer<String, String> producer, String topic) throws Exception {
        installIcebergCatalogBarrier(processes);
        String fingerprint;
        try (var barrier = processes.metadataDataSource().getConnection();
                var lock = barrier.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
            barrier.setAutoCommit(false);
            lock.setLong(1, ICEBERG_CATALOG_BARRIER);
            lock.execute();
            producer.send(new ProducerRecord<>(topic, null,
                    "{\"op\":\"c\",\"after\":{\"id\":42,\"payload\":\"cdc-visible\"}}")).get(30, TimeUnit.SECONDS);
            await("Iceberg metadata update is blocked while its CDC intent is pending")
                    .atMost(Duration.ofSeconds(45)).until(() -> icebergCommitBlocked(processes));
            fingerprint = pendingCdcFingerprint(processes);
            assertThat(icebergSnapshotCount(processes)).isEqualTo(1);
            assertThat(confirmed(processes, 1)).isFalse();
            processes.restartQueryCoordinatorAfterCrash(() -> {
                barrier.rollback();
                try (var connection = processes.metadataDataSource().getConnection();
                        var statement = connection.createStatement()) {
                    statement.execute("DROP TRIGGER hold_cdc_catalog_commit ON iceberg_tables");
                }
            });
        }
        await("recovery acknowledges the interrupted Iceberg catalog commit").atMost(Duration.ofMinutes(2))
                .until(() -> confirmed(processes, 1));
        assertThat(journalFingerprint(processes, "COMMITTED", 1)).isEqualTo(fingerprint);
        assertThat(icebergSnapshotCount(processes)).isEqualTo(2);
    }

    private static void installIcebergCatalogBarrier(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION hold_cdc_catalog_commit() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        IF NEW.table_name = 'orders' THEN
                            PERFORM pg_advisory_xact_lock(%d);
                        END IF;
                        RETURN NEW;
                    END;
                    $$
                    """.formatted(ICEBERG_CATALOG_BARRIER));
            statement.execute("""
                    CREATE TRIGGER hold_cdc_catalog_commit BEFORE UPDATE OF metadata_location ON iceberg_tables
                    FOR EACH ROW EXECUTE FUNCTION hold_cdc_catalog_commit()
                    """);
        }
    }

    private static boolean icebergCommitBlocked(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT 1 FROM pg_locks
                        WHERE locktype='advisory' AND classid=0 AND objid=CAST(? AS oid) AND NOT granted
                        """)) {
            statement.setInt(1, ICEBERG_CATALOG_BARRIER);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static String pendingCdcFingerprint(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT commit_fingerprint FROM write_commit_journal
                        WHERE cdc_stream_id='orders_stream' AND state='PENDING'
                        """)) {
            assertThat(rows.next()).isTrue();
            String fingerprint = rows.getString(1);
            assertThat(rows.next()).isFalse();
            return fingerprint;
        }
    }

    private static boolean hasPositionDeleteRead(ProcessWorkerHttpRelay workerHttp, String queryId)
            throws java.io.IOException {
        for (var transfer : workerHttp.transfers()) {
            if (!transfer.isTaskSubmission()) continue;
            var task = transfer.submittedTask().taskRequest();
            if (!task.getAttempt().getQueryOrJobId().equals(queryId)) continue;
            if (task.getSplitsList().stream().map(DataSplitProtoCodec::fileDetail)
                    .anyMatch(detail -> detail.hasIceberg() && detail.getIceberg().getDeleteFilesCount() > 0)) {
                return true;
            }
        }
        return false;
    }

    private static long icebergSnapshotCount(IndependentLocalProcesses processes) throws IOException {
        try (var catalog = ProcessInputSnapshot.openCatalog(processes.warehouse(), processes.hostJdbcUrl())) {
            var table = catalog.loadTable(TableIdentifier.of("default", "orders"));
            return StreamSupport.stream(table.snapshots().spliterator(), false).count();
        }
    }

    private static void rejectOffsetAcknowledgement(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION reject_cdc_offset() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        IF NEW.stream_name = 'orders_stream' AND NEW.committed_offset = 3 THEN
                            RAISE EXCEPTION 'injected offset failure after CDC journal completion';
                        END IF;
                        RETURN NEW;
                    END;
                    $$
                    """);
            statement.execute("""
                    CREATE TRIGGER reject_cdc_offset BEFORE UPDATE ON ingestion_stream_offsets
                    FOR EACH ROW EXECUTE FUNCTION reject_cdc_offset()
                    """);
        }
    }

    private static void verifyOffsetRecovery(IndependentLocalProcesses processes, String priorFingerprint)
            throws Exception {
        await("DELETE is committed while the source checkpoint is still at offset two").atMost(Duration.ofSeconds(45))
                .until(() -> {
                    String actual = journalFingerprint(processes, "COMMITTED", 2);
                    return actual != null && !actual.equals(priorFingerprint);
                });
        processes.stopCoordinator();
        String fingerprint = journalFingerprint(processes, "COMMITTED", 2);
        assertThat(fingerprint).isNotBlank().isNotEqualTo(priorFingerprint);
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_cdc_offset ON ingestion_stream_offsets");
        }
        processes.startMaintenanceOnly();
        awaitPublication(processes, 3);
        assertThat(journalFingerprint(processes, "COMMITTED", 3)).isEqualTo(fingerprint);
    }

    private static void verifyPublication(Scenario scenario) throws Exception {
        try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))) {
            kafka.start();
            String broker = kafka.getBootstrapServers();
            try (var processes = IndependentLocalProcesses.cdc(broker)) {
                try {
                    assertPublication(broker, processes, scenario, IndependentCdcProcessesE2ETest::verifyWorkerExit);
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "CDC process diagnostics retained at " + processes.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    private static void verifyExitBeforeDelivery(String broker, Scenario scenario) throws Exception {
        var firstTarget = new AtomicReference<URI>();
        var secondTarget = new AtomicReference<URI>();
        var interrupted = new AtomicReference<InterruptedDelivery>();
        var workerExited = new CountDownLatch(1);
        try (var first = new ProcessWorkerHttpRelay(firstTarget::get);
                var second = new ProcessWorkerHttpRelay(secondTarget::get);
                var processes = IndependentLocalProcesses.cdc(broker)) {
            first.disconnectBeforeDelivery(transfer ->
                    disconnectCdcDelivery(transfer, "local-worker-1", interrupted, workerExited));
            second.disconnectBeforeDelivery(transfer ->
                    disconnectCdcDelivery(transfer, "local-worker-2", interrupted, workerExited));
            processes.reports().mapWorkerHttpPorts(worker -> {
                boolean isFirst = worker.workerId().equals("local-worker-1");
                (isFirst ? firstTarget : secondTarget).set(URI.create(
                        "http://" + worker.httpHost() + ":" + worker.httpPort()));
                return (isFirst ? first : second).port();
            });
            try {
                assertPublication(broker, processes, scenario, (owner, producer, change, iceberg) -> {
                    producer.send(change).get(30, TimeUnit.SECONDS);
                    await("the first CDC task is intercepted before Worker delivery").atMost(Duration.ofSeconds(45))
                            .until(() -> interrupted.get() != null);
                    var delivery = interrupted.get();
                    assertThat(unpublished(owner)).isTrue();
                    owner.crashWorker(delivery.workerId());
                    workerExited.countDown();
                    await("a surviving Worker stages and commits the original CDC range").atMost(Duration.ofMinutes(7))
                            .until(() -> confirmed(owner, 1));
                    assertThat(owner.reports().deliveries()).noneMatch(report ->
                            report.report().taskId().equals(delivery.taskId()));
                    await("Worker membership removes the departed incarnation before the verification read")
                            .atMost(Duration.ofMinutes(3))
                            .until(() -> workerRegistrationRemoved(owner, delivery.workerId()));
                    verifyWorkerRecovery(owner, iceberg);
                });
            } catch (Exception | Error failure) {
                failure.addSuppressed(new IllegalStateException(
                        "CDC pre-delivery diagnostics retained at " + processes.retainDiagnostics()));
                throw failure;
            } finally {
                workerExited.countDown();
            }
        }
    }

    private static boolean disconnectCdcDelivery(ProcessWorkerHttpRelay.Transfer transfer, String workerId,
            AtomicReference<InterruptedDelivery> interrupted, CountDownLatch workerExited) {
        if (!transfer.isTaskSubmission()) return false;
        try {
            var task = transfer.submittedTask().taskRequest();
            if (task.getTaskKind() != TaskKind.TASK_KIND_CDC_INGEST) return false;
            String taskId = task.getAttempt().getTaskId();
            interrupted.compareAndSet(null, new InterruptedDelivery(workerId, taskId));
            if (!interrupted.get().taskId().equals(taskId)) return false;
            if (!workerExited.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Worker exit did not complete at the CDC delivery barrier");
            }
            return true;
        } catch (IOException malformed) {
            throw new UncheckedIOException(malformed);
        } catch (InterruptedException interruptedWait) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("CDC delivery barrier was interrupted", interruptedWait);
        }
    }

    private record InterruptedDelivery(String workerId, String taskId) {}

    @FunctionalInterface
    private interface WorkerExitVerification {
        void verify(IndependentLocalProcesses processes, KafkaProducer<String, String> producer,
                    ProducerRecord<String, String> change, boolean iceberg) throws Exception;
    }

    private static void assertPublication(String broker, IndependentLocalProcesses processes, Scenario scenario,
            WorkerExitVerification workerExit)
            throws Exception {
        boolean streamingOnly = scenario == Scenario.MAINTENANCE;
        boolean iceberg = scenario == Scenario.ICEBERG_WORKER_EXIT;
        String topic = "independent-cdc-orders";
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        processes.startCoordinatorAndFirstWorker();
        processes.enableMetadataStatementCounters();
        long legacyReads = processes.metadataStatementCalls("%FROM cdc_commit_state%");
        if (iceberg) {
            processes.createSourceCatalog();
        } else {
            String warehouse = processes.warehouse().toString().replace("'", "''");
            processes.command("CREATE CATALOG spec_catalog WITH ('connector'='paimon', 'warehouse'='"
                    + warehouse + "')", ResultProtocol.GRPC_TLS);
        }
        processes.command("CREATE SCHEMA IF NOT EXISTS spec_catalog.default", ResultProtocol.GRPC_TLS);
        String key = iceberg ? "" : ", PRIMARY KEY (id) NOT ENFORCED";
        processes.command("CREATE TABLE spec_catalog.default.orders (id BIGINT, payload VARCHAR"
                + key + ")", ResultProtocol.GRPC_TLS);
        if (iceberg) {
            var seed = processes.command(
                    "INSERT INTO spec_catalog.default.orders VALUES (42, 'cdc-visible'), (99, 'unrelated')",
                    ResultProtocol.GRPC_TLS);
            await("setup INSERT releases its execution anchor before the Worker fault")
                    .atMost(Duration.ofSeconds(45)).until(() -> executionReleased(processes, seed.queryId()));
        }
        if (!streamingOnly) {
            processes.startSecondWorker();
            assertThat(processes.pid("local-worker-1")).isNotEqualTo(processes.pid("local-worker-2"));
        }
        processes.query("CREATE STREAM orders_stream FROM KAFKA ("
                + "'bootstrap.servers'='" + broker + "', 'topic'='" + topic + "',"
                + "'group.id'='independent-cdc-orders') FORMAT 'debezium-json' INTO spec_catalog.default.orders"
                + " WITH ('cdc.write.privileges'='INSERT,UPDATE,DELETE')", ResultProtocol.GRPC_TLS);

        if (streamingOnly) {
            processes.switchToMaintenanceOnly();
        }

        try (var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", broker,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            if (iceberg) {
                workerExit.verify(processes, producer, new ProducerRecord<>(topic, null, "{\"op\":\"u\","
                        + "\"before\":{\"id\":42,\"payload\":\"cdc-visible\"},"
                        + "\"after\":{\"id\":42,\"payload\":\"cdc-updated\"}}"), true);
                return;
            }
            var insert = new ProducerRecord<String, String>(topic, null,
                    "{\"op\":\"c\",\"after\":{\"id\":42,\"payload\":\"cdc-visible\"}}");
            if (scenario == Scenario.WORKER_EXIT) {
                workerExit.verify(processes, producer, insert, false);
            } else if (scenario == Scenario.COMBINED) {
                verifyPaimonCommitInterruption(processes, producer, insert);
            } else {
                producer.send(insert).get(30, TimeUnit.SECONDS);
                awaitPublication(processes, 1);
            }
            if (streamingOnly) {
                assertThat(paimonPayloads(processes)).containsExactly("cdc-visible");
            } else {
                assertThat(payload(processes)).contains("cdc-visible");
            }

            if (streamingOnly) {
                rejectJournalCompletion(processes);
            }
            producer.send(new ProducerRecord<>(topic, null, "{\"op\":\"u\","
                    + "\"before\":{\"id\":42,\"payload\":\"cdc-visible\"},"
                    + "\"after\":{\"id\":42,\"payload\":\"cdc-updated\"}}")).get(30, TimeUnit.SECONDS);
            if (streamingOnly) {
                verifyMaintenanceRecovery(processes);
            } else {
                awaitPublication(processes, 2);
                assertThat(payload(processes)).contains("cdc-updated").doesNotContain("cdc-visible");
            }

            long updatedSnapshot = streamingOnly ? paimonSnapshot(processes) : 0;
            String updateFingerprint = streamingOnly ? journalFingerprint(processes, "COMMITTED", 2) : null;
            if (streamingOnly) rejectOffsetAcknowledgement(processes);
            producer.send(new ProducerRecord<>(topic, null,
                    "{\"op\":\"d\",\"before\":{\"id\":42,\"payload\":\"cdc-updated\"}}")).get(30, TimeUnit.SECONDS);
            if (streamingOnly) {
                verifyOffsetRecovery(processes, updateFingerprint);
                assertThat(paimonSnapshot(processes)).isEqualTo(updatedSnapshot + 1);
                assertThat(paimonPayloads(processes)).isEmpty();
            } else {
                awaitPublication(processes, 3);
                assertThat(payload(processes)).contains("0 rows");
            }
        }
        boolean workerCompletedCdc = processes.reports().deliveries().stream().anyMatch(delivery ->
                "CDC_INGEST".equals(delivery.report().taskType()) && "FINISHED".equals(delivery.report().state()));
        assertThat(workerCompletedCdc).isEqualTo(!streamingOnly);
        assertThat(processes.metadataStatementCalls("%FROM cdc_commit_state%"))
                .as("The journaled CDC path reads only its own checkpoint authority")
                .isEqualTo(legacyReads);
        assertThat(processes.coordinatorLog()).doesNotContain("event=stream_resume_all");
    }

    private static void verifyPaimonCommitInterruption(IndependentLocalProcesses processes,
            KafkaProducer<String, String> producer, ProducerRecord<String, String> insert) throws Exception {
        Path snapshotDirectory;
        var options = new Options();
        options.setString("warehouse", processes.warehouse().toUri().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create("default", "orders"));
            snapshotDirectory = Path.of(table.snapshotManager().snapshotDirectory().toUri());
        }
        Files.createDirectories(snapshotDirectory);
        var original = Files.getPosixFilePermissions(snapshotDirectory);
        var held = EnumSet.copyOf(original);
        held.removeAll(EnumSet.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.GROUP_WRITE,
                PosixFilePermission.OTHERS_WRITE));
        String fingerprint;
        try {
            Files.setPosixFilePermissions(snapshotDirectory, held);
            assertThat(Files.isWritable(snapshotDirectory)).isFalse();
            producer.send(insert).get(30, TimeUnit.SECONDS);
            await("Paimon SDK fails while publishing the staged CDC snapshot").atMost(Duration.ofSeconds(45))
                    .until(() -> processes.coordinatorLogged("Failed to commit Paimon distributed write"));
            fingerprint = pendingCdcFingerprint(processes);
            assertThat(paimonPayloads(processes)).isEmpty();
            assertThat(confirmed(processes, 1)).isFalse();
            processes.restartQueryCoordinatorAfterCrash(() -> Files.setPosixFilePermissions(snapshotDirectory, original));
        } finally {
            Files.setPosixFilePermissions(snapshotDirectory, original);
        }
        await("Paimon recovers the interrupted CDC snapshot").atMost(Duration.ofMinutes(2))
                .until(() -> confirmed(processes, 1));
        assertThat(journalFingerprint(processes, "COMMITTED", 1)).isEqualTo(fingerprint);
        assertThat(paimonSnapshot(processes)).isEqualTo(1);
    }

    private static void verifyWorkerExit(IndependentLocalProcesses processes, KafkaProducer<String, String> producer,
            ProducerRecord<String, String> change, boolean iceberg) throws Exception {
        var withheld = new AtomicReference<TaskStatusReport>();
        try (var hold = processes.reports().holdReports(report -> {
            if (!"CDC_INGEST".equals(report.taskType()) || !"FINISHED".equals(report.state())) {
                return false;
            }
            withheld.compareAndSet(null, report);
            return withheld.get().taskId().equals(report.taskId());
        })) {
            producer.send(change).get(30, TimeUnit.SECONDS);
            await("production Worker stages the exact CDC range").atMost(Duration.ofSeconds(45))
                    .until(() -> withheld.get() != null);
            var lost = withheld.get();
            assertThat(lost.writeResult().commitFragments()).isNotEmpty();
            if (iceberg) {
                assertThat(icebergSnapshotCount(processes)).isEqualTo(1);
            }
            processes.crashWorker(lost.workerId());
            if (!iceberg) assertThat(paimonPayloads(processes)).isEmpty();
            assertThat(unpublished(processes)).isTrue();

            // An unresolved task times out at the production deadline. Its receipt stays blocked
            // while a different Worker stages the same source range and publishes a fresh intent.
            await("surviving Worker publishes after the lost attempt's deadline").atMost(Duration.ofMinutes(7))
                    .until(() -> confirmed(processes, 1));
            assertThat(processes.reports().deliveries()).anyMatch(delivery -> {
                var report = delivery.report();
                return "CDC_INGEST".equals(report.taskType()) && "FINISHED".equals(report.state())
                        && !report.workerId().equals(lost.workerId()) && !report.queryId().equals(lost.queryId());
            });
        }
        verifyWorkerRecovery(processes, iceberg);
    }

    private static void verifyWorkerRecovery(IndependentLocalProcesses processes, boolean iceberg) throws Exception {
        if (iceberg) {
            assertThat(processes.query("SELECT payload FROM spec_catalog.default.orders ORDER BY id",
                    ResultProtocol.GRPC_TLS).output()).contains("cdc-updated", "unrelated", "2 rows")
                    .doesNotContain("cdc-visible");
            assertThat(icebergSnapshotCount(processes)).isEqualTo(2);
        } else {
            assertThat(paimonPayloads(processes)).containsExactly("cdc-visible");
            assertThat(paimonSnapshot(processes)).isEqualTo(1L);
        }
    }

    private static void recordCdcIssuanceOnDelivery(IndependentLocalProcesses processes,
            ProcessWorkerHttpRelay workerHttp, AtomicReference<String> issuedId) {
        workerHttp.beforeDelivery(transfer -> {
            if (!transfer.isTaskSubmission()) return;
            try {
                var request = transfer.submittedTask().taskRequest();
                if (request.getTaskKind() == TaskKind.TASK_KIND_CDC_INGEST) {
                    assertCommittedCdcIssuance(processes, request, workerHttp.port());
                    issuedId.set(request.getAttempt().getQueryOrJobId());
                }
            } catch (IOException malformed) {
                throw new UncheckedIOException(malformed);
            } catch (SQLException failure) {
                throw new IllegalStateException("Cannot inspect committed CDC issuance", failure);
            }
        });
    }

    private static void assertCommittedCdcIssuance(
            IndependentLocalProcesses processes, TaskRequest request, int controlPort) throws SQLException {
        var attempt = request.getAttempt();
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT t.worker_http_port, t.admission_seal, t.owner_fencing_token, a.fencing_token
                        FROM task_state_journal t JOIN execution_anchor a ON t.query_id = a.execution_id
                        WHERE t.query_id = ? AND t.task_id = ? AND t.admission_seal IS NOT NULL
                        """)) {
            statement.setString(1, attempt.getQueryOrJobId());
            statement.setString(2, attempt.getTaskId());
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt("worker_http_port")).isEqualTo(controlPort);
                assertThat(rows.getBytes("admission_seal"))
                        .isEqualTo(CanonicalTaskAdmissionSeals.create(request).toByteArray());
                assertThat(rows.getLong("owner_fencing_token")).isEqualTo(attempt.getExecutionOwner().getFencingToken());
                assertThat(rows.getLong("fencing_token")).isEqualTo(attempt.getExecutionOwner().getFencingToken());
                assertThat(rows.next()).isFalse();
            }
        }
    }

    private static boolean cdcIssuanceCleared(IndependentLocalProcesses processes, String executionId)
            throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("SELECT 1 FROM task_state_journal WHERE query_id = ?")) {
            statement.setString(1, executionId);
            try (var rows = statement.executeQuery()) {
                return !rows.next();
            }
        }
    }

    private static boolean executionReleased(IndependentLocalProcesses processes, String queryId) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("SELECT 1 FROM execution_anchor WHERE execution_id=?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                return !rows.next();
            }
        }
    }

    private static boolean workerRegistrationRemoved(IndependentLocalProcesses processes, String workerId)
            throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("SELECT 1 FROM worker_registry WHERE worker_id=?")) {
            statement.setString(1, workerId);
            try (var rows = statement.executeQuery()) {
                return !rows.next();
            }
        }
    }

    private static boolean unpublished(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("""
                        SELECT NOT EXISTS (SELECT 1 FROM write_commit_journal WHERE cdc_stream_id='orders_stream')
                        AND NOT EXISTS (SELECT 1 FROM ingestion_stream_offsets WHERE stream_name='orders_stream')
                        """)) {
            return result.next() && result.getBoolean(1);
        }
    }

    private static void rejectJournalCompletion(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION reject_cdc_journal_completion() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        IF NEW.cdc_stream_id = 'orders_stream' AND NEW.state = 'COMMITTED' THEN
                            RAISE EXCEPTION 'injected failure after CDC catalog publication';
                        END IF;
                        RETURN NEW;
                    END;
                    $$
                    """);
            statement.execute("""
                    CREATE TRIGGER reject_cdc_journal_completion BEFORE UPDATE ON write_commit_journal
                    FOR EACH ROW EXECUTE FUNCTION reject_cdc_journal_completion()
                    """);
        }
    }

    private static void verifyMaintenanceRecovery(IndependentLocalProcesses processes) throws Exception {
        await("Paimon UPDATE visible while journal completion is unavailable").atMost(Duration.ofSeconds(45))
                .until(() -> journalFingerprint(processes, "PENDING", 1) != null
                        && paimonPayloads(processes).equals(List.of("cdc-updated")));
        String fingerprint = journalFingerprint(processes, "PENDING", 1);
        long snapshot = paimonSnapshot(processes);
        processes.stopCoordinator();
        assertThat(journalFingerprint(processes, "PENDING", 1)).isEqualTo(fingerprint);
        dropCompletionTrigger(processes);
        processes.startMaintenanceOnly();
        awaitPublication(processes, 2);
        assertThat(journalFingerprint(processes, "COMMITTED", 2)).isEqualTo(fingerprint);
        assertThat(paimonSnapshot(processes)).isEqualTo(snapshot);
        assertThat(paimonPayloads(processes)).containsExactly("cdc-updated");
    }

    private static void dropCompletionTrigger(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_cdc_journal_completion ON write_commit_journal");
        }
    }

    private static String journalFingerprint(IndependentLocalProcesses processes, String state, long offset)
            throws SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT commit_fingerprint FROM write_commit_journal
                        WHERE cdc_stream_id='orders_stream' AND state=?
                        AND EXISTS (SELECT 1 FROM ingestion_stream_offsets
                            WHERE stream_name='orders_stream' AND committed_offset=?)
                        ORDER BY timestamp_ms DESC, query_id DESC LIMIT 1
                        """)) {
            statement.setString(1, state);
            statement.setLong(2, offset);
            try (var result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        }
    }

    private static long paimonSnapshot(IndependentLocalProcesses processes) throws Exception {
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(paimonOptions(processes)))) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create("default", "orders"));
            return table.latestSnapshot().orElseThrow().id();
        }
    }

    private static Options paimonOptions(IndependentLocalProcesses processes) {
        Options options = new Options();
        options.setString("warehouse", processes.warehouse().toUri().toString());
        return options;
    }

    private static List<String> paimonPayloads(IndependentLocalProcesses processes) throws Exception {
        List<String> payloads = new ArrayList<>();
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(paimonOptions(processes)))) {
            var table = catalog.getTable(Identifier.create("default", "orders"));
            var builder = table.newReadBuilder().withProjection(new int[]{0, 1});
            try (var rows = builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
                while (rows.hasNext()) {
                    var row = rows.next();
                    assertThat(row.getLong(0)).isEqualTo(42L);
                    payloads.add(row.getString(1).toString());
                }
            }
        }
        return payloads;
    }

    private static String payload(IndependentLocalProcesses processes) throws Exception {
        return processes.query("SELECT payload FROM spec_catalog.default.orders WHERE id = 42",
                ResultProtocol.GRPC_TLS).output();
    }

    private static void awaitPublication(IndependentLocalProcesses processes, long endOffset) {
        await("confirmed CDC publication of exclusive end offset " + endOffset).atMost(Duration.ofSeconds(45))
                .until(() -> confirmed(processes, endOffset));
    }

    private static boolean confirmed(IndependentLocalProcesses processes, long endOffset) throws java.sql.SQLException {
        try (var connection = processes.metadataDataSource().getConnection();
                var statement = connection.prepareStatement("SELECT COUNT(*) FROM write_commit_journal"
                        + " WHERE cdc_stream_id='orders_stream' AND state='COMMITTED'"
                        + " AND EXISTS (SELECT 1 FROM ingestion_stream_offsets"
                        + " WHERE stream_name='orders_stream' AND committed_offset=?)")) {
            statement.setLong(1, endOffset);
            try (var result = statement.executeQuery()) {
                return result.next() && result.getLong(1) == endOffset;
            }
        }
    }
}
