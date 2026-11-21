package org.pangrova.testing.integration.e2e.speculation;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.enums.ResultProtocol;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.E2ETest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@E2ETest
class IndependentCdcProcessesE2ETest extends ContainerTestBase {

    private enum Scenario { COMBINED, MAINTENANCE, WORKER_EXIT, ICEBERG_WORKER_EXIT }

    @Test
    void combinedCoordinatorPublishesKafkaRangesThroughWorkerHttpAndPaimon() throws Exception {
        verifyPublication(Scenario.COMBINED);
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
        processes.query("CREATE SCHEMA IF NOT EXISTS spec_catalog.default", ResultProtocol.GRPC_TLS);
        processes.query("CREATE TABLE spec_catalog.default.orders (id BIGINT, payload VARCHAR)", ResultProtocol.GRPC_TLS);
        processes.query("INSERT INTO spec_catalog.default.orders VALUES (99, 'unrelated')", ResultProtocol.GRPC_TLS);
        processes.query("CREATE STREAM orders_stream FROM KAFKA ("
                + "'bootstrap.servers'='" + broker + "', 'topic'='" + topic + "',"
                + "'group.id'='independent-cdc-orders') FORMAT 'debezium-json' INTO spec_catalog.default.orders"
                + " WITH ('cdc.write.privileges'='INSERT,UPDATE,DELETE')", ResultProtocol.GRPC_TLS);
        try (var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", broker,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>(topic, null,
                    "{\"op\":\"c\",\"after\":{\"id\":42,\"payload\":\"cdc-visible\"}}")).get(30, TimeUnit.SECONDS);
            awaitPublication(processes, 1);
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

    private static long icebergSnapshotCount(IndependentLocalProcesses processes) throws SQLException {
        try (var connection = processes.openJdbc();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT COUNT(*) FROM spec_catalog.default.orders$snapshots")) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
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
                    assertPublication(broker, processes, scenario);
                } catch (Exception | Error failure) {
                    failure.addSuppressed(new IllegalStateException(
                            "CDC process diagnostics retained at " + processes.retainDiagnostics()));
                    throw failure;
                }
            }
        }
    }

    private static void assertPublication(String broker, IndependentLocalProcesses processes, Scenario scenario)
            throws Exception {
        boolean streamingOnly = scenario == Scenario.MAINTENANCE;
        boolean iceberg = scenario == Scenario.ICEBERG_WORKER_EXIT;
        String topic = "independent-cdc-orders";
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", broker))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        processes.startCoordinatorAndFirstWorker();
        if (!streamingOnly) {
            processes.startSecondWorker();
            assertThat(processes.pid("local-worker-1")).isNotEqualTo(processes.pid("local-worker-2"));
        }
        if (iceberg) {
            processes.createSourceCatalog();
        } else {
            String warehouse = processes.warehouse().toString().replace("'", "''");
            processes.query("CREATE CATALOG spec_catalog WITH ('connector'='paimon', 'warehouse'='"
                    + warehouse + "')", ResultProtocol.GRPC_TLS);
        }
        processes.query("CREATE SCHEMA IF NOT EXISTS spec_catalog.default", ResultProtocol.GRPC_TLS);
        String key = iceberg ? "" : ", PRIMARY KEY (id) NOT ENFORCED";
        processes.query("CREATE TABLE spec_catalog.default.orders (id BIGINT, payload VARCHAR"
                + key + ")", ResultProtocol.GRPC_TLS);
        if (iceberg) {
            processes.query("INSERT INTO spec_catalog.default.orders VALUES (42, 'cdc-visible'), (99, 'unrelated')",
                    ResultProtocol.GRPC_TLS);
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
                verifyWorkerExit(processes, producer, new ProducerRecord<>(topic, null, "{\"op\":\"u\","
                        + "\"before\":{\"id\":42,\"payload\":\"cdc-visible\"},"
                        + "\"after\":{\"id\":42,\"payload\":\"cdc-updated\"}}"), true);
                return;
            }
            var insert = new ProducerRecord<String, String>(topic, null,
                    "{\"op\":\"c\",\"after\":{\"id\":42,\"payload\":\"cdc-visible\"}}");
            if (scenario == Scenario.WORKER_EXIT) {
                verifyWorkerExit(processes, producer, insert, false);
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

            producer.send(new ProducerRecord<>(topic, null,
                    "{\"op\":\"d\",\"before\":{\"id\":42,\"payload\":\"cdc-updated\"}}")).get(30, TimeUnit.SECONDS);
            awaitPublication(processes, 3);
            if (streamingOnly) {
                assertThat(paimonPayloads(processes)).isEmpty();
            } else {
                assertThat(payload(processes)).contains("0 rows");
            }
        }
        boolean workerCompletedCdc = processes.reports().deliveries().stream().anyMatch(delivery ->
                "CDC_INGEST".equals(delivery.report().taskType()) && "FINISHED".equals(delivery.report().state()));
        assertThat(workerCompletedCdc).isEqualTo(!streamingOnly);
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
                assertThat(payload(processes)).contains("cdc-visible").doesNotContain("cdc-updated");
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
        if (iceberg) {
            assertThat(payload(processes)).contains("cdc-updated", "1 row").doesNotContain("cdc-visible");
            assertThat(icebergSnapshotCount(processes)).isEqualTo(2);
        } else {
            assertThat(paimonPayloads(processes)).containsExactly("cdc-visible");
            assertThat(paimonSnapshot(processes)).isEqualTo(1L);
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
