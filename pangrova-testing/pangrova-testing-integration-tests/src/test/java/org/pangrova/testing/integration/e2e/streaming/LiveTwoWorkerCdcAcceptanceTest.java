package org.pangrova.testing.integration.e2e.streaming;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.data.ConnectorCdcPageSink;
import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.coordinator.query.task.write.WriteAllocationIntent;
import org.pangrova.coordinator.streaming.CdcStreamExecution;
import org.pangrova.coordinator.streaming.CdcTaskDispatchPort;
import org.pangrova.coordinator.streaming.CdcTaskCompletionRegistry;
import org.pangrova.coordinator.streaming.CdcTaskPlanner;
import org.pangrova.coordinator.streaming.CdcWriteAuthority;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.cdc.CdcRangeBatchWriter;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.streaming.worker.CdcWorkerTaskBackend;
import org.pangrova.streaming.worker.CdcWorkerTaskOptions;
import org.pangrova.streaming.worker.CdcWorkerTaskResources;
import org.pangrova.streaming.worker.CdcWorkerTaskResourcesProvider;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.types.value.TypedRow;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Live acceptance for two Worker CDC execution.
 *
 * <p>Real Kafka records are planned into bounded ranges, executed by two independent
 * Worker-JVM backends, and published as one Paimon merge. The first attempt injects
 * a resource-open failure in one backend; it must not publish or acknowledge offsets.
 * The retry must publish exactly once and acknowledge only after publication.</p>
 */
@Testcontainers
class LiveTwoWorkerCdcAcceptanceTest extends ContainerTestBase {

    private static final String TOPIC = "cdc-live-orders";
    private static final String CATALOG_NAME = "cdc_catalog";
    private static final String DATABASE = "cdc_db";
    private static final String TABLE = "events";
    private static final String TENANT = "tenant-cdc";
    private static final ConnectorSession SESSION = new Session();

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @TempDir
    Path warehouse;

    @Test
    void failedWorkerExecutionDoesNotPublishOrAckAndRetryIsExactlyOnce() throws Exception {
        produceRecords(12);
        Catalog catalog = openCatalog();
        PaimonSplitRegistry splits = new PaimonSplitRegistry();
        CdcSourceRegistry sources = CdcSourceRegistry.discover(getClass().getClassLoader());
        ExecutorService tasks = Executors.newCachedThreadPool();
        WorkerFleet firstAttempt = null;
        WorkerFleet retry = null;
        try {
            PaimonMetadata metadata = new PaimonMetadata(catalog, CATALOG_NAME, splits);
            PaimonOutputTableHandle output = (PaimonOutputTableHandle) metadata.beginMerge(
                    SESSION, metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE)));
            CdcWriteAuthority.CdcWritePlan writePlan = writePlan(output);
            PaimonPageSinkProvider sinks = new PaimonPageSinkProvider(
                    catalog, warehouse.resolve("spill").toString());
            CdcRecordConverter converter = CdcRecordConverterRegistry
                    .discover(getClass().getClassLoader()).get("debezium-json");
            assertThat(converter).isNotNull();

            InMemoryWriteCommitJournal journal = new InMemoryWriteCommitJournal();
            List<WriteCommitEntry> published = new ArrayList<>();
            List<Map<TopicPartitionOffset, Long>> acknowledged = new ArrayList<>();
            AtomicBoolean crashWorkerB = new AtomicBoolean(true);
            WorkerFleet failingFleet = new WorkerFleet(
                    sources, converter, sinks, output, tasks, crashWorkerB, "attempt-a");
            WorkerFleet retryFleet = new WorkerFleet(
                    sources, converter, sinks, output, tasks, new AtomicBoolean(false), "attempt-b");
            firstAttempt = failingFleet;
            retry = retryFleet;

            CdcTaskPlanner.CdcTaskPlan firstPlan = plan(sources, writePlan, output, "attempt-a");
            assertThatThrownBy(() -> execution(failingFleet, metadata, output,
                    journal, published, acknowledged, Duration.ofSeconds(5))
                    .execute(firstPlan, writePlan, stream(), SESSION,
                            source(firstPlan), output.baseSnapshotId(), 1_000L,
                            "coordinator", "process", 7L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("did not report terminal state");
            assertThat(published).isEmpty();
            assertThat(acknowledged).isEmpty();
            assertThat(journal.isCommitted(firstPlan.tasks().get(0).getAttempt().getQueryOrJobId()))
                    .isFalse();
            assertThat(readRows(catalog)).isEmpty();
            assertThat(firstAttempt.executedTasks()).isGreaterThan(0);

            CdcTaskPlanner.CdcTaskPlan retryPlan = plan(sources, writePlan, output, "attempt-b");
            WriteCommitEntry committed = execution(retryFleet, metadata, output,
                    journal, published, acknowledged, Duration.ofSeconds(30))
                    .execute(retryPlan, writePlan, stream(), SESSION,
                            source(retryPlan), output.baseSnapshotId(), 2_000L,
                            "coordinator", "process", 8L);

            assertThat(published).containsExactly(committed);
            assertThat(journal.isCommitted(committed.queryId())).isTrue();
            assertThat(acknowledged).containsExactly(retryPlan.endOffsets());
            assertThat(retry.executedTasks()).isGreaterThan(0);
            assertThat(readRows(catalog)).hasSize(12)
                    .containsEntry(1L, "value-1")
                    .containsEntry(12L, "value-12");
        } finally {
            if (firstAttempt != null) {
                firstAttempt.close();
            }
            if (retry != null) {
                retry.close();
            }
            tasks.shutdownNow();
            splits.close();
            catalog.close();
        }
    }

    private CdcStreamExecution execution(
            WorkerFleet fleet,
            PaimonMetadata metadata,
            PaimonOutputTableHandle output,
            InMemoryWriteCommitJournal journal,
            List<WriteCommitEntry> published,
            List<Map<TopicPartitionOffset, Long>> acknowledged,
            Duration timeout) {
        return new CdcStreamExecution(
                fleet.dispatcher(),
                fleet.completions(),
                journal,
                intent -> {
                    metadata.finishDistributedWrite(
                            SESSION, output, intent.fragments(),
                            journal.publicationContext(intent, intent.executionFencingToken()));
                    published.add(intent);
                },
                (source, offsets, fingerprint) -> acknowledged.add(Map.copyOf(offsets)),
                timeout);
    }

    private CdcTaskPlanner.CdcTaskPlan plan(
            CdcSourceRegistry sources,
            CdcWriteAuthority.CdcWritePlan writePlan,
            PaimonOutputTableHandle output,
            String attempt) {
        try (CdcSourceReader reader = sources.open("kafka", attempt,
                sourceProperties("plan-" + attempt))) {
            return CdcTaskPlanner.plan(stream(), CdcTaskPlanner.SourceOffsets.observe(reader), writePlan, Map.of(),
                    2L, Long.MAX_VALUE, 1, attempt("cdc-" + attempt), security(), grant(),
                    System.currentTimeMillis() + 120_000L,
                    output.tableUuid(), output.baseSnapshotId());
        }
    }

    private CdcSourceIdentity source(CdcTaskPlanner.CdcTaskPlan plan) {
        List<CdcSourceIdentity.OffsetRange> ranges = new ArrayList<>();
        plan.tasks().forEach(task -> task.getCdcIngest().getRangesList().forEach(range ->
                ranges.add(new CdcSourceIdentity.OffsetRange(
                        range.getTopic(), range.getPartition(),
                        range.getStartOffset(), range.getEndOffset()))));
        return new CdcSourceIdentity("stream-live", "epoch-1", ranges);
    }

    private static CdcWriteAuthority.CdcWritePlan writePlan(PaimonOutputTableHandle output) {
        var receipt = new WriteAuthorizationReceipt(new byte[]{1},
                new WriteDeclarationSnapshot(
                        new QualifiedTableName(CATALOG_NAME, DATABASE, TABLE),
                        "0".repeat(64), "0".repeat(64), false));
        var codec = new PaimonOutputHandleCodec();
        byte[] payload = codec.serialize(output);
        var serialized = SerializedConnectorOutputHandle.newBuilder()
                .setTypeUrl(codec.typeUrl())
                .setCodecVersion(codec.codecVersion())
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setPayloadChecksum(
                        org.pangrova.protocol.checksum.ProtocolChecksums.sha256(payload))
                .addWriteColumnNames("id")
                .addWriteColumnNames("payload")
                .build();
        return new CdcWriteAuthority.CdcWritePlan(output,
                WriteOutputBinding.newBuilder()
                        .setConnector(ConnectorWriteBinding.newBuilder()
                                .setOutputHandle(serialized))
                        .build(),
                receipt, new WriteAllocationIntent("MERGE", Map.of()));
    }

    private Catalog openCatalog() throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE, false);
        catalog.createTable(Identifier.create(DATABASE, TABLE), Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("payload", DataTypes.STRING())
                .primaryKey(List.of("id"))
                .option("bucket", "1")
                .build(), false);
        return catalog;
    }

    private Map<Long, String> readRows(Catalog catalog) throws Exception {
        Table table = catalog.getTable(Identifier.create(DATABASE, TABLE));
        var builder = table.newReadBuilder().withProjection(new int[]{0, 1});
        Map<Long, String> rows = new java.util.TreeMap<>();
        try (var iterator = builder.newRead().createReader(builder.newScan().plan())
                .toCloseableIterator()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                rows.put(row.getLong(0), row.getString(1).toString());
            }
        }
        return rows;
    }

    private void produceRecords(int count) throws Exception {
        long topicDeadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try (AdminClient admin = AdminClient.create(Map.of(
                    "bootstrap.servers", KAFKA.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1)))
                        .all().get(10, TimeUnit.SECONDS);
                break;
            } catch (Exception failure) {
                if (System.nanoTime() >= topicDeadline) {
                    throw failure;
                }
                Thread.sleep(500L);
            }
        }
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", ByteArraySerializer.class.getName(),
                "value.serializer", ByteArraySerializer.class.getName()))) {
            for (int i = 1; i <= count; i++) {
                String payload = "{\"op\":\"c\",\"after\":{\"id\":" + i
                        + ",\"payload\":\"value-" + i + "\"}}";
                producer.send(new ProducerRecord<>(TOPIC, i % 2, null,
                        payload.getBytes(StandardCharsets.UTF_8)))
                        .get(30, TimeUnit.SECONDS);
            }
        }
    }

    private Map<String, String> sourceProperties(String group) {
        return Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "topic", TOPIC,
                "group.id", "pangrova-live-" + group,
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest");
    }

    private static StreamState stream() {
        return new StreamState("stream-live", TENANT, "checkpoint-live",
                StreamLifecycleState.RUNNING, CATALOG_NAME + "." + DATABASE + "." + TABLE,
                "kafka", Map.of("topic", TOPIC), "debezium-json",
                Map.of("cdc.write.privileges", "INSERT,UPDATE,DELETE"),
                "creator", "coordinator", 1L, 1, null,
                0L, 0L, 0L, null, Instant.EPOCH, Instant.EPOCH);
    }

    private static TaskAttemptId attempt(String queryId) {
        return TaskAttemptId.newBuilder().setQueryOrJobId(queryId)
                .setTaskId(queryId + "-task").setPlanEpoch(1)
                .setFencingToken(1).setControlEpoch(1)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("coordinator").setOwnerIncarnation("process")
                        .setFencingToken(1))
                .build();
    }

    private static SecurityContext security() {
        return SecurityContext.newBuilder().setTenantId(TENANT)
                .setPrincipal("creator").build();
    }

    private static StageResourceGrant grant() {
        return StageResourceGrant.newBuilder().setGrantId("grant")
                .setCpuSlots(1).setNativeMemoryBytes(1_048_576).setJvmHeapBytes(1_048_576).build();
    }

    private final class WorkerFleet implements AutoCloseable {
        private final CdcSourceRegistry sources;
        private final CdcRecordConverter converter;
        private final PaimonPageSinkProvider sinks;
        private final PaimonOutputTableHandle output;
        private final ExecutorService tasks;
        private final AtomicBoolean crashWorkerB;
        private final String group;
        private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        private final CdcTaskCompletionRegistry completions = new CdcTaskCompletionRegistry(tracker);
        private final CdcWorkerTaskBackend workerA;
        private final CdcWorkerTaskBackend workerB;
        private final AtomicInteger nextWorker = new AtomicInteger();
        private final AtomicInteger executedTasks = new AtomicInteger();

        private WorkerFleet(
                CdcSourceRegistry sources,
                CdcRecordConverter converter,
                PaimonPageSinkProvider sinks,
                PaimonOutputTableHandle output,
                ExecutorService tasks,
                AtomicBoolean crashWorkerB,
                String group) {
            this.sources = sources;
            this.converter = converter;
            this.sinks = sinks;
            this.output = output;
            this.tasks = tasks;
            this.crashWorkerB = crashWorkerB;
            this.group = group;
            this.workerA = backend();
            this.workerB = backend();
        }

        private CdcWorkerTaskBackend backend() {
            return new CdcWorkerTaskBackend(request -> resources(),
                    new CdcWorkerTaskOptions(10, Duration.ofMillis(100), Duration.ofSeconds(10)));
        }

        private CdcWorkerTaskResources resources() {
            CdcSourceReader reader = sources.open("kafka", group + "-" + executedTasks.incrementAndGet(),
                    sourceProperties(group + "-" + executedTasks.get()));
            return new CdcWorkerTaskResources() {
                @Override public CdcSourceReader sourceReader() { return reader; }
                @Override public CdcRecordConverter converter() { return converter; }
                @Override public CdcRangeBatchWriter writer() { return WorkerFleet.this::write; }
                @Override public void close() { reader.close(); }
            };
        }

        private List<byte[]> write(List<CdcEnvelope> envelopes) {
            ConnectorCdcPageSink sink = sinks.createCdcPageSink(
                    ConnectorTransactionHandle.empty(), SESSION, output);
            try {
                for (CdcEnvelope envelope : envelopes) {
                    switch (envelope.operation()) {
                        case INSERT -> sink.appendRow(row(envelope.after()), CdcRowKind.INSERT);
                        case UPDATE -> {
                            sink.appendRow(row(envelope.before()), CdcRowKind.UPDATE_BEFORE);
                            sink.appendRow(row(envelope.after()), CdcRowKind.UPDATE_AFTER);
                        }
                        case DELETE -> sink.appendRow(row(envelope.before()), CdcRowKind.DELETE);
                        default -> throw new IllegalStateException(
                                "Unsupported CDC operation: " + envelope.operation());
                    }
                }
                byte[] fragment = sink.finishWithMetadata();
                return fragment == null ? List.of() : List.of(fragment);
            } finally {
                sink.close();
            }
        }

        private CdcTaskDispatchPort dispatcher() {
            return request -> {
                String queryId = request.getAttempt().getQueryOrJobId();
                String taskId = request.getAttempt().getTaskId();
                tracker.registerTask(queryId, taskId);
                tracker.registerTaskRequest(taskId, SubmitTaskRequest.from(request));
                tracker.markScheduled(queryId, taskId);
                int index = nextWorker.getAndIncrement();
                String workerId = index % 2 == 0 ? "worker-a" : "worker-b";
                CdcWorkerTaskBackend backend = index % 2 == 0 ? workerA : workerB;
                if (workerId.equals("worker-b") && crashWorkerB.compareAndSet(true, false)) {
                    backend.close();
                    return;
                }
                long handle = backend.create(request);
                backend.start(handle);
                CompletableFuture.runAsync(() -> complete(
                        request, workerId, backend, handle), tasks);
            };
        }

        private void complete(
                TaskRequest request,
                String workerId,
                CdcWorkerTaskBackend backend,
                long handle) {
            try {
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (!backend.isFinished(handle) && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                TaskReport canonical = TaskReport.parseFrom(backend.getTaskStatus(handle));
                boolean finished = canonical.getState() == TaskState.TASK_STATE_FINISHED;
                WriteResultDescriptor write = finished
                        ? new WriteResultDescriptor(List.of(), 0, 0, new byte[0],
                                backend.getCommitFragments(handle))
                        : null;
                canonical = TaskReport.parseFrom(backend.release(handle).orElseThrow());
                var report = new TaskStatusReport(
                        request.getAttempt().getTaskId(),
                        request.getAttempt().getQueryOrJobId(),
                        request.getAttempt().getStageId(),
                        request.getAttempt().getPartitionId(),
                        "CDC_INGEST", workerId, finished ? "FINISHED" : "FAILED",
                        null, null, write, 1, 1,
                        finished ? null : "CDC_INGEST_FAILED", null,
                        TaskAttemptDescriptor.from(request.getAttempt()),
                        canonical.toByteArray());
                completions.accept(SubmitTaskRequest.from(request), report, canonical);
                executedTasks.incrementAndGet();
            } catch (IOException | InterruptedException failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("Worker completion failed", failure);
            } finally {
                backend.release(handle);
            }
        }

        private CdcTaskCompletionRegistry completions() {
            return completions;
        }

        private int executedTasks() {
            return executedTasks.get();
        }

        @Override
        public void close() {
            workerA.close();
            workerB.close();
        }
    }

    private static TypedRow row(CdcRow row) {
        if (row == null) {
            throw new IllegalStateException("CDC envelope has no row for its operation");
        }
        return TypedRow.fromRawMap(row.toMap());
    }

    private record Session() implements ConnectorSession {
        @Override public String getUser() { return "creator"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.of(DATABASE); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", TENANT); }
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() {
            return Optional.empty();
        }
    }
}
