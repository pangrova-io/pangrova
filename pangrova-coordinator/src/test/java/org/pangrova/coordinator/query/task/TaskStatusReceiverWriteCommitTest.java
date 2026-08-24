package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * Verifies TaskStatusReceiver correctly collects native write commitMetadata
 * from task status reports and calls finishDistributedWrite on query completion.
 *
 * <p>Uses hand-written test doubles (no mock frameworks) per pangrova-rules §12.
 * Follows wiring patterns from {@code SpeculativeExecutionWiringTest}.</p>
 */
class TaskStatusReceiverWriteCommitTest {

    private static final String QUERY_ID = "q-write-1";
    private static final String WORKER_ID = "w-1";
    private static final String CATALOG_NAME = "test-catalog";

    private CoordinatorTaskTracker tracker;
    private NativeWriteCommitTracker writeCommitTracker;
    private RecordingConnectorMetadata recordingMetadata;
    private TaskStatusReceiver receiver;

    private long sequenceCounter = 100;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(workerInfo(WORKER_ID));
        writeCommitTracker = new NativeWriteCommitTracker();
        recordingMetadata = new RecordingConnectorMetadata();

        CatalogRegistry catalogRegistry = buildCatalogRegistry(
                CATALOG_NAME, recordingMetadata);
        catalogRegistry.initialize();

        QueryManager qm = buildQueryManager(workerManager, tracker);
        TaskStatusReceiverContext deps = new TaskStatusReceiverContext(
                qm, tracker, workerManager,
                null, null, null, null,
                catalogRegistry, writeCommitTracker, null, null, null);
        receiver = new TaskStatusReceiver(deps);
    }

    @Test
    void writeResultNonNull_collectsCommitMetadataAndCallsFinishDistributedWrite() {
        byte[] fragment1 = {1, 2, 3};
        byte[] fragment2 = {4, 5, 6};

        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        registerTwoTasks();

        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment1));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", fragment2));

        assertThat(recordingMetadata.finishCalls).hasSize(1);
        RecordingConnectorMetadata.FinishCall call = recordingMetadata.finishCalls.get(0);
        assertThat(call.fragments()).hasSize(2);
        assertThat(call.fragments().get(0)).isEqualTo(fragment1);
        assertThat(call.fragments().get(1)).isEqualTo(fragment2);
    }

    @Test
    void writeResultNull_finishDistributedWriteNotCalled() {
        // No write context registered — simulates a pure read/compute query
        registerTwoTasks();

        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(recordingMetadata.finishCalls).isEmpty();
    }

    @Test
    void allTasksComplete_noCommitMetadataCollected_callsFinishWithEmptyFragments() {
        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        registerTwoTasks();

        // First task finishes without writeResult — no commitMetadata collected
        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));

        // Second task triggers allTasksFinished → commitDistributedWriteIfNeeded
        // → fragments empty → finishDistributedWrite called with empty list
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(recordingMetadata.finishCalls).hasSize(1);
        assertThat(recordingMetadata.finishCalls.get(0).fragments()).isEmpty();
    }

    @Test
    void queryFailure_cleansUpWriteState() {
        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");

        receiver.onTaskStatus(failedReport("t-1"));

        assertThat(writeCommitTracker.hasWriteContext(QUERY_ID)).isFalse();
        assertThat(recordingMetadata.finishCalls).isEmpty();
    }

    @Test
    void demotedLeaderIsFencedBeforeCatalogCommit() {
        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());
        // A-FENCE-01: this coordinator has been demoted (another node is now leader).
        receiver.setWriteCommitFencer(new WriteCommitFencer() {
            @Override public long currentTerm() { return 7L; }
            @Override public String coordinatorId() { return "demoted-node"; }
            @Override public boolean isActiveLeader() { return false; }
        });

        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        // A zombie leader must NOT commit to the catalog; the intent is left PENDING for the new
        // leader's WriteCommitReconciler to auto-finish (A-COMMIT-01).
        assertThat(recordingMetadata.finishCalls)
                .as("demoted leader must be fenced before finishDistributedWrite").isEmpty();
    }

    @Test
    void activeLeaderCommitProceeds() {
        byte[] fragment = {9};
        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());
        // A-FENCE-01: this coordinator is still the active leader -> commit proceeds normally.
        receiver.setWriteCommitFencer(new WriteCommitFencer() {
            @Override public long currentTerm() { return 7L; }
            @Override public String coordinatorId() { return "leader-node"; }
            @Override public boolean isActiveLeader() { return true; }
        });

        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(recordingMetadata.finishCalls)
                .as("active leader commits normally").hasSize(1);
    }

    // --- Helpers ---

    private void registerTwoTasks() {
        for (String taskId : List.of("t-1", "t-2")) {
            tracker.registerTask(QUERY_ID, taskId);
            tracker.markScheduled(QUERY_ID, taskId);
            tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
        }
    }

    private TaskStatusReport finishedWithWriteResult(String taskId, byte[] commitMetadata) {
        WriteResultDescriptor wr = new WriteResultDescriptor(
                List.of(), 100, 1024, commitMetadata);
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FINISHED",
                null, null, wr, sequenceCounter++, System.currentTimeMillis());
    }

    private TaskStatusReport finishedWithoutWriteResult(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FINISHED",
                null, null, null, sequenceCounter++, System.currentTimeMillis());
    }

    private TaskStatusReport failedReport(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FAILED",
                null, null, null, sequenceCounter++, System.currentTimeMillis());
    }

    private static QueryManager buildQueryManager(WorkerManager wm,
                                                   CoordinatorTaskTracker tracker) {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        RemoteTaskDispatcher td = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        QueryDispatcherContext qdDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                td, scheduler, wm, tracker,
                null, null, null, null, 0L);
        QueryDispatcher dispatcher = new QueryDispatcher(
                qdDeps, null, null);
        return new QueryManager(
                QueryManagerContext.minimal(dispatcher, new NoOpTracingProvider()));
    }

    private static CatalogRegistry buildCatalogRegistry(String catalogName,
                                                         ConnectorMetadata metadata) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                catalogName, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        ConnectorCatalogHandle handle = new ConnectorCatalogHandle(
                descriptor, new TestConnector(metadata));
        ConnectorHandleFactory factory = desc -> handle;
        return new CatalogRegistry(
                new ReadOnlyMetadataStore(List.of(descriptor)), factory, null, null);
    }

    private static WorkerInfo workerInfo(String workerId) {
        return new WorkerInfo(workerId, "host-1", 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                1024L * 1024 * 1024, Instant.now(), 1.0, null, 1,
                List.of(), null, List.of());
    }

    // --- Test doubles (hand-written, no mock frameworks — pangrova-rules §12) ---

    /** Records finishDistributedWrite invocations for assertion. */
    static final class RecordingConnectorMetadata implements ConnectorMetadata {

        final List<FinishCall> finishCalls =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments) {
            finishCalls.add(new FinishCall(session, outputHandle, List.copyOf(fragments)));
        }

        record FinishCall(ConnectorSession session,
                          ConnectorOutputTableHandle outputHandle,
                          List<byte[]> fragments) {}

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName n) {
            return null; // not exercised in write-commit tests
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession s,
                                                        ConnectorTableHandle t) {
            return null; // not exercised in write-commit tests
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s,
                                                           ConnectorTableHandle t) {
            return Map.of(); // not exercised in write-commit tests
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession s,
                                                   ConnectorTableHandle t) {
            return null; // not exercised in write-commit tests
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession s) {
            return List.of(); // not exercised in write-commit tests
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of(); // not exercised in write-commit tests
        }
    }

    /** Returns the recording metadata for any session. */
    static final class TestConnector implements Connector {

        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return null; // not exercised in write-commit tests
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return null; // not exercised in write-commit tests
        }
    }

    /** Marker output table handle for test wiring. */
    static final class TestOutputTableHandle implements ConnectorOutputTableHandle {}

    /** Minimal session for test wiring. */
    static final class TestConnectorSession implements ConnectorSession {

        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.of(CATALOG_NAME);
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.of("default");
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of();
        }
    }

    /**
     * Pre-populated read-only metadata store for CatalogRegistry initialization.
     * Write operations are intentional no-ops since the registry only reads
     * descriptors during initialize().
     */
    static final class ReadOnlyMetadataStore implements MetadataStore {

        private final List<CatalogDescriptor> descriptors;

        ReadOnlyMetadataStore(List<CatalogDescriptor> descriptors) {
            this.descriptors = List.copyOf(descriptors);
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return descriptors;
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { /* read-only store */ }
        @Override public void deleteCatalogDescriptor(String name) { /* read-only store */ }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return descriptors.stream().filter(d -> d.name().equals(name)).findFirst();
        }

        @Override public void saveTable(TableMetadata m) { /* read-only store */ }

        @Override
        public Optional<TableMetadata> loadTable(String c, String s, String t) {
            return Optional.empty();
        }

        @Override public void deleteTable(String c, String s, String t) { /* read-only store */ }

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) {
            return List.of();
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.empty();
        }

        @Override public void updateSyncTime(String cn, Instant st) { /* read-only store */ }
        @Override public void recordSyncHistory(SyncResult r) { /* read-only store */ }
        @Override public void saveSchemas(String cn, List<String> s) { /* read-only store */ }
    }
}
