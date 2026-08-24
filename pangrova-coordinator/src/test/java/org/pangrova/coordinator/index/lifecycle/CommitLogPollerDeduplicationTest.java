package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.metadata.index.CommitEventRequest;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.metadata.index.model.CommitLogEntry;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for F-COORD-FEAT-07 fixes:
 * <ol>
 *   <li>Leader does NOT process its own commits twice (single-path via poller)</li>
 *   <li>Failed processing leaves entries available for retry (process-first-mark-later)</li>
 * </ol>
 */
@DisplayName("CommitLogPoller Deduplication & Retry (F-COORD-FEAT-07)")
class CommitLogPollerDeduplicationTest {

    @Test
    @DisplayName("Leader processes each commit exactly once (no double-processing)")
    void leaderProcessesCommitExactlyOnce() {
        var store = new TrackingStore();
        var lease = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var leaderElection = new MaintenanceLeaderElection(lease, "coord-1");
        var processCount = new AtomicInteger(0);

        var poller = new CommitLogPoller(
                stubContext(store), leaderElection, 16,
                files -> processCount.incrementAndGet());
        try {
            // Seed a commit log entry (simulating what onCommit() persists)
            store.seedCommitLogEntry(new CommitLogEntry(
                    1L, "cat", "sch", "tbl", "snap-1",
                    "file1.parquet", "", "APPEND", Instant.now()));

            // Poll once
            poller.pollCommitLog();

            // Should be processed exactly once
            assertThat(processCount.get())
                    .as("Entry must be processed exactly once")
                    .isEqualTo(1);

            // Should be marked as processed
            assertThat(store.processedEventIds)
                    .containsExactly(1L);

            // Poll again — no more entries to process
            poller.pollCommitLog();
            assertThat(processCount.get())
                    .as("Already-processed entry must not be processed again")
                    .isEqualTo(1);
        } finally {
            poller.close();
        }
    }

    @Test
    @DisplayName("onCommit() only persists to commit log — no inline processing")
    void onCommitDoesNotProcessInline() {
        var store = new TrackingStore();
        var lease = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var leaderElection = new MaintenanceLeaderElection(lease, "coord-1");
        var processCount = new AtomicInteger(0);

        // Create handler with a poller that tracks processing
        var handler = new IndexCommitHandler(
                stubContext(store), null, leaderElection, 16, 16);
        try {
            var event = new org.pangrova.spi.connector.index.CommitEvent(
                    "cat", "sch", "tbl",
                    List.of("file1.parquet"), List.of(),
                    Instant.now(), "snap-1", CommitEventType.APPEND);

            handler.onCommit(event);

            // Commit log write should have occurred
            assertThat(store.recordedEvents)
                    .as("onCommit must persist to commit log")
                    .hasSize(1);

            // But no direct processing (processTableCommit) was invoked
            assertThat(store.findIndexesByTableCallCount.get())
                    .as("onCommit must NOT process inline — processing is poller-only")
                    .isZero();
        } finally {
            handler.close();
        }
    }

    @Test
    @DisplayName("Failed processing leaves entry available for retry (at-least-once)")
    void failedProcessingLeavesEntryForRetry() {
        var store = new TrackingStore();
        var lease = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var leaderElection = new MaintenanceLeaderElection(lease, "coord-1");
        var failOnFirstAttempt = new AtomicInteger(0);

        var poller = new CommitLogPoller(
                stubContext(store), leaderElection, 16,
                files -> {
                    int attempt = failOnFirstAttempt.incrementAndGet();
                    if (attempt == 1) {
                        throw new RuntimeException("Simulated processing failure");
                    }
                    // Second attempt succeeds
                });
        try {
            store.seedCommitLogEntry(new CommitLogEntry(
                    1L, "cat", "sch", "tbl", "snap-1",
                    "file1.parquet", "", "APPEND", Instant.now()));

            // First poll — processing fails
            poller.pollCommitLog();

            // Entry must NOT be marked as processed (it failed)
            assertThat(store.processedEventIds)
                    .as("Failed entry must not be marked as processed")
                    .isEmpty();

            // Entry is still available for retry
            assertThat(store.pollUnprocessedCommitEvents(10))
                    .as("Failed entry must remain in unprocessed queue")
                    .hasSize(1);

            // Second poll — processing succeeds
            poller.pollCommitLog();

            // Now entry is marked as processed
            assertThat(store.processedEventIds)
                    .as("Successfully processed entry must be marked")
                    .containsExactly(1L);

            // And no longer available
            assertThat(store.pollUnprocessedCommitEvents(10))
                    .as("Processed entry must not reappear")
                    .isEmpty();
        } finally {
            poller.close();
        }
    }

    @Test
    @DisplayName("Multiple entries: failure of one does not affect others")
    void partialFailureDoesNotBlockOtherEntries() {
        var store = new TrackingStore();
        var lease = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var leaderElection = new MaintenanceLeaderElection(lease, "coord-1");
        var failForEventId = new AtomicInteger(1); // fail processing for event 2 once

        var poller = new CommitLogPoller(
                stubContext(store), leaderElection, 16,
                files -> {
                    // Fail if this is the entry for "snap-2" (from event 2)
                    if ("snap-2".equals(files.snapshotId)) {
                        if (failForEventId.get() > 0) {
                            failForEventId.decrementAndGet();
                            throw new RuntimeException("Simulated failure for event 2");
                        }
                    }
                });
        try {
            store.seedCommitLogEntry(new CommitLogEntry(
                    1L, "cat", "sch", "tbl", "snap-1",
                    "file1.parquet", "", "APPEND", Instant.now()));
            store.seedCommitLogEntry(new CommitLogEntry(
                    2L, "cat", "sch", "tbl", "snap-2",
                    "file2.parquet", "", "APPEND", Instant.now()));
            store.seedCommitLogEntry(new CommitLogEntry(
                    3L, "cat", "sch", "tbl", "snap-3",
                    "file3.parquet", "", "APPEND", Instant.now()));

            // First poll — event 1 and 3 succeed, event 2 fails
            poller.pollCommitLog();

            // Events 1 and 3 should be processed, event 2 should remain
            assertThat(store.processedEventIds)
                    .as("Successful entries must be marked processed")
                    .containsExactlyInAnyOrder(1L, 3L);
            assertThat(store.pollUnprocessedCommitEvents(10))
                    .as("Failed entry 2 must remain for retry")
                    .hasSize(1)
                    .extracting(CommitLogEntry::eventId)
                    .containsExactly(2L);

            // Second poll — event 2 succeeds on retry
            poller.pollCommitLog();
            assertThat(store.processedEventIds)
                    .as("All entries should now be processed after retry")
                    .containsExactlyInAnyOrder(1L, 2L, 3L);
        } finally {
            poller.close();
        }
    }

    // ── Test infrastructure ─────────────────────────────────────────────

    private IndexCommitContext stubContext(TrackingStore store) {
        // Minimal context — we only need the metadata store for commit log operations.
        // buildCoordinator and connectorAccessorResolver are not exercised in these tests
        // (processTableCommit finds no indexes, so builds/accessor are never invoked).
        var config = new org.pangrova.indexing.config.IndexingConfig("/tmp/test", false, null, null);
        var engineRegistry = new org.pangrova.indexing.IndexEngineRegistry(List.of());
        var storageManager = new org.pangrova.indexing.storage.IndexStorageManager();
        var serializer = new org.pangrova.indexing.serde.IndexSerializer(
                org.pangrova.indexing.serde.TypeSerializerRegistry.createDefault());
        var buildCtx = new org.pangrova.indexing.build.IndexBuildContext(
                config, engineRegistry, storageManager, serializer);
        var buildCoordinator = new org.pangrova.indexing.build.IndexBuildCoordinator(buildCtx, null);
        var resolver = new org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver(null);
        return new IndexCommitContext(store, buildCoordinator, resolver);
    }

    @SuppressWarnings("NullableProblems")
    static final class TrackingStore implements IndexMetadataStore {
        final CopyOnWriteArrayList<Long> processedEventIds = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String[]> recordedEvents = new CopyOnWriteArrayList<>();
        final AtomicInteger findIndexesByTableCallCount = new AtomicInteger();
        private final CopyOnWriteArrayList<CommitLogEntry> commitLogEntries =
                new CopyOnWriteArrayList<>();

        void seedCommitLogEntry(CommitLogEntry entry) {
            commitLogEntries.add(entry);
        }

        @Override
        public void recordCommitEvent(CommitEventRequest request) {
            recordedEvents.add(new String[]{request.catalogName(), request.schemaName(),
                    request.tableName(), request.snapshotId()});
        }

        @Override
        public List<CommitLogEntry> pollUnprocessedCommitEvents(int limit) {
            var result = List.copyOf(commitLogEntries);
            return result.subList(0, Math.min(limit, result.size()));
        }

        @Override
        public void markCommitEventProcessed(long eventId) {
            processedEventIds.add(eventId);
            commitLogEntries.removeIf(e -> e.eventId() == eventId);
        }

        @Override
        public List<IndexDescriptor> findIndexesByTable(String c, String s, String t) {
            findIndexesByTableCallCount.incrementAndGet();
            return List.of();
        }

        // ── No-op implementations ──
        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String c, String s, String t, List<String> cols, boolean b) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void registerIndexFile(IndexFileMapping m) {}
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) {}
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) { return List.of(); }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(
                List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {}
        @Override public List<String> filterUnindexedFiles(
                String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) { return List.of(); }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String c, String s, String o, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) { return List.of(); }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
    }

}
