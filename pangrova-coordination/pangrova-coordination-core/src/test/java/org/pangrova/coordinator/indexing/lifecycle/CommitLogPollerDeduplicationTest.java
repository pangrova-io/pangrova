package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.store.CommitEventRequest;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.CommitLogEntry;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @DisplayName("Stable leader does not process an acknowledged commit twice")
    void leaderProcessesCommitExactlyOnce() {
        var store = new TrackingStore();
        var lease = new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease();
        var leaderElection = new MaintenanceLeaderElection(lease, "coord-1");
        var processCount = new AtomicInteger(0);

        var poller = new CommitLogPoller(
                stubContext(store), leaderElection, 16,
                files -> processCount.incrementAndGet());
        try {
            // Seed a commit log entry (simulating what onCommit() persists)
            store.seedCommitLogEntry(new CommitLogEntry(
                    1L, "tenant-a", "cat", "sch", "tbl", "snap-1",
                    "[\"file1.parquet\"]", "[]", "APPEND", Instant.now(), null));

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
        var lease = new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease();
        var leaderElection = new MaintenanceLeaderElection(lease, "coord-1");
        var processCount = new AtomicInteger(0);

        // Create handler with a poller that tracks processing
        var handler = new IndexCommitHandler(
                stubContext(store), null, leaderElection, 16);
        try {
            var event = new org.pangrova.spi.connector.index.CommitEvent(
                    "tenant-a", "cat", "sch", "tbl",
                    List.of("file1.parquet"), List.of(),
                    Instant.now(), "snap-1", CommitEventType.APPEND, "a".repeat(64));

            handler.onCommit(event);

            // Commit log write should have occurred
            assertThat(store.recordedEvents)
                    .as("onCommit must persist to commit log")
                    .hasSize(1);
            assertThat(CommitFileListCodec.decode(
                    store.recordedEvents.get(0).addedFilesJson()))
                    .containsExactly("file1.parquet");
            assertThat(store.recordedEvents.get(0).commitFingerprint())
                    .isEqualTo("a".repeat(64));

            // But no direct processing (processTableCommit) was invoked
            assertThat(store.findIndexesByTableCallCount.get())
                    .as("onCommit must NOT process inline — processing is poller-only")
                    .isZero();
        } finally {
            handler.close();
        }
    }

    @Test
    @DisplayName("Commit-log persistence failures propagate to the connector")
    void commitLogWriteFailureIsNotAcknowledged() {
        var store = new TrackingStore();
        store.failWrites = true;
        var handler = new IndexCommitHandler(stubContext(store), null, 16);
        try {
            var event = new org.pangrova.spi.connector.index.CommitEvent(
                    "tenant-a", "cat", "sch", "tbl",
                    List.of("s3://bucket/path,with,commas.parquet"), List.of(),
                    Instant.now(), "snap-1", CommitEventType.APPEND, null);

            assertThatThrownBy(() -> handler.onCommit(event))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("commit-log unavailable");
        } finally {
            handler.close();
        }
    }

    @Test
    void storeWithoutCommitLogImplementationFailsInsteadOfDroppingEvent() {
        var store = new IndexCommitHandlerStatisticsTest.RecordingMetadataStore();
        var handler = new IndexCommitHandler(stubContext(store), null, 16);
        try {
            var event = new org.pangrova.spi.connector.index.CommitEvent(
                    "tenant-a", "cat", "sch", "tbl", List.of(), List.of(),
                    Instant.now(), "snap-1", CommitEventType.APPEND, "b".repeat(64));
            assertThatThrownBy(() -> handler.onCommit(event))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("durable commit events");
        } finally {
            handler.close();
        }
    }

    @Test
    @DisplayName("File-list JSON preserves paths containing commas")
    void fileListCodecIsLossless() {
        List<String> paths = List.of(
                "s3://bucket/path,with,commas.parquet", "s3://bucket/\"quoted\".parquet");

        assertThat(CommitFileListCodec.decode(CommitFileListCodec.encode(paths)))
                .containsExactlyElementsOf(paths);
        assertThatThrownBy(() -> CommitFileListCodec.decode("file1.parquet,file2.parquet"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Failed processing leaves entry available for retry (at-least-once)")
    void failedProcessingLeavesEntryForRetry() {
        var store = new TrackingStore();
        var lease = new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease();
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
                    1L, "tenant-a", "cat", "sch", "tbl", "snap-1",
                    "[\"file1.parquet\"]", "[]", "APPEND", Instant.now(), null));

            // First poll — processing fails
            poller.pollCommitLog();

            // Entry must NOT be marked as processed (it failed)
            assertThat(store.processedEventIds)
                    .as("Failed entry must not be marked as processed")
                    .isEmpty();

            // Entry is still available for retry
            assertThat(store.pollUnprocessedCommitEvents(10, 1L))
                    .as("Failed entry must remain in unprocessed queue")
                    .hasSize(1);

            // Second poll — processing succeeds
            poller.pollCommitLog();

            // Now entry is marked as processed
            assertThat(store.processedEventIds)
                    .as("Successfully processed entry must be marked")
                    .containsExactly(1L);

            // And no longer available
            assertThat(store.pollUnprocessedCommitEvents(10, 1L))
                    .as("Processed entry must not reappear")
                    .isEmpty();
        } finally {
            poller.close();
        }
    }

    @Test
    @DisplayName("Failure blocks later events for the same table until retry")
    void failurePreservesPerTableCommitOrder() {
        var store = new TrackingStore();
        var lease = new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease();
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
                    1L, "tenant-a", "cat", "sch", "tbl", "snap-1",
                    "[\"file1.parquet\"]", "[]", "APPEND", Instant.now(), null));
            store.seedCommitLogEntry(new CommitLogEntry(
                    2L, "tenant-a", "cat", "sch", "tbl", "snap-2",
                    "[\"file2.parquet\"]", "[]", "APPEND", Instant.now(), null));
            store.seedCommitLogEntry(new CommitLogEntry(
                    3L, "tenant-a", "cat", "sch", "tbl", "snap-3",
                    "[\"file3.parquet\"]", "[]", "APPEND", Instant.now(), null));

            // First poll — event 1 succeeds, event 2 fails, event 3 is held back.
            poller.pollCommitLog();

            assertThat(store.processedEventIds)
                    .as("A later snapshot must not overtake a failed earlier snapshot")
                    .containsExactly(1L);
            assertThat(store.pollUnprocessedCommitEvents(10, 1L))
                    .as("Failed event and later same-table event remain for retry")
                    .hasSize(2)
                    .extracting(CommitLogEntry::eventId)
                    .containsExactly(2L, 3L);

            // Second poll — event 2 succeeds on retry
            poller.pollCommitLog();
            assertThat(store.processedEventIds)
                    .as("All entries should now be processed after retry")
                    .containsExactlyInAnyOrder(1L, 2L, 3L);
        } finally {
            poller.close();
        }
    }

    @Test
    void failedTableDoesNotStarveAnotherTableOutsideItsBacklog() {
        var store = new TrackingStore();
        var leaderElection = new MaintenanceLeaderElection(
                new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease(),
                "coord-1");
        Instant first = Instant.parse("2026-09-05T00:00:00Z");
        for (int i = 0; i < 4; i++) {
            store.seedCommitLogEntry(new CommitLogEntry(
                    i + 1L, "tenant-a", "cat", "sch", "blocked", "snap-" + i,
                    "[]", "[]", "APPEND", first.plusSeconds(i), null));
        }
        store.seedCommitLogEntry(new CommitLogEntry(
                5L, "tenant-a", "cat", "sch", "healthy", "snap-healthy",
                "[]", "[]", "APPEND", first.plusSeconds(10), null));
        var poller = new CommitLogPoller(
                stubContext(store), leaderElection, 2,
                files -> {
                    if ("blocked".equals(files.table)) {
                        throw new IllegalStateException("blocked table unavailable");
                    }
                });
        try {
            poller.pollCommitLog();
            assertThat(store.processedEventIds).containsExactly(5L);
            assertThat(store.pollUnprocessedCommitEvents(10, 1L))
                    .extracting(CommitLogEntry::eventId)
                    .contains(1L, 2L, 3L, 4L);
        } finally {
            poller.close();
        }
    }

    @Test
    void lostLeaseStopsLaterBatchEffectsEvenForAnotherTable() {
        var store = new TrackingStore();
        var leaderElection = new MaintenanceLeaderElection(
                new org.pangrova.persistence.memory.coordination.InMemoryDistributedLease(),
                "coord-1");
        Instant now = Instant.parse("2026-09-05T00:00:00Z");
        store.seedCommitLogEntry(new CommitLogEntry(
                1L, "tenant-a", "cat", "sch", "first", "snap-1",
                "[]", "[]", "APPEND", now, null));
        store.seedCommitLogEntry(new CommitLogEntry(
                2L, "tenant-a", "cat", "sch", "second", "snap-2",
                "[]", "[]", "APPEND", now.plusSeconds(1), null));
        var effects = new AtomicInteger();
        var poller = new CommitLogPoller(
                stubContext(store), leaderElection, 2,
                files -> {
                    if (effects.incrementAndGet() == 1) {
                        assertThat(leaderElection.release()).isTrue();
                    }
                });
        try {
            poller.pollCommitLog();
            assertThat(effects.get()).isEqualTo(1);
            assertThat(store.processedEventIds).isEmpty();
            assertThat(store.pollUnprocessedCommitEvents(2, 2L))
                    .extracting(CommitLogEntry::eventId).containsExactly(1L, 2L);
        } finally {
            poller.close();
        }
    }

    // ── Test infrastructure ─────────────────────────────────────────────

    private IndexCommitContext stubContext(IndexMetadataStore store) {
        // Minimal context — we only need the metadata store for commit log operations.
        // buildCoordinator and connectorAccessorResolver are not exercised in these tests
        // (processTableCommit finds no indexes, so builds/accessor are never invoked).
        var config = new org.pangrova.indexing.config.IndexingConfig("/tmp/test", false, null, null);
        var engineRegistry = new org.pangrova.indexing.registry.IndexEngineRegistry(List.of());
        var storageManager = new org.pangrova.indexing.storage.IndexStorageManager();
        var serializer = new org.pangrova.indexing.serde.IndexSerializer(
                org.pangrova.indexing.assembly.serde.DefaultTypeSerializers.createRegistry());
        var buildCtx = new org.pangrova.indexing.build.IndexBuildContext(
                config, engineRegistry, storageManager, serializer);
        var buildCoordinator = new org.pangrova.indexing.build.IndexBuildCoordinator(buildCtx, null);
        var resolver = new org.pangrova.coordinator.indexing.ddl.ConnectorAccessorResolver(null);
        return new IndexCommitContext(
                store,
                new org.pangrova.coordinator.indexing.ddl.IndexBuildServices(
                        buildCoordinator, null),
                resolver);
    }

    @SuppressWarnings("NullableProblems")
    static final class TrackingStore implements IndexMetadataStore {
        final CopyOnWriteArrayList<Long> processedEventIds = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<CommitEventRequest> recordedEvents = new CopyOnWriteArrayList<>();
        final AtomicInteger findIndexesByTableCallCount = new AtomicInteger();
        volatile boolean failWrites;
        private final CopyOnWriteArrayList<CommitLogEntry> commitLogEntries =
                new CopyOnWriteArrayList<>();

        void seedCommitLogEntry(CommitLogEntry entry) {
            commitLogEntries.add(entry);
        }

        @Override
        public void recordCommitEvent(CommitEventRequest request) {
            if (failWrites) {
                throw new IllegalStateException("commit-log unavailable");
            }
            recordedEvents.add(request);
        }

        @Override
        public List<CommitLogEntry> pollUnprocessedCommitEvents(int limit, long leaseEpoch) {
            var result = new ArrayList<>(commitLogEntries);
            result.sort(Comparator.comparing(CommitLogEntry::createdAt)
                    .thenComparingLong(CommitLogEntry::eventId));
            Map<String, Integer> tablePositions = new HashMap<>();
            Map<Long, Integer> positionsByEvent = new HashMap<>();
            for (CommitLogEntry entry : result) {
                String key = entry.tenantId() + ':' + entry.catalogName() + ':'
                        + entry.schemaName() + ':' + entry.tableName();
                positionsByEvent.put(entry.eventId(),
                        tablePositions.merge(key, 1, Integer::sum));
            }
            result.sort(Comparator.comparingInt(
                            (CommitLogEntry entry) -> positionsByEvent.get(entry.eventId()))
                    .thenComparing(CommitLogEntry::createdAt)
                    .thenComparingLong(CommitLogEntry::eventId));
            return result.subList(0, Math.min(limit, result.size()));
        }

        @Override
        public void markCommitEventProcessed(long eventId, long leaseEpoch) {
            processedEventIds.add(eventId);
            commitLogEntries.removeIf(e -> e.eventId() == eventId);
        }

        @Override
        public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) {
            findIndexesByTableCallCount.incrementAndGet();
            return List.of();
        }

        // ── No-op implementations ──
        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.indexing.store.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.indexing.store.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.indexing.store.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean b) { return List.of(); }
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
                String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String tenantId, String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String tenantId, String c, String s, String o, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) { return List.of(); }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
    }

}
