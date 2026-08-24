package org.pangrova.coordinator.coordination.inmemory;

import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory write commit journal for single-node or non-HA deployments.
 *
 * <p>Provides idempotency within a single JVM lifetime but does not survive
 * process restarts. Use {@link org.pangrova.metadata.coordination.JdbcWriteCommitJournal}
 * for crash-recovery durability.</p>
 */
public final class InMemoryWriteCommitJournal implements WriteCommitJournal {

    public static final InMemoryWriteCommitJournal INSTANCE = new InMemoryWriteCommitJournal();

    private final ConcurrentHashMap<String, WriteCommitEntry> entries = new ConcurrentHashMap<>();

    @Override
    public void recordIntent(WriteCommitEntry entry) {
        entries.put(entry.queryId(), entry);
    }

    @Override
    public void markCommitted(String queryId) {
        entries.computeIfPresent(queryId, (k, existing) ->
                new WriteCommitEntry(
                        existing.queryId(),
                        existing.catalogName(),
                        existing.tableName(),
                        WriteCommitEntry.State.COMMITTED,
                        System.currentTimeMillis(),
                        existing.fragments(),
                        existing.writeMode(),
                        existing.idempotencyKey(),
                        existing.coordinatorId(),
                        existing.term()));
    }

    @Override
    public boolean isCommitted(String queryId) {
        WriteCommitEntry entry = entries.get(queryId);
        return entry != null && entry.state() == WriteCommitEntry.State.COMMITTED;
    }

    @Override
    public Optional<WriteCommitEntry> getEntry(String queryId) {
        return Optional.ofNullable(entries.get(queryId));
    }

    @Override
    public List<WriteCommitEntry> replayPendingEntries() {
        return entries.values().stream()
                .filter(e -> e.state() == WriteCommitEntry.State.PENDING)
                .sorted((a, b) -> Long.compare(a.timestampMs(), b.timestampMs()))
                .toList();
    }

    @Override
    public void purge(String queryId) {
        entries.remove(queryId);
    }
}
