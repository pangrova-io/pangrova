package org.pangrova.coordinator.coordination.inmemory;

import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.pangrova.spi.coordination.CopyFilePublication;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory write commit journal for single-node or non-HA deployments.
 *
 * <p>Provides idempotency within a single JVM lifetime but does not survive
 * process restarts. Use {@link org.pangrova.query.transaction.JdbcWriteCommitJournal}
 * for crash-recovery durability.</p>
 */
public final class InMemoryWriteCommitJournal implements WriteCommitJournal {

    public static final InMemoryWriteCommitJournal INSTANCE = new InMemoryWriteCommitJournal();

    private final ConcurrentHashMap<String, WriteCommitEntry> entries = new ConcurrentHashMap<>();
    private final java.util.Set<String> acknowledged = ConcurrentHashMap.newKeySet();

    @Override
    public boolean requiresPublication(WriteCommitEntry intent) {
        java.util.Objects.requireNonNull(intent, "intent");
        return false; // This journal cannot register a durable downstream publication consumer.
    }

    @Override
    public void recordIntent(WriteCommitEntry entry) {
        if (entry.copyFile() != null) {
            throw new IllegalStateException("COPY requires a durable PostgreSQL write journal");
        }
        entries.compute(entry.queryId(), (ignored, existing) -> {
            if (existing == null) {
                return entry;
            }
            if (!existing.commitFingerprint().equals(entry.commitFingerprint())
                    || existing.state() == WriteCommitEntry.State.ABORTED) {
                throw new IllegalStateException(
                        "Conflicting write commit intent for queryId=" + entry.queryId());
            }
            return existing;
        });
    }

    @Override
    public void markCommitted(String queryId) {
        entries.compute(queryId, (key, existing) -> {
            if (existing == null) {
                throw new IllegalStateException("Cannot commit a missing write intent: " + queryId);
            }
            return existing.withState(WriteCommitEntry.State.COMMITTED, System.currentTimeMillis());
        });
    }

    @Override
    public void recordPublication(String queryId, String fingerprint, NativeWriteFinalization publication) {
        entries.compute(queryId, (ignored, existing) -> {
            if (existing == null || !existing.commitFingerprint().equals(fingerprint)) {
                throw new IllegalStateException("Publication does not match a durable write intent");
            }
            if (existing.state() == WriteCommitEntry.State.COMMITTED && existing.publication() == null) {
                throw new IllegalStateException("Cannot add publication evidence after catalog commit");
            }
            return existing.withPublication(publication);
        });
    }

    @Override
    public void recordCopyFilePublication(WriteCommitEntry entry, CopyFilePublication publication) {
        throw new IllegalStateException("COPY requires a durable PostgreSQL publication store");
    }

    @Override
    public Optional<CopyFilePublication> findCopyFilePublication(WriteCommitEntry entry) {
        throw new IllegalStateException("COPY requires a durable PostgreSQL publication store");
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
    public Optional<WriteCommitEntry> findByFingerprint(String fingerprint) {
        return entries.values().stream().filter(entry -> entry.commitFingerprint().equals(fingerprint)).findFirst();
    }

    @Override
    public boolean isDeferredWorkAcknowledged(String fingerprint) {
        var entry = findByFingerprint(fingerprint).orElseThrow(() ->
                new IllegalStateException("Deferred status requires its exact COMMITTED write anchor"));
        if (entry.state() != WriteCommitEntry.State.COMMITTED) {
            throw new IllegalStateException("Deferred status requires a committed write");
        }
        return acknowledged.contains(fingerprint);
    }

    @Override
    public void acknowledgeNoDeferredWork(String fingerprint) {
        var entry = findByFingerprint(fingerprint).orElseThrow(() ->
                new IllegalStateException("Deferred work requires its original committed intent"));
        if (entry.state() != WriteCommitEntry.State.COMMITTED
                || entry.authorizationReceipt().declaration().hasGeneratedColumns()) {
            throw new IllegalStateException("No-work acknowledgement requires a COMMITTED absence declaration");
        }
        acknowledged.add(fingerprint);
    }

    @Override
    public List<WriteCommitEntry> replayPendingEntries(
            java.util.Set<String> executionIds) {
        return entries.values().stream()
                .filter(e -> executionIds.contains(e.queryId()))
                .filter(e -> e.state() == WriteCommitEntry.State.PENDING)
                .sorted((a, b) -> Long.compare(a.timestampMs(), b.timestampMs()))
                .toList();
    }

    @Override
    public void markAborted(String queryId, String fingerprint) {
        java.util.Objects.requireNonNull(queryId, "queryId");
        java.util.Objects.requireNonNull(fingerprint, "fingerprint");
        entries.compute(queryId, (id, entry) -> {
            if (entry == null || entry.state() == WriteCommitEntry.State.COMMITTED
                    || !entry.commitFingerprint().equals(fingerprint) || entry.publication() != null) {
                throw new IllegalStateException("Abort does not match an exact unpublished pending or aborted intent");
            }
            return entry.withState(WriteCommitEntry.State.ABORTED, System.currentTimeMillis());
        });
    }

    @Override
    public void purge(String queryId) {
        entries.computeIfPresent(queryId, (id, entry) -> {
            if (entry.state() == WriteCommitEntry.State.COMMITTED && acknowledged.remove(entry.commitFingerprint())) {
                return null;
            }
            return entry;
        });
    }
}
