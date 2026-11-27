package org.pangrova.persistence.query.memory.transaction;

import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.persistence.query.api.commit.WriteCommitReceipt;
import org.pangrova.common.file.CopyFilePublication;
import org.pangrova.connector.api.commit.NativeWriteFinalization;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory write commit journal for single-node or non-HA deployments.
 *
 * <p>Provides idempotency within a single JVM lifetime but does not survive
 * process restarts. Use the JDBC backend
 * for crash-recovery durability.</p>
 */
public final class InMemoryWriteCommitJournal implements WriteCommitJournal {

    private final ConcurrentHashMap<String, WriteCommitEntry> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WriteCommitReceipt> receipts = new ConcurrentHashMap<>();
    private final java.util.Set<String> acknowledged = ConcurrentHashMap.newKeySet();

    @Override
    public boolean requiresPublication(WriteCommitEntry intent) {
        java.util.Objects.requireNonNull(intent, "intent");
        return false; // This journal cannot register a durable downstream publication consumer.
    }

    @Override
    public void recordIntent(WriteCommitEntry entry) {
        java.util.Objects.requireNonNull(entry, "entry");
        if (entry.state() != WriteCommitState.PENDING) {
            throw new IllegalArgumentException("Only PENDING write intents may be recorded");
        }
        if (entry.copyFile() != null) {
            throw new IllegalStateException("COPY requires a durable PostgreSQL write journal");
        }
        entries.compute(entry.queryId(), (ignored, existing) -> {
            if (existing == null) {
                if (receipts.containsKey(entry.queryId())) {
                    throw new IllegalStateException("Cannot re-admit a retired write intent: " + entry.queryId());
                }
                return entry;
            }
            if (!existing.commitFingerprint().equals(entry.commitFingerprint())
                    || existing.state() == WriteCommitState.ABORTED) {
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
                if (isCommitted(queryId)) return null;
                throw new IllegalStateException("Cannot commit a missing write intent: " + queryId);
            }
            if (existing.state() == WriteCommitState.ABORTED) {
                throw new IllegalStateException("Cannot commit an aborted write intent: " + queryId);
            }
            var committed = existing.state() == WriteCommitState.COMMITTED ? existing
                    : existing.withState(WriteCommitState.COMMITTED, System.currentTimeMillis());
            retainReceipt(committed);
            return committed;
        });
    }

    @Override
    public void recordPublication(String queryId, String fingerprint, NativeWriteFinalization publication) {
        entries.compute(queryId, (ignored, existing) -> {
            if (existing == null || !existing.commitFingerprint().equals(fingerprint)) {
                throw new IllegalStateException("Publication does not match a durable write intent");
            }
            if (existing.state() == WriteCommitState.COMMITTED && existing.publication() == null) {
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
        return getReceipt(queryId).filter(receipt -> receipt.state() == WriteCommitState.COMMITTED).isPresent();
    }

    @Override
    public Optional<WriteCommitReceipt> getReceipt(String queryId) {
        return Optional.ofNullable(receipts.get(queryId));
    }

    private void retainReceipt(WriteCommitEntry entry) {
        receipts.putIfAbsent(entry.queryId(), new WriteCommitReceipt(entry.queryId(), entry.tenantId(),
                entry.catalogName(), entry.tableName(), entry.writeMode(), entry.baseSnapshotId(),
                entry.commitFingerprint(), entry.state(), entry.timestampMs(), entry.copyFile() != null,
                entry.cdcSource() != null, entry.publication()));
    }

    @Override
    public Optional<WriteCommitEntry> getEntry(String queryId) {
        return Optional.ofNullable(entries.get(queryId));
    }

    @Override
    public Optional<WriteCommitEntry> findCdcCheckpoint(String tenantId, String streamId, String sourceEpoch) {
        return entries.values().stream()
                .filter(entry -> entry.cdcSource() != null && entry.tenantId().equals(tenantId)
                        && entry.cdcSource().streamId().equals(streamId)
                        && entry.cdcSource().sourceEpoch().equals(sourceEpoch))
                .filter(entry -> entry.state() == WriteCommitState.PENDING
                        || entry.state() == WriteCommitState.COMMITTED)
                .max(java.util.Comparator.<WriteCommitEntry, Boolean>comparing(
                        entry -> entry.state() == WriteCommitState.PENDING)
                        .thenComparingLong(WriteCommitEntry::timestampMs)
                        .thenComparing(WriteCommitEntry::queryId));
    }

    @Override
    public Optional<WriteCommitEntry> findByFingerprint(String fingerprint) {
        return entries.values().stream().filter(entry -> entry.commitFingerprint().equals(fingerprint)).findFirst();
    }

    @Override
    public boolean isDeferredWorkAcknowledged(String fingerprint) {
        var entry = findByFingerprint(fingerprint).orElseThrow(() ->
                new IllegalStateException("Deferred status requires its exact COMMITTED write anchor"));
        if (entry.state() != WriteCommitState.COMMITTED) {
            throw new IllegalStateException("Deferred status requires a committed write");
        }
        return acknowledged.contains(fingerprint);
    }

    @Override
    public void acknowledgeNoDeferredWork(String fingerprint) {
        var entry = findByFingerprint(fingerprint).orElseThrow(() ->
                new IllegalStateException("Deferred work requires its original committed intent"));
        if (entry.state() != WriteCommitState.COMMITTED
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
                .filter(e -> e.state() == WriteCommitState.PENDING)
                .sorted((a, b) -> Long.compare(a.timestampMs(), b.timestampMs()))
                .toList();
    }

    @Override
    public void markAborted(String queryId, String fingerprint) {
        java.util.Objects.requireNonNull(queryId, "queryId");
        java.util.Objects.requireNonNull(fingerprint, "fingerprint");
        entries.compute(queryId, (id, entry) -> {
            if (entry == null || entry.state() == WriteCommitState.COMMITTED
                    || !entry.commitFingerprint().equals(fingerprint) || entry.publication() != null) {
                throw new IllegalStateException("Abort does not match an exact unpublished pending or aborted intent");
            }
            var aborted = entry.state() == WriteCommitState.ABORTED ? entry
                    : entry.withState(WriteCommitState.ABORTED, System.currentTimeMillis());
            retainReceipt(aborted);
            return aborted;
        });
    }

    @Override
    public void purge(String queryId) {
        entries.computeIfPresent(queryId, (id, entry) -> {
            if (entry.state() == WriteCommitState.COMMITTED && acknowledged.remove(entry.commitFingerprint())) {
                return null;
            }
            return entry;
        });
    }
}
