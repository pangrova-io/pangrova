package org.pangrova.persistence.query.memory.transaction;

import org.pangrova.query.api.write.QuarantinedWriteCommit;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.persistence.query.api.transaction.WriteCommitQuarantineStore;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local quarantine store for deployments without a metadata datasource.
 * Survives neither restart nor failover; durable deployments must wire the JDBC store.
 */
@ThreadSafe
public final class InMemoryWriteCommitQuarantineStore implements WriteCommitQuarantineStore {

    private final Map<Intent, QuarantinedWriteCommit> entries = new ConcurrentHashMap<>();

    @Override
    public boolean exists(String queryId, String commitFingerprint) {
        return entries.containsKey(new Intent(queryId, commitFingerprint));
    }

    @Override
    public void record(QuarantinedWriteCommit entry) {
        Objects.requireNonNull(entry, "entry");
        entries.putIfAbsent(new Intent(entry.queryId(), entry.commitFingerprint()), entry);
    }

    @Override
    public Optional<QuarantinedWriteCommit> find(String queryId) {
        Objects.requireNonNull(queryId, "queryId");
        return entries.values().stream()
                .filter(entry -> entry.queryId().equals(queryId))
                .max(Comparator.comparing(QuarantinedWriteCommit::quarantinedAt));
    }

    private record Intent(String queryId, String commitFingerprint) {
        private Intent {
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(commitFingerprint, "commitFingerprint");
        }
    }
}
