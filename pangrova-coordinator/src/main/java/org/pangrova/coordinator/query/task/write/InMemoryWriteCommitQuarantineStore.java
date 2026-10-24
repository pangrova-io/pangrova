package org.pangrova.coordinator.query.task.write;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local quarantine store for deployments without a metadata datasource.
 * Survives neither restart nor failover; durable deployments must wire the JDBC store.
 */
public final class InMemoryWriteCommitQuarantineStore implements WriteCommitQuarantineStore {

    private final Map<String, QuarantinedWriteCommit> entries = new ConcurrentHashMap<>();

    @Override
    public boolean exists(String queryId, String commitFingerprint) {
        QuarantinedWriteCommit entry = entries.get(queryId);
        return entry != null && entry.commitFingerprint().equals(commitFingerprint);
    }

    @Override
    public void record(QuarantinedWriteCommit entry) {
        entries.putIfAbsent(entry.queryId(), entry);
    }

    @Override
    public Optional<QuarantinedWriteCommit> find(String queryId) {
        return Optional.ofNullable(entries.get(queryId));
    }
}
