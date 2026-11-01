package org.pangrova.metadata.core.control;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local maintenance policy and history.
 *
 * <p>Policy decisions must agree across coordinators, so deployments that enable
 * maintenance use the JDBC store. This implementation serves tests and
 * metadata-less modes.</p>
 */
@ThreadSafe
public final class InMemoryTableControlMaintenanceStore implements TableControlMaintenanceStore {

    private final Map<UUID, TableMaintenancePolicy> policies = new ConcurrentHashMap<>();
    private final List<TableMaintenanceRun> runs =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override
    public Optional<TableMaintenancePolicy> findPolicy(UUID tableId) {
        return Optional.ofNullable(policies.get(tableId));
    }

    @Override
    public void savePolicy(TableMaintenancePolicy policy) {
        policies.put(policy.tableId(), policy);
    }

    @Override
    public void saveRun(TableMaintenanceRun run) {
        runs.removeIf(existing -> existing.runId().equals(run.runId()));
        runs.add(run);
    }

    @Override
    public List<TableMaintenanceRun> listRuns(UUID tableId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return runs.stream()
                .filter(run -> run.tableId().equals(tableId))
                .sorted(Comparator.comparing(TableMaintenanceRun::startedAt).reversed())
                .limit(limit)
                .toList();
    }
}
