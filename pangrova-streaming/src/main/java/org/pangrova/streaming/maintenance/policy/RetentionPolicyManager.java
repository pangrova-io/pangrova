package org.pangrova.streaming.maintenance.policy;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-table data retention policy manager.
 *
 * <p>Delegates snapshot expiration to {@link TableMaintenanceSpi#expireSnapshotsAsync}
 * for each table that has a registered {@link RetentionPolicy}.</p>
 *
 * <p>Thread safety: all mutable state is in a {@link ConcurrentHashMap};
 * {@code enforceRetention} and {@code enforceAllRetention} are idempotent
 * and safe to call concurrently.</p>
 */
@ThreadSafe
public class RetentionPolicyManager {

    private static final Logger LOG = LoggerFactory.getLogger(RetentionPolicyManager.class);

    private final TableMaintenanceSpi maintenance;
    private final Map<String, RetentionPolicy> policies = new ConcurrentHashMap<>();

    public RetentionPolicyManager(TableMaintenanceSpi maintenance) {
        this.maintenance = maintenance;
    }

    public void setPolicy(String tableName, RetentionPolicy policy) {
        policies.put(tableName, policy);
        LOG.info("event=retention_policy_set table={} action={} period={} column={}",
            tableName, policy.action(), policy.retentionPeriod(), policy.retentionColumn());
    }

    public void removePolicy(String tableName) {
        policies.remove(tableName);
    }

    public RetentionPolicy getPolicy(String tableName) {
        return policies.get(tableName);
    }

    /**
     * Enforce retention for a single table. Idempotent — safe to call repeatedly.
     *
     * <p>If no policy is registered for the table, this is a no-op.
     * Calls {@link TableMaintenanceSpi#expireSnapshotsAsync} with the policy's
     * retention period and logs the outcome.</p>
     *
     * @return future that completes with the expiration result, or a completed
     *         future with {@code null} if no policy exists
     */
    public CompletableFuture<SnapshotExpirationResult> enforceRetention(String tableName) {
        RetentionPolicy policy = policies.get(tableName);
        if (policy == null) {
            LOG.debug("event=retention_skip table={} reason=no_policy", tableName);
            return CompletableFuture.completedFuture(null);
        }

        LOG.info("event=retention_enforce_start table={} action={} period={}",
            tableName, policy.action(), policy.retentionPeriod());

        return maintenance.expireSnapshotsAsync(tableName, policy.retentionPeriod())
            .whenComplete((result, error) -> {
                if (error != null) {
                    LOG.warn("event=retention_enforce_failed table={} error={}",
                        tableName, error.getMessage(), error);
                } else {
                    LOG.info("event=retention_enforce_done table={} snapshots_expired={}",
                        tableName, result.snapshotsExpired());
                }
            });
    }

    /**
     * Enforce retention for all tables that have a registered policy.
     *
     * @return future that completes when all per-table enforcements finish
     */
    public CompletableFuture<Void> enforceAllRetention() {
        List<String> tables = getTablesWithRetentionPolicy();
        if (tables.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<?>[] futures = tables.stream()
            .map(this::enforceRetention)
            .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }

    public List<String> getTablesWithRetentionPolicy() {
        return List.copyOf(policies.keySet());
    }
}
