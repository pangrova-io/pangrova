package org.pangrova.shuffle.spool;

import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.shuffle.api.ShuffleCommitResult;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local-spill {@link ShuffleLifecycleManager} — purely local, no remote shuffle service (ADR-002 D4).
 *
 * <p>Since local-spill data lives on the worker's local filesystem, there is no external
 * shuffle service to register with. The lifecycle manager simply tracks registrations for
 * cleanup purposes and delegates deletion to the underlying {@link SpoolStore}.
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} for registration tracking.
 */
final class LocalSpillLifecycleManager implements ShuffleLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSpillLifecycleManager.class);

    private final SpoolStore spoolStore;
    private final ConcurrentHashMap<String, Integer> registrations = new ConcurrentHashMap<>();

    LocalSpillLifecycleManager(SpoolStore spoolStore) {
        this.spoolStore = spoolStore;
    }

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        String key = registration.queryId() + ":" + registration.stageId();
        registrations.put(key, registration.numPartitions());
        LOG.debug("event=local_spill_shuffle_registered queryId={} stageId={} partitions={}",
                registration.queryId(), registration.stageId(), registration.numPartitions());
        // Local handle — no external shuffle server endpoints.
        return ShuffleHandle.LOCAL;
    }

    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        // For local-spill, data is committed per-batch via SpoolWriter.commit().
        // Stage-level commit is a no-op — data is already readable.
        LOG.debug("event=local_spill_shuffle_committed queryId={} stageId={}", queryId, stageId);
        return ShuffleCommitResult.EMPTY;
    }

    @Override
    public void cleanupShuffle(String queryId) {
        // Remove all spooled data for this query.
        try {
            spoolStore.delete(queryId);
        } catch (Exception e) {
            LOG.warn("event=local_spill_cleanup_failed queryId={} error={}", queryId, e.getMessage());
        }
        registrations.keySet().removeIf(k -> k.startsWith(queryId + ":"));
        LOG.debug("event=local_spill_shuffle_cleaned queryId={}", queryId);
    }
}
