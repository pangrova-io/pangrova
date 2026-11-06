package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Recovers indexes stuck in BUILDING state after a Coordinator crash.
 *
 * <p>Finds indexes that have been in BUILDING state longer than
 * {@code buildTimeout × stuckDetectionMultiplier} and transitions them to
 * STALE so the consistency checker will trigger a rebuild.</p>
 *
 * <p>In multi-coordinator deployments, uses {@link MaintenanceLeaderElection}
 * to ensure only one coordinator runs recovery at a time.</p>
 */
public final class StuckBuildRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(StuckBuildRecovery.class);

    private final IndexMetadataStore metadataStore;
    private final Duration buildTimeout;
    private final int stuckDetectionMultiplier;
    private final MaintenanceLeaderElection leaderElection;

    /**
     * Creates a recovery instance with leader election for distributed coordination.
     */
    public StuckBuildRecovery(
            IndexMetadataStore metadataStore,
            Duration buildTimeout,
            int stuckDetectionMultiplier,
            MaintenanceLeaderElection leaderElection) {
        this.metadataStore = metadataStore;
        this.buildTimeout = buildTimeout;
        this.stuckDetectionMultiplier = stuckDetectionMultiplier;
        this.leaderElection = leaderElection;
    }

    /**
     * Backward-compatible constructor without leader election.
     */
    public StuckBuildRecovery(
            IndexMetadataStore metadataStore,
            Duration buildTimeout,
            int stuckDetectionMultiplier) {
        this(metadataStore, buildTimeout, stuckDetectionMultiplier, null);
    }

    /**
     * Finds BUILDING indexes exceeding the stuck threshold and transitions
     * them to STALE with a warning log. Acquires leader lease first in
     * multi-coordinator deployments; skips if another coordinator is leader.
     *
     * @return number of indexes recovered
     */
    public int recoverStuckBuilds() {
        if (leaderElection != null && !leaderElection.tryAcquireOrRenew()) {
            LOG.debug("event=stuck_recovery_skip reason=not_leader");
            return 0;
        }
        Instant cutoff = computeStuckCutoff();
        List<IndexDescriptor> buildingIndexes = metadataStore.findIndexesByState(IndexState.BUILDING);
        int recovered = recoverStuckIndexes(buildingIndexes, cutoff);
        if (recovered > 0) {
            LOG.info("event=stuck_build_recovery_completed recoveredCount={}", recovered);
        }
        return recovered;
    }

    private Instant computeStuckCutoff() {
        long stuckThresholdMillis = buildTimeout.toMillis() * stuckDetectionMultiplier;
        return Instant.now().minusMillis(stuckThresholdMillis);
    }

    private int recoverStuckIndexes(List<IndexDescriptor> indexes, Instant cutoff) {
        int recovered = 0;
        for (IndexDescriptor idx : indexes) {
            if (idx.updatedAt().isBefore(cutoff) && tryRecoverIndex(idx)) {
                recovered++;
            }
        }
        return recovered;
    }

    private boolean tryRecoverIndex(IndexDescriptor idx) {
        boolean updated = metadataStore.updateIndexState(
                idx.indexId(), IndexState.STALE.name(), Instant.now(), idx.updatedAt());
        if (!updated) {
            LOG.debug("event=stuck_build_recovery_skipped indexId={} reason=concurrent_update",
                    idx.indexId());
            return false;
        }
        long stuckDurationMs = Instant.now().toEpochMilli() - idx.updatedAt().toEpochMilli();
        LOG.warn("event=stuck_build_recovered indexId={} indexName={} "
                        + "table={}.{}.{} stuckDurationMs={}",
                idx.indexId(), idx.indexName(),
                idx.catalogName(), idx.schemaName(), idx.tableName(),
                stuckDurationMs);
        return true;
    }
}
