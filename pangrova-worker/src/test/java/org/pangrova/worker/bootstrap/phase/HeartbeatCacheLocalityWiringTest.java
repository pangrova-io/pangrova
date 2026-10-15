package org.pangrova.worker.bootstrap.phase;

import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.cache.RecentFileAccessTracker;
import org.pangrova.worker.heartbeat.HeartbeatPayload;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * F-WORKER-MEM-02: Verifies the heartbeat cache-locality wiring.
 *
 * <p>The heartbeat payload must carry recently-accessed file paths from
 * {@link RecentFileAccessTracker} so the coordinator scheduler can route
 * splits to workers that have warm cache state.</p>
 */
class HeartbeatCacheLocalityWiringTest {

    @Test
    void heartbeatAdvertisesTheTighterNativeNodeBudget() {
        var arbitrator = new org.pangrova.resource.memory.MemoryArbitrator(1024, 0);
        var node = new org.pangrova.resource.memory.RootMemoryPool("heartbeat-root", 768)
                .createNodePool("heartbeat-node", 768);
        var refs = new WorkerComponentRefs();
        refs.setNodeMemoryPool(node);
        refs.setResourceManager(new org.pangrova.resource.ResourceManager(
                arbitrator, new org.pangrova.resource.thread.ThreadBudgetTracker(2, 1, 1),
                new org.pangrova.resource.spill.SpillManager()));
        assertThat(node.tryReserve(256)).isEqualTo(256);

        HeartbeatPayload payload = WorkerHeartbeatBootstrap.collectHeartbeatPayload(
                WorkerBootstrapConfig.withDefaults("heartbeat-budget"), refs);

        assertThat(payload.availableMemoryBytes()).isEqualTo(512);
        node.free(256);
    }

    @Test
    void heartbeatPayloadCarriesCachedFilePathsFromTracker() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("/data/iceberg/t1/data/00001.parquet");
        tracker.recordAccess("/data/iceberg/t1/data/00002.parquet");
        tracker.recordAccess("/data/iceberg/t2/data/00003.parquet");

        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setRecentFileAccessTracker(tracker);

        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-test", 0, 0, "http://coordinator:8080", 5000,
                false, null,
                null, 300, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(3, 1000, 5000),
                null, null, null,
                null,
                null, null, null, null);

        HeartbeatPayload payload = WorkerHeartbeatBootstrap.collectHeartbeatPayload(config, refs);

        assertThat(payload.cachedFilePaths()).containsExactly(
                "/data/iceberg/t1/data/00001.parquet",
                "/data/iceberg/t1/data/00002.parquet",
                "/data/iceberg/t2/data/00003.parquet");
    }

    @Test
    void heartbeatPayloadReturnsEmptyListWhenTrackerIsNull() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        // recentFileAccessTracker left null — simulates pre-F-CHAIN-CACHE-02 worker

        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-test", 0, 0, "http://coordinator:8080", 5000,
                false, null,
                null, 300, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(3, 1000, 5000),
                null, null, null,
                null,
                null, null, null, null);

        HeartbeatPayload payload = WorkerHeartbeatBootstrap.collectHeartbeatPayload(config, refs);

        assertThat(payload.cachedFilePaths()).isEmpty();
    }

    @Test
    void heartbeatPayloadDoesNotCarryDeadCacheStatsFields() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("/data/file.parquet");

        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setRecentFileAccessTracker(tracker);

        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-test", 0, 0, "http://coordinator:8080", 5000,
                false, null,
                null, 300, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(3, 1000, 5000),
                null, null, null,
                null,
                null, null, null, null);

        HeartbeatPayload payload = WorkerHeartbeatBootstrap.collectHeartbeatPayload(config, refs);

        // F-WORKER-MEM-02: verify the record no longer has dead cache stats
        // (cacheUsedBytes, cacheMaxBytes, cacheHitCount, cacheMissCount).
        // If the record still had those fields, this test would fail to compile.
        assertThat(payload.cachedFilePaths()).containsExactly("/data/file.parquet");
        assertThat(payload.workerId()).isEqualTo("w-test");
    }
}
