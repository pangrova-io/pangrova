package org.pangrova.worker.bootstrap.phase;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.protocol.v1.NativeInitConfig;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.cache.NativeCacheConfig;
import org.junit.jupiter.api.Test;

/**
 * Validates cache memory budget wiring: NativeCacheConfig fields are
 * propagated to the NativeInitConfig proto via buildNativeInitConfigBytes.
 */
class NativeCacheMemoryBudgetTest {

    @Test
    void cacheEnabledFlagIsPropagatedToProto() throws Exception {
        NativeCacheConfig cacheConfig = new NativeCacheConfig(
                true,
                4L * 1024 * 1024 * 1024,
                "/mnt/nvme0/cache",
                100L * 1024 * 1024 * 1024,
                8);

        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-cache");
        byte[] bytes = NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, cacheConfig);
        NativeInitConfig parsed = NativeInitConfig.parseFrom(bytes);

        assertThat(parsed.getCacheEnabled()).isTrue();
        assertThat(parsed.getRamCacheBytes()).isEqualTo(4L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCacheBytes()).isEqualTo(100L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCachePath()).isEqualTo("/mnt/nvme0/cache");
    }

    @Test
    void disabledCacheIsReflectedInProto() throws Exception {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-disabled");
        byte[] bytes = NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DISABLED);
        NativeInitConfig parsed = NativeInitConfig.parseFrom(bytes);

        assertThat(parsed.getCacheEnabled()).isFalse();
        assertThat(parsed.getRamCacheBytes()).isZero();
        assertThat(parsed.getSsdCacheBytes()).isZero();
    }

    @Test
    void defaultConfigPropagatesExpectedValues() throws Exception {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-defaults");
        byte[] bytes = NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DEFAULT);
        NativeInitConfig parsed = NativeInitConfig.parseFrom(bytes);

        assertThat(parsed.getCacheEnabled()).isTrue();
        assertThat(parsed.getRamCacheBytes()).isEqualTo(8L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCacheBytes()).isEqualTo(100L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCachePath()).isEqualTo("/mnt/nvme0/native-cache");
        // Task 158: ssdShards is now propagated (was silently dropped)
        assertThat(parsed.getSsdShards()).isEqualTo(8);
        // Task 157: checkpointIntervalBytes enables SSD warm restart
        assertThat(parsed.getCheckpointIntervalBytes())
                .isEqualTo(NativeCacheConfig.DEFAULT_CHECKPOINT_INTERVAL_BYTES);
    }

    @Test
    void customRamCacheBytesConsistentThroughProto() throws Exception {
        NativeCacheConfig cacheConfig = new NativeCacheConfig(
                true, 2L * 1024 * 1024 * 1024, "/mnt/nvme0/cache",
                50L * 1024 * 1024 * 1024, 4);

        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-custom");
        byte[] bytes = NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, cacheConfig);
        NativeInitConfig parsed = NativeInitConfig.parseFrom(bytes);

        assertThat(parsed.getRamCacheBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCacheBytes()).isEqualTo(50L * 1024 * 1024 * 1024);
        // 5-arg ctor defaults checkpointIntervalBytes to 0
        assertThat(parsed.getSsdShards()).isEqualTo(4);
        assertThat(parsed.getCheckpointIntervalBytes()).isZero();
    }

    @Test
    void ssdShardsAndCheckpointIntervalPropagatedToProto() throws Exception {
        NativeCacheConfig cacheConfig = new NativeCacheConfig(
                true,
                4L * 1024 * 1024 * 1024,
                "/mnt/nvme0/cache",
                100L * 1024 * 1024 * 1024,
                16,
                2L * 1024 * 1024 * 1024);

        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-shards");
        byte[] bytes = NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, cacheConfig);
        NativeInitConfig parsed = NativeInitConfig.parseFrom(bytes);

        assertThat(parsed.getSsdShards()).isEqualTo(16);
        assertThat(parsed.getCheckpointIntervalBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
    }

    /**
     * Regression guard: the spill fields were absent from the proto, so the native
     * side could never learn the configured directory and engine-level spill was
     * silently inert.
     */
    @Test
    void spillDirectoryPropagatedAndEnablesSpill() throws Exception {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-spill", 0, 0, null,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, resourceConfigWithSpillDir("/mnt/nvme0/spill"),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                null);

        NativeInitConfig parsed = NativeInitConfig.parseFrom(
                NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DISABLED));

        assertThat(parsed.getSpillDirectory()).isEqualTo("/mnt/nvme0/spill");
        assertThat(parsed.getSpillEnabled()).isTrue();
    }

    @Test
    void unsetSpillDirectoryLeavesSpillDisabled() throws Exception {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-no-spill");

        NativeInitConfig parsed = NativeInitConfig.parseFrom(
                NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DISABLED));

        assertThat(parsed.getSpillDirectory()).isEmpty();
        assertThat(parsed.getSpillEnabled()).isFalse();
    }

    @Test
    void blankSpillDirectoryLeavesSpillDisabled() throws Exception {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-blank-spill", 0, 0, null,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, resourceConfigWithSpillDir("   "),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                null);

        NativeInitConfig parsed = NativeInitConfig.parseFrom(
                NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DISABLED));

        assertThat(parsed.getSpillDirectory()).isEmpty();
        assertThat(parsed.getSpillEnabled()).isFalse();
    }

    private static WorkerBootstrapConfig.ResourceConfig resourceConfigWithSpillDir(String spillDir) {
        WorkerBootstrapConfig.ResourceConfig defaults = WorkerBootstrapConfig.ResourceConfig.defaults();
        return new WorkerBootstrapConfig.ResourceConfig(
                defaults.memoryBudgetBytes(), defaults.systemReservedBytes(),
                defaults.queryThreads(), defaults.ioThreads(), defaults.schedulerThreads(),
                spillDir, defaults.nodeMemoryBytes(),
                defaults.pressureThresholdLow(), defaults.pressureThresholdMedium(),
                defaults.pressureThresholdHigh(), defaults.pressureThresholdCritical());
    }
}
