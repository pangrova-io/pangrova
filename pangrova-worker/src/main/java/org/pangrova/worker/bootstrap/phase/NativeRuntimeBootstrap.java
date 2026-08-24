package org.pangrova.worker.bootstrap.phase;

import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.cache.NativeCacheConfig;
import org.pangrova.worker.jni.NativeRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the JNI native runtime to the C++ execution engine.
 * Gracefully degrades to no-native mode if the native library is unavailable.
 */
public final class NativeRuntimeBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(NativeRuntimeBootstrap.class);

    private NativeRuntimeBootstrap() {}

    /**
     * Attempts native initialization and writes result to component refs.
     * Uses {@link NativeCacheConfig#DISABLED} as default cache configuration.
     */
    public static void initialize(WorkerBootstrapConfig config, WorkerComponentRefs refs) {
        initialize(config, refs, NativeCacheConfig.DISABLED);
    }

    /**
     * Attempts native initialization with explicit cache configuration.
     *
     * @param config      worker bootstrap configuration
     * @param refs        component refs to write initialization result
     * @param cacheConfig native cache configuration (from YAML config)
     */
    public static void initialize(WorkerBootstrapConfig config, WorkerComponentRefs refs,
                                  NativeCacheConfig cacheConfig) {
        try {
            byte[] configBytes = buildNativeInitConfigBytes(config, cacheConfig);
            // Task 148: use the shared MemoryArbitrator from refs so native and
            // Java memory accounting share a single unified budget.
            org.pangrova.resource.memory.MemoryArbitrator sharedArbitrator = refs.memoryArbitrator();
            var listener = sharedArbitrator != null
                    ? new org.pangrova.worker.memory.NativeMemoryCoordinator(
                            config.resourceConfig().memoryBudgetBytes(), sharedArbitrator)
                    : new org.pangrova.worker.memory.NativeMemoryCoordinator(
                            config.resourceConfig().memoryBudgetBytes());
            NativeRuntime.initNativeRuntime(configBytes, listener);
            refs.setNativeAvailable(true);
            LOG.info("event=native_runtime_initialized");
        } catch (UnsatisfiedLinkError | ExceptionInInitializerError | Exception e) {
            refs.setNativeAvailable(false);
            LOG.warn("event=native_runtime_unavailable reason={}", e.getMessage());
        } catch (NoClassDefFoundError e) {
            refs.setNativeAvailable(false);
            LOG.warn("event=native_runtime_unavailable reason=class_init_failed");
        }
    }

    static byte[] buildNativeInitConfigBytes(WorkerBootstrapConfig config,
                                             NativeCacheConfig cacheConfig) {
        var rc = config.resourceConfig();
        String spillDirectory = rc.spillDirectory() == null ? "" : rc.spillDirectory().trim();
        return org.pangrova.protocol.v1.NativeInitConfig.newBuilder()
                .setMaxMemoryBytes(rc.memoryBudgetBytes())
                .setIoThreadCount(rc.ioThreads())
                .setCacheEnabled(cacheConfig.enabled())
                .setRamCacheBytes(cacheConfig.memoryCacheBytes())
                .setSsdCacheBytes(cacheConfig.ssdCacheBytes())
                .setSsdCachePath(cacheConfig.ssdCachePath())
                .setSsdShards(cacheConfig.ssdShards())
                .setCheckpointIntervalBytes(cacheConfig.checkpointIntervalBytes())
                .setMemoryPressureThresholdLow(rc.pressureThresholdLow())
                .setMemoryPressureThresholdMedium(rc.pressureThresholdMedium())
                .setMemoryPressureThresholdHigh(rc.pressureThresholdHigh())
                .setMemoryPressureThresholdCritical(rc.pressureThresholdCritical())
                // worker.spill.directory is the switch for engine-level spill:
                // an unset directory leaves it disabled, which keeps the native
                // side from ever seeing spill_enabled with nowhere to spill to.
                .setSpillDirectory(spillDirectory)
                .setSpillEnabled(!spillDirectory.isEmpty())
                .build()
                .toByteArray();
    }
}
