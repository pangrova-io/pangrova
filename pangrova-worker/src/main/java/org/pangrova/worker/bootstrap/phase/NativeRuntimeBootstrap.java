package org.pangrova.worker.bootstrap.phase;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.cache.NativeCacheConfig;
import org.pangrova.worker.jni.NativeRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the JNI native runtime to the C++ execution engine.
 * Native execution is mandatory; initialization failures abort Worker startup.
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
        byte[] configBytes = buildNativeInitConfigBytes(config, cacheConfig);
        // Task 148: use the shared MemoryArbitrator from refs so native and
        // Java memory accounting share a single unified budget.
        org.pangrova.worker.resource.memory.MemoryArbitrator sharedArbitrator = refs.memoryArbitrator();
        // Ask the Native arbitrator to reclaim operator memory on an allocation denial.
        var spillController = new org.pangrova.worker.memory.NativeSpillController(
                NativeRuntime::nativeRequestReclaim);
        spillController.setSpillEnabled(isEngineSpillConfigured(config));
        var listener = sharedArbitrator != null
                ? new org.pangrova.worker.memory.NativeMemoryCoordinator(
                        config.resourceConfig().memoryBudgetBytes(), sharedArbitrator,
                        refs.nodeMemoryPool(), spillController)
                : new org.pangrova.worker.memory.NativeMemoryCoordinator(
                        config.resourceConfig().memoryBudgetBytes(),
                        new org.pangrova.worker.resource.memory.MemoryArbitrator(
                                config.resourceConfig().memoryBudgetBytes(), 0),
                        spillController);
        NativeRuntime.initNativeRuntime(configBytes, listener);
        refs.setNativeAvailable(true);
        LOG.info("event=native_runtime_initialized");
    }

    /**
     * Engine-level spill requires a destination directory; an unset directory
     * leaves spill disabled so native never sees spill_enabled with nowhere to
     * spill to. Shared by the init config and the Java-side spill controller so the
     * two cannot disagree.
     */
    private static boolean isEngineSpillConfigured(WorkerBootstrapConfig config) {
        return !spillDirectoryOf(config).isEmpty();
    }

    private static String spillDirectoryOf(WorkerBootstrapConfig config) {
        String directory = config.resourceConfig().spillDirectory();
        return directory == null ? "" : directory.trim();
    }

    static byte[] buildNativeInitConfigBytes(WorkerBootstrapConfig config,
                                             NativeCacheConfig cacheConfig) {
        var rc = config.resourceConfig();
        var security = config.transportSecurityConfig();
        String spillDirectory = spillDirectoryOf(config);
        var transportSecurity = org.pangrova.protocol.v2.TransportSecurityConfig.newBuilder()
                .setTlsEnabled(security.enabled())
                .setCertificateChainPath(security.certificateChainPath())
                .setPrivateKeyPath(security.privateKeyPath())
                .setRootCaPath(security.rootCaPath())
                .setTlsIdentity(security.tlsIdentity())
                .build();
        return org.pangrova.protocol.v2.RuntimeInitConfig.newBuilder()
                .setMaxMemoryBytes(rc.memoryBudgetBytes())
                .setIoThreadCount(rc.ioThreads())
                .setCacheEnabled(cacheConfig.enabled())
                .setRamCacheBytes(cacheConfig.memoryCacheBytes())
                .setSsdCacheBytes(cacheConfig.ssdCacheBytes())
                .setSsdCachePath(cacheConfig.ssdCachePath())
                .setSsdShards(cacheConfig.ssdShards())
                .setCheckpointIntervalBytes(cacheConfig.checkpointIntervalBytes())
                .setArtifactCachePath(cacheConfig.artifactCachePath())
                .setArtifactCacheBytes(cacheConfig.artifactCacheBytes())
                .setArtifactIndexMemoryBytes(cacheConfig.artifactIndexMemoryBytes())
                .setMemoryPressureThresholdLow(rc.pressureThresholdLow())
                .setMemoryPressureThresholdMedium(rc.pressureThresholdMedium())
                .setMemoryPressureThresholdHigh(rc.pressureThresholdHigh())
                .setMemoryPressureThresholdCritical(rc.pressureThresholdCritical())
                .setSpillDirectory(spillDirectory)
                .setSpillEnabled(isEngineSpillConfigured(config))
                .setShuffleRetentionBytes(rc.shuffleRetentionBytes())
                .setShuffleTransitBytes(rc.shuffleTransitBytes())
                .setShuffleSpillBytes(rc.shuffleSpillBytes())
                .setShuffleSpillFiles(rc.shuffleSpillFiles())
                .setShuffleSpillDirectory(rc.shuffleSpillDirectory() == null
                        ? "" : rc.shuffleSpillDirectory().trim())
                .setTransportSecurity(transportSecurity)
                .build()
                .toByteArray();
    }
}
