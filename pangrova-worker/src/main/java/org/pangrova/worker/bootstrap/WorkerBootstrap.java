package org.pangrova.worker.bootstrap;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.bootstrap.phase.NativeRuntimeBootstrap;
import org.pangrova.worker.bootstrap.phase.WorkerHeartbeatBootstrap;
import org.pangrova.worker.cache.NativeCacheConfig;
import org.pangrova.worker.bootstrap.phase.WorkerHttpBootstrap;
import org.pangrova.worker.bootstrap.phase.WorkerObservabilitySetup;
import org.pangrova.worker.bootstrap.phase.WorkerResourceBootstrap;
import org.pangrova.worker.bootstrap.phase.WorkerTaskBootstrap;
import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import org.pangrova.worker.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the full Worker startup/shutdown sequence.
 * Delegates to sub-bootstraps: Observability → NativeRuntime → Resource → Task → CDC → Transport → HTTP → Heartbeat.
 * Shutdown runs in reverse order.
 */
public final class WorkerBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerBootstrap.class);

    private final WorkerBootstrapConfig config;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final WorkerObservabilitySetup observability = new WorkerObservabilitySetup();
    private final WorkerComponentRefs refs = new WorkerComponentRefs();

    public WorkerBootstrap(WorkerBootstrapConfig config) {
        this.config = checkNotNull(config, "config must not be null");
    }

    /**
     * Executes the ordered startup sequence.
     */
    public void start() {
        checkState(!started.get(), "WorkerBootstrap already started");
        LOG.info("event=worker_bootstrap_start workerId={}", config.workerId());
        observability.initialize(config);
        // Task 148: create the shared MemoryArbitrator BEFORE native init
        // so NativeMemoryCoordinator and WorkerResourceBootstrap share a
        // single unified memory budget (fixes dual-arbitrator split).
        var rc = config.resourceConfig();
        var tenantTracker = new org.pangrova.resource.tenant.TenantResourceTracker();
        var sharedArbitrator = new org.pangrova.resource.memory.MemoryArbitrator(
                rc.memoryBudgetBytes(), rc.systemReservedBytes(), tenantTracker);
        refs.setMemoryArbitrator(sharedArbitrator);
        refs.setTenantResourceTracker(tenantTracker);
        NativeCacheConfig nativeCacheConfig = buildNativeCacheConfig(config.cacheConfig());
        NativeRuntimeBootstrap.initialize(config, refs, nativeCacheConfig);
        loadNativeCapabilities(refs);
        NativeFunctionBootstrap.initialize(refs,
                org.pangrova.worker.jni.NativeFunctionConfig.DEFAULT);
        WorkerResourceBootstrap.startResources(config, refs);
        WorkerResourceBootstrap.startNativeMetrics(refs, observability);
        WorkerResourceBootstrap.startWorkerMetrics(refs, observability);
        WorkerTaskBootstrap.startTaskManager(config, refs, observability);
        startCdc();
        WorkerHttpBootstrap.startHttp(config, refs, observability, started);
        WorkerHeartbeatBootstrap.startHeartbeat(config, refs);
        WorkerHeartbeatBootstrap.register(config, refs);
        started.set(true);
        LOG.info("event=worker_bootstrap_completed workerId={}", config.workerId());
    }

    /**
     * Executes the ordered shutdown sequence (reverse of startup).
     */
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        LOG.info("event=worker_shutdown_start workerId={}", config.workerId());
        WorkerHeartbeatBootstrap.deregister(refs, config.workerId());
        WorkerHeartbeatBootstrap.stopHeartbeat(refs);
        stopCdc();
        WorkerShutdownSequence.drainTasks(refs.worker(), refs.taskManager());
        WorkerHttpBootstrap.stopHttp(refs);
        closeNativeWriteExecutor(refs);
        closeTaskManager(refs);
        // Native shutdown is handled by WorkerShutdownSequence.releaseResources()
        // via shutdownNative(). Calling it here would cause double-free of native
        // thread pools and memory pools (P0-BUG-2).
        WorkerResourceBootstrap.releaseResources(refs, observability);
        LOG.info("event=worker_shutdown_completed workerId={}", config.workerId());
    }

    /**
     * Loads native engine capabilities via JNI command channel.
     * Best-effort: failure is logged but does not block startup.
     */
    private static void loadNativeCapabilities(WorkerComponentRefs refs) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=native_capabilities reason=native_unavailable");
            return;
        }
        try {
            java.util.List<String> allCapabilities = new java.util.ArrayList<>();
            org.pangrova.worker.jni.NativeCapabilityLoader.loadCapabilities(
                    (relTypes, exprTypes, functions) -> {
                        allCapabilities.addAll(relTypes);
                        allCapabilities.addAll(exprTypes);
                        allCapabilities.addAll(functions);
                    });
            refs.setNativeCapabilities(allCapabilities);
            LOG.info("event=native_capabilities_loaded count={}", allCapabilities.size());
        } catch (Exception e) {
            LOG.warn("event=native_capabilities_load_failed error={}", e.getMessage());
        }
    }

    /**
     * Closes the TaskManager to release its internal thread pools
     * (completion monitor executor, task executor). Without this, those
     * threads leak in long-running JVM processes.
     */
    private static void closeTaskManager(WorkerComponentRefs refs) {
        TaskManager tm = refs.taskManager();
        if (tm != null) {
            try {
                tm.close();
                LOG.info("event=task_manager_closed");
            } catch (Exception e) {
                LOG.warn("event=task_manager_close_error error={}", e.getMessage(), e);
            }
        }
    }

    private static void closeNativeWriteExecutor(WorkerComponentRefs refs) {
        var writer = refs.nativeWriteExecutor();
        if (writer != null) {
            writer.close();
            LOG.info("event=native_write_executor_closed");
        }
    }

    /**
     * Converts the bootstrap {@link WorkerBootstrapConfig.CacheConfig} into the
     * native-layer {@link NativeCacheConfig} passed across the JNI boundary.
     */
    private static NativeCacheConfig buildNativeCacheConfig(WorkerBootstrapConfig.CacheConfig cc) {
        if (cc == null || !cc.enabled()) {
            return NativeCacheConfig.DISABLED;
        }
        return new NativeCacheConfig(
                true,
                cc.memoryCacheBytes(),
                cc.ssdCachePath(),
                cc.ssdCacheBytes(),
                cc.ssdShards(),
                cc.checkpointIntervalBytes());
    }

    private void startCdc() {
        if (!config.cdcEnabled()) {
            LOG.info("event=phase_skipped phase=CDC reason=not_enabled");
            return;
        }
        if (refs.cdcConsumer() == null) {
            LOG.warn("event=phase_skipped phase=CDC reason=no_consumer_injected");
            return;
        }
        LOG.info("event=phase_started phase=CDC");
    }

    private void stopCdc() {
        AutoCloseable cdc = refs.cdcConsumer();
        if (cdc == null) {
            return;
        }
        try {
            cdc.close();
            LOG.info("event=cdc_consumer_stopped");
        } catch (Exception e) {
            LOG.warn("event=cdc_consumer_stop_error error={}", e.getMessage(), e);
        }
    }

    // -- Public accessors --

    public boolean isStarted() { return started.get(); }

    public int getHttpPort() {
        return refs.httpServer() != null ? refs.httpServer().getPort() : -1;
    }

    public int getTransportPort() {
        return refs.resultServerPort();
    }

    public TaskManager getTaskManager() { return refs.taskManager(); }

    public NodeMemoryPool getNodeMemoryPool() { return refs.nodeMemoryPool(); }

    /**
     * Injects an externally-created CDC consumer for lifecycle management.
     * Uses AutoCloseable to avoid coupling pangrova-worker to pangrova-streaming.
     */
    public void setCdcConsumer(AutoCloseable consumer) {
        refs.setCdcConsumer(consumer);
    }

    /**
     * Injects a catalog resource resolver for the native write path.
     * Must be called before {@link #start()} if write tasks are expected.
     * The resolver is used to construct a {@link org.pangrova.worker.jni.write.NativeWriteExecutor}
     * that the native engine calls back into for write operations.
     */
    public void setCatalogResourceResolver(
            org.pangrova.worker.jni.write.CatalogResourceResolver catalogResourceResolver) {
        refs.setCatalogResourceResolver(catalogResourceResolver);
    }
}
