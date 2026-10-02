package org.pangrova.worker.bootstrap;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;

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
 * Delegates to sub-bootstraps: Observability → Resource → NativeRuntime → Task → CDC → Transport → HTTP → Heartbeat.
 * Shutdown runs in reverse order.
 */
public final class WorkerBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerBootstrap.class);

    private final WorkerBootstrapConfig config;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final CountDownLatch shutdownCompleted = new CountDownLatch(1);
    private final WorkerObservabilitySetup observability = new WorkerObservabilitySetup();
    private final WorkerComponentRefs refs = new WorkerComponentRefs();

    public WorkerBootstrap(WorkerBootstrapConfig config) {
        this.config = checkNotNull(config, "config must not be null");
    }

    /** Installs the production Worker-local connector plugin runtime. */
    public void installProductionCatalogRuntime() {
        checkState(!started.get(), "Catalog runtime must be installed before start");
        refs.setCatalogResourceResolver(
                org.pangrova.worker.jni.write.WorkerCatalogRuntime.discover(
                        config.coordinatorUrl(), config.internalAuthSecret(),
                        Thread.currentThread().getContextClassLoader()));
    }

    /**
     * Executes the ordered startup sequence.
     */
    public void start() {
        checkState(!started.get(), "WorkerBootstrap already started");
        LOG.info("event=worker_bootstrap_start workerId={}", config.workerId());
        try {
            startPhases();
        } catch (RuntimeException | Error startupFailure) {
            rollbackStartup(startupFailure);
            throw startupFailure;
        }
    }

    private void startPhases() {
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
        // Create the node pool before installing the Native callback, so every
        // subsequent Native allocation is charged to the same node cap as
        // transport buffers and the Flight service lease.
        WorkerResourceBootstrap.startResources(config, refs);
        NativeRuntimeBootstrap.initialize(config, refs, nativeCacheConfig);
        WorkerResourceBootstrap.startShuffleServiceLease(config, refs);
        NativeFunctionBootstrap.initialize(refs, config.nativeFunctionConfig());
        // Capability admission is sampled only after every JVM callback has
        // been installed successfully. A Worker must never advertise a
        // governed relation whose callback registration failed or was disabled.
        loadNativeCapabilities(refs, config.nativeFunctionConfig());
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

    private void rollbackStartup(Throwable startupFailure) {
        LOG.error("event=worker_bootstrap_rollback workerId={} error={}",
                config.workerId(), startupFailure.getMessage(), startupFailure);
        runRollback("heartbeat", startupFailure,
                () -> WorkerHeartbeatBootstrap.stopHeartbeat(refs));
        runRollback("http", startupFailure,
                () -> WorkerHttpBootstrap.stopHttp(refs));
        runRollback("cdc", startupFailure, this::stopCdc);
        runRollback("tasks", startupFailure,
                () -> WorkerShutdownSequence.drainTasks(refs.worker(), refs.taskManager()));
        runRollback("task_manager", startupFailure,
                () -> closeTaskManager(refs));
        runRollback("native_writer", startupFailure,
                () -> closeNativeWriteExecutor(refs));
        runRollback("native_functions", startupFailure,
                () -> NativeFunctionBootstrap.close(refs));
        runRollback("native_resources", startupFailure,
                () -> WorkerResourceBootstrap.releaseResources(refs, observability));
    }

    private static void runRollback(
            String resource, Throwable startupFailure, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | Error cleanupFailure) {
            startupFailure.addSuppressed(cleanupFailure);
            LOG.warn("event=worker_bootstrap_rollback_failed resource={} error={}",
                    resource, cleanupFailure.getMessage(), cleanupFailure);
        }
    }

    /**
     * Executes the ordered shutdown sequence (reverse of startup).
     */
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        try {
            LOG.info("event=worker_shutdown_start workerId={}", config.workerId());
            WorkerHeartbeatBootstrap.deregister(refs, config.workerId());
            WorkerHeartbeatBootstrap.stopHeartbeat(refs);
            stopCdc();
            WorkerShutdownSequence.drainTasks(refs.worker(), refs.taskManager());
            WorkerHttpBootstrap.stopHttp(refs);
            closeTaskManager(refs);
            closeNativeWriteExecutor(refs);
            closeNativeFunctions(refs);
            // Native shutdown is handled by WorkerShutdownSequence.releaseResources()
            // via shutdownNative(). Calling it here would cause double-free of native
            // thread pools and memory pools (P0-BUG-2).
            WorkerResourceBootstrap.releaseResources(refs, observability);
            LOG.info("event=worker_shutdown_completed workerId={}", config.workerId());
        } finally {
            shutdownCompleted.countDown();
        }
    }

    /** Keeps the process entry point alive until owned resources finish shutdown. */
    public void awaitShutdown() throws InterruptedException {
        shutdownCompleted.await();
    }

    /** Loads and fail-closed validates native capabilities via the JNI channel. */
    private static void loadNativeCapabilities(
            WorkerComponentRefs refs,
            org.pangrova.worker.jni.NativeFunctionConfig functionConfig) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=native_capabilities reason=native_unavailable");
            return;
        }
        try {
            NativeCapabilityAdvertisement.Advertisement advertisement =
                    NativeCapabilityAdvertisement.load(functionConfig);
            refs.setNativeCapabilityInventory(advertisement.encodedInventory());
            LOG.info("event=native_capabilities_loaded genericCount={} modelCount={} engineVersion={}",
                    advertisement.inventory().getCapabilitiesCount(),
                    advertisement.inventory().getModelsCount(),
                    advertisement.engineVersion());
        } catch (Exception e) {
            LOG.error("event=native_capabilities_load_failed error={}", e.getMessage(), e);
            throw new IllegalStateException(
                    "Native capability negotiation failed; Worker cannot become ready", e);
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
        } else if (refs.catalogResourceResolver() instanceof AutoCloseable closeable) {
            // Native registration may fail after the catalog runtime is installed.
            // In that case the writer never acquired ownership of the resolver.
            try {
                closeable.close();
            } catch (Exception failure) {
                throw new IllegalStateException("Failed to close Worker catalog runtime", failure);
            }
        }
    }

    private static void closeNativeFunctions(WorkerComponentRefs refs) {
        try {
            NativeFunctionBootstrap.close(refs);
            LOG.info("event=native_function_resources_closed");
        } catch (RuntimeException failure) {
            LOG.warn("event=native_function_resources_close_error error={}",
                    failure.getMessage(), failure);
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
                cc.checkpointIntervalBytes(),
                cc.artifactCachePath(),
                cc.artifactCacheBytes(),
                cc.artifactIndexMemoryBytes());
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
