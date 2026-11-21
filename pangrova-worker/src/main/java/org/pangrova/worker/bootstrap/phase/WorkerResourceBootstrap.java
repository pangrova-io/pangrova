package org.pangrova.worker.bootstrap.phase;

import org.pangrova.worker.resource.memory.ShuffleServiceMemoryLease;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPuller;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPublisher;
import org.pangrova.observability.sdk.metrics.WorkerMetrics;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.CriticalPressureHandler;
import org.pangrova.worker.resource.memory.LargestQueryKiller;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.memory.RootMemoryPool;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.lifecycle.WorkerResourceShutdown;
import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes Worker resource infrastructure: memory pools, thread pools,
 * spill manager, and native metrics collection.
 */
public final class WorkerResourceBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerResourceBootstrap.class);
    private static final long NATIVE_METRICS_INTERVAL_MS = 5_000;

    private WorkerResourceBootstrap() {}

    /**
     * Creates ResourceManager, RootMemoryPool, and NodeMemoryPool.
     * Injects TenantResourceTracker into MemoryArbitrator for per-tenant
     * memory quota enforcement.
     */
    public static void startResources(WorkerBootstrapConfig config, WorkerComponentRefs refs) {
        var rc = config.resourceConfig();
        // Task 148: reuse the shared MemoryArbitrator and TenantResourceTracker
        // created by WorkerBootstrap so native and Java memory share a single
        // unified budget. Fall back to local creation for backward compat
        // (standalone tests that call startResources directly).
        TenantResourceTracker tenantTracker = refs.tenantResourceTracker();
        if (tenantTracker == null) {
            tenantTracker = new TenantResourceTracker();
            refs.setTenantResourceTracker(tenantTracker);
        }
        MemoryArbitrator arbitrator = refs.memoryArbitrator();
        if (arbitrator == null) {
            arbitrator = new MemoryArbitrator(
                    rc.memoryBudgetBytes(), rc.systemReservedBytes(), tenantTracker);
            refs.setMemoryArbitrator(arbitrator);
        }
        ThreadBudgetTracker threadPool = new ThreadBudgetTracker(rc.queryThreads(), rc.ioThreads(),
                rc.schedulerThreads());
        SpillManager spillManager = rc.spillDirectory() != null
                ? new SpillManager(rc.spillDirectory()) : new SpillManager();
        RootMemoryPool cluster = new RootMemoryPool("root-" + config.workerId(), rc.nodeMemoryBytes());
        refs.setRootMemoryPool(cluster);
        NodeMemoryPool nodePool = cluster.createNodePool("node-" + config.workerId(), rc.nodeMemoryBytes());
        // Wire CriticalPressureHandler with LargestQueryKiller so that OOM pressure
        // triggers victim selection instead of JVM crash (F-RESOURCE-02).
        CriticalPressureHandler pressureHandler = new CriticalPressureHandler(
                new LargestQueryKiller(),
                () -> nodePool.getActiveQueryPools(),
                pool -> LOG.warn("event=oom_kill_query pool={} usedBytes={}", pool.getName(), pool.getUsedBytes()));
        refs.setResourceManager(new ResourceManager(arbitrator, threadPool, spillManager, pressureHandler));
        refs.setNodeMemoryPool(nodePool);
        if (refs.nativeAvailable()) {
            startShuffleServiceLease(config, refs);
        }
        LOG.info("event=phase_started phase=resources memoryMB={} nodePoolMB={} queryThreads={} "
                        + "ioThreads={} schedulerThreads={} tenantQuotaEnabled=true",
                rc.memoryBudgetBytes() / (1024 * 1024),
                rc.nodeMemoryBytes() / (1024 * 1024),
                rc.queryThreads(), rc.ioThreads(), rc.schedulerThreads());
    }

    /** Reserves process-lifetime Flight service memory after Native initialization. */
    public static void startShuffleServiceLease(WorkerBootstrapConfig config, WorkerComponentRefs refs) {
        var rc = config.resourceConfig();
        refs.setShuffleServiceMemoryLease(ShuffleServiceMemoryLease.reserve(
                config.workerId(), rc.shuffleRetentionBytes(), rc.shuffleTransitBytes(),
                refs.nodeMemoryPool(), refs.memoryArbitrator()));
    }

    /**
     * Starts periodic native metrics collection if native engine is available.
     */
    public static void startNativeMetrics(WorkerComponentRefs refs, WorkerObservabilitySetup observability) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=native_metrics reason=native_unavailable");
            return;
        }
        NativeMetricsPuller puller = new NativeMetricsPuller(
                timeoutMs -> {
                    byte[] cmd = NativeCommandBuilder.getMetrics();
                    byte[] resp = NativeRuntime.nativeCommand(0, cmd);
                    return NativeCommandBuilder.extractResultPayload(resp);
                });
        NativeMetricsPublisher publisher = new NativeMetricsPublisher(puller, observability.metricsProvider());
        refs.setNativeMetricsPublisher(publisher);
        ScheduledExecutorService executor = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                "worker-native-metrics");
        refs.setMetricsExecutor(executor);
        executor.scheduleAtFixedRate(
                publisher::pullAndPublish, 0, NATIVE_METRICS_INTERVAL_MS, TimeUnit.MILLISECONDS);
        LOG.info("event=phase_started phase=native_metrics intervalMs={}", NATIVE_METRICS_INTERVAL_MS);
    }

    /**
     * Creates WorkerMetrics and wires memory gauges from NodeMemoryPool.
     */
    public static WorkerMetrics startWorkerMetrics(WorkerComponentRefs refs, WorkerObservabilitySetup observability) {
        var pool = refs.nodeMemoryPool();
        WorkerMetrics metrics = new WorkerMetrics(
                observability.metricsProvider(),
                () -> (double) pool.getUsedBytes(),
                () -> (double) (pool.getMaxBytes() - pool.getFreeBytes()));
        LOG.info("event=phase_started phase=worker_metrics");
        return metrics;
    }

    /**
     * Releases all resource infrastructure (delegated to WorkerShutdownSequence).
     */
    public static void releaseResources(WorkerComponentRefs refs, WorkerObservabilitySetup observability) {
        WorkerResourceShutdown resources = new WorkerResourceShutdown(
                refs.metricsExecutor(), refs.resourceManager(), refs.nativeMetricsPublisher(),
                observability.metricsScope(), observability.otelInitializer(), refs.nativeAvailable(),
                refs.shuffleServiceMemoryLease(), refs.shuffleBufferManager());
        WorkerShutdownSequence.releaseResources(resources, () -> refs.setNativeAvailable(false));
    }
}
