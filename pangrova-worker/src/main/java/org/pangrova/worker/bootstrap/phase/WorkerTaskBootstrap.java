package org.pangrova.worker.bootstrap.phase;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.shuffle.NativeShuffleTransport;
import org.pangrova.worker.result.NativeResultTransport;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.QueryCancellationPropagator;
import org.pangrova.worker.task.RemoteTaskStatusReporter;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;
import org.pangrova.worker.task.TaskRetryConfig;
import org.pangrova.worker.task.TaskRetryExecutor;
import org.pangrova.worker.task.TenantExecutionQuotaEnforcer;
import org.pangrova.worker.task.TransportRefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes the TaskManager subsystem: executor, transport SPIs,
 * status reporter, and result expiration.
 *
 * <p>All data-plane transport uses native implementations (data exchange
 * for shuffle/exchange/results). No Java fallback.</p>
 */
public final class WorkerTaskBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerTaskBootstrap.class);

    /** Interval between periodic task-manager cleanup sweeps. */
    private static final long TASK_CLEANUP_INTERVAL_MINUTES = 15;

    /** Idle keep-alive for surplus orchestration-monitor threads. */
    private static final long MONITOR_KEEP_ALIVE_SECONDS = 60L;

    private WorkerTaskBootstrap() {}

    /**
     * Creates and initializes TaskManager with native transport SPIs.
     * Throws {@link org.pangrova.operations.bootstrap.BootstrapFailedException}
     * when native engine is unavailable — worker cannot operate without it.
     */
    public static void startTaskManager(WorkerBootstrapConfig config,
                                        WorkerComponentRefs refs,
                                        WorkerObservabilitySetup observability) {
        TaskExecutor executor = createTaskExecutor(refs.nativeAvailable());
        registerNativeWriter(refs);
        injectShuffleHmacKey(refs);
        RemoteTaskStatusReporter reporter = createStatusReporter(config, observability);
        MemoryRefs memoryDeps = new MemoryRefs(
                refs.resourceManager(), refs.nodeMemoryPool(),
                observability.tracingProvider());
        TransportRefs transportDeps = refs.nativeAvailable()
                ? createTransportRefs(executor, config)
                : TransportRefs.empty();
        refs.setResultServerPort(extractResultServerPort(transportDeps));
        refs.setExchangeTransport(transportDeps.exchangeTransport());
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor, memoryDeps, transportDeps,
                createOrchestrationExecutor(config));
        TaskManager tm = new TaskManager(deps, config.workerId(), 0.0);
        // F-CHAIN-CACHE-02: wire the recent-file-access tracker so every task
        // submission feeds split paths into the heartbeat-reported sample.
        // Reuse via refs so the heartbeat phase reads the same tracker.
        org.pangrova.worker.cache.RecentFileAccessTracker tracker =
                refs.recentFileAccessTracker();
        if (tracker == null) {
            tracker = new org.pangrova.worker.cache.RecentFileAccessTracker();
            refs.setRecentFileAccessTracker(tracker);
        }
        tm.setRecentFileAccessTracker(tracker);
        tm.initialize();
        TenantExecutionQuotaEnforcer quotaEnforcer =
                new TenantExecutionQuotaEnforcer(refs.tenantResourceTracker());
        tm.setQuotaEnforcer(quotaEnforcer);
        // Wire transient-failure retry executor (BUG-13.1.1) and per-query
        // cancellation propagator (BUG-13.1.7). Both are queried from request
        // handlers via TaskManager accessors.
        TaskRetryExecutor retryExecutor = new TaskRetryExecutor(TaskRetryConfig.defaults());
        tm.setRetryExecutor(retryExecutor);
        QueryCancellationPropagator cancellationPropagator = new QueryCancellationPropagator(tm);
        tm.setCancellationPropagator(cancellationPropagator);
        LOG.info("event=task_manager_components_wired retryExecutor=true cancellationPropagator=true");
        java.util.concurrent.ScheduledExecutorService cleanupExecutor =
                org.pangrova.common.concurrent.ManagedExecutorFactory
                        .newSingleThreadScheduledExecutor("worker-task-cleanup");
        ((java.util.concurrent.ScheduledThreadPoolExecutor) cleanupExecutor)
                .setRemoveOnCancelPolicy(true);
        // Retention is measured from terminal-time (see TaskCleanupScheduler)
        // rather than start-time. 15 minutes gives long-running JDBC result
        // fetchers ample time to drain buffered batches before the
        // task entry is evicted, while still bounding worker memory growth.
        tm.startPeriodicCleanup(cleanupExecutor,
                java.time.Duration.ofMinutes(TASK_CLEANUP_INTERVAL_MINUTES), java.time.Duration.ofMinutes(1));
        refs.setTaskManager(tm);
        LOG.info("event=task_manager_initialized transport=native");
    }

    /**
     * Creates a bounded orchestration executor that is separate from the
     * native driver pool. Task 188 fix: the completion monitor must not
     * share threads with the native folly executor, as blocking orchestration
     * (poll-until-finished) on driver pool threads breaks morsel-driven
     * scheduling and causes deadlock under fan-out.
     *
     * <p>The pool size is set to 2x the number of concurrent native task
     * slots to allow headroom for pipeline overlap without risking unbounded
     * thread creation (which was the previous CachedThreadPool behavior).</p>
     */
    static java.util.concurrent.ExecutorService createOrchestrationExecutor(
            WorkerBootstrapConfig config) {
        // Default to 2 * available processors as max concurrent tasks
        int maxConcurrentTasks = Runtime.getRuntime().availableProcessors() * 2;
        java.util.concurrent.ExecutorService orchestrationExecutor =
                new java.util.concurrent.ThreadPoolExecutor(
                        2, // core threads
                        maxConcurrentTasks,
                        MONITOR_KEEP_ALIVE_SECONDS, java.util.concurrent.TimeUnit.SECONDS,
                        new java.util.concurrent.LinkedBlockingQueue<>(maxConcurrentTasks * 2),
                        r -> {
                            Thread t = new Thread(r, "orchestration-monitor");
                            t.setDaemon(true);
                            return t;
                        },
                        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        LOG.info("event=orchestration_executor_created maxThreads={} "
                + "detail=separate_from_native_driver_pool", maxConcurrentTasks);
        return orchestrationExecutor;
    }

    /**
     * Creates native transport SPI implementations.
     * Native transport is mandatory -- failure to start is fatal.
     */
    static TransportRefs createTransportRefs(TaskExecutor executor,
                                             WorkerBootstrapConfig config) {
        String workerHost = WorkerHttpBootstrap.resolveAdvertisedHost(config);
        var shuffleTransport = new NativeShuffleTransport(executor, workerHost);
        var resultTransport = new NativeResultTransport(executor, workerHost);
        resultTransport.startServer(0);
        var exchangeTransport = new NativeExchangeTransport(executor);
        LOG.info("event=transport_spi_selected shuffle=native result=native exchange=native resultPort={}",
                resultTransport.boundPort());
        return new TransportRefs(shuffleTransport, resultTransport, exchangeTransport);
    }

    /**
     * Extracts the native result server bound port from the result transport.
     * Returns -1 if the transport is not a NativeResultTransport.     */
    private static int extractResultServerPort(TransportRefs transportDeps) {
        if (transportDeps.resultTransport() instanceof NativeResultTransport nfrt) {
            return nfrt.boundPort();
        }
        return -1;
    }

    static TaskExecutor createTaskExecutor(boolean nativeAvailable) {
        if (nativeAvailable) {
            return new NativeTaskExecutor();
        }
        throw new org.pangrova.operations.bootstrap.BootstrapFailedException(
                "TaskExecutor",
                new IllegalStateException(
                        "Native library unavailable — worker cannot execute queries without native engine"));
    }

    /**
     * Registers the native write executor with the C++ runtime via JNI.
     * Enables native write tasks to call back into Java for connector page sink operations.
     * Skipped if native is unavailable or no catalog resolver was injected.
     */
    static void registerNativeWriter(WorkerComponentRefs refs) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=native_writer reason=native_unavailable");
            return;
        }
        var catalogResolver = refs.catalogResourceResolver();
        if (catalogResolver == null) {
            LOG.warn("event=phase_skipped phase=native_writer reason=no_catalog_resolver_injected");
            return;
        }
        var writer = new org.pangrova.worker.jni.write.NativeWriteExecutor(catalogResolver);
        org.pangrova.worker.jni.NativeRuntime.nativeRegisterWriter(writer);
        refs.setNativeWriteExecutor(writer);
        if (refs.taskManager() != null) {
            refs.taskManager().setWriteExecutor(writer);
        }
        LOG.info("event=native_writer_registered");
    }

    /**
     * Injects HMAC key(s) for shuffle ticket verification into the native side.
     * Called once during startup after worker registration provides the key.
     * Skipped if native is unavailable or no HMAC key was provided.
     */
    static void injectShuffleHmacKey(WorkerComponentRefs refs) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=shuffle_hmac_key reason=native_unavailable");
            return;
        }
        byte[] currentKey = refs.shuffleHmacCurrentKey();
        if (currentKey == null || currentKey.length == 0) {
            LOG.info("event=phase_skipped phase=shuffle_hmac_key reason=no_key_configured");
            return;
        }
        byte[] previousKey = refs.shuffleHmacPreviousKey();
        if (previousKey == null) {
            previousKey = new byte[0];
        }
        org.pangrova.worker.jni.NativeRuntime.nativeSetHmacKey(currentKey, previousKey);
        LOG.info("event=shuffle_hmac_key_injected keyLength={}", currentKey.length);
    }

    static RemoteTaskStatusReporter createStatusReporter(WorkerBootstrapConfig config,
                                                       WorkerObservabilitySetup observability) {
        String coordinatorUrl = config.coordinatorUrl();
        if (coordinatorUrl == null || coordinatorUrl.isBlank()) {
            throw new org.pangrova.operations.bootstrap.BootstrapFailedException(
                    "TaskStatusReporter",
                    new IllegalStateException(
                            "coordinatorUrl must not be null or blank — "
                            + "set worker.coordinator-url in config YAML or PANGROVA_WORKER_COORDINATOR_URL env var"));
        }
        warnIfSingleHostUrl(coordinatorUrl);
        return parseReporterFromUrl(coordinatorUrl, observability);
    }

    static void warnIfSingleHostUrl(String coordinatorUrl) {
        try {
            java.net.URI uri = java.net.URI.create(coordinatorUrl);
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return;
            }
            if (looksLikeSingleHost(host)) {
                LOG.warn("event=coordinator_url_single_host_detected coordinatorUrl={} "
                        + "detail=coordinatorUrl appears to point to a single host. "
                        + "In multi-coordinator deployments, set worker.coordinator-url "
                        + "to the load-balancer VIP.",
                        coordinatorUrl);
            }
        } catch (IllegalArgumentException e) {
            // Malformed URI — parseReporterFromUrl will handle the error
        }
    }

    static boolean looksLikeSingleHost(String host) {
        if ("localhost".equalsIgnoreCase(host) || host.equals("::1")) {
            return true;
        }
        if (host.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
            return true;
        }
        if (!host.contains(".")) {
            return true;
        }
        return false;
    }

    private static RemoteTaskStatusReporter parseReporterFromUrl(String url,
                                                                WorkerObservabilitySetup observability) {
        try {
            return new RemoteTaskStatusReporter(url,
                    observability.tracingProvider());
        } catch (org.pangrova.operations.bootstrap.BootstrapFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new org.pangrova.operations.bootstrap.BootstrapFailedException(
                    "TaskStatusReporter",
                    new IllegalArgumentException(
                            "Failed to parse coordinatorUrl: " + url, e));
        }
    }
}
