package org.pangrova.worker.bootstrap.phase;

import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.config.ShuffleMode;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.WorkerTaskOwnership;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientPool;
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
 * Initializes the TaskManager subsystem: executor, transport SPIs, status reporter, and result
 * expiration.
 *
 * <p>All data-plane transport uses native implementations (data exchange for
 * shuffle/exchange/results). No Java fallback.
 */
public final class WorkerTaskBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerTaskBootstrap.class);
    private static final java.security.SecureRandom SHUFFLE_KEY_RANDOM = new java.security.SecureRandom();

    /** Interval between periodic task-manager cleanup sweeps. */
    private static final long TASK_CLEANUP_INTERVAL_MINUTES = 15;

    /** Idle keep-alive for surplus orchestration-monitor threads. */
    private static final long MONITOR_KEEP_ALIVE_SECONDS = 60L;

    private WorkerTaskBootstrap() {}

    /**
     * Creates and initializes TaskManager with native transport SPIs. Throws {@link
     * org.pangrova.common.lifecycle.BootstrapFailedException} when native engine is unavailable
     * — worker cannot operate without it.
     */
    public static void startTaskManager(
            WorkerBootstrapConfig config,
            WorkerComponentRefs refs,
            WorkerObservabilitySetup observability) {
        var ownership = new WorkerTaskOwnership(config.resourceConfig().shuffleChannelLimits());
        refs.installTaskOwnership(ownership);
        TaskExecutor executor = createTaskExecutor(refs.nativeAvailable(), config, ownership);
        if (executor instanceof NativeTaskExecutor nativeExecutor
                && refs.catalogResourceResolver()
                        instanceof
                        org.pangrova.worker.nativebridge.jni.write.catalog.WorkerCatalogRuntime catalogs) {
            nativeExecutor.setCatalogRuntime(catalogs);
        }
        injectShuffleHmacKey(refs);
        RemoteTaskStatusReporter reporter = createStatusReporter(config, observability);
        MemoryRefs memoryDeps =
                new MemoryRefs(
                        refs.resourceManager(),
                        refs.nodeMemoryPool(),
                        observability.tracingProvider());
        TransportRefs transportDeps = createTransportRefs(executor, config, ownership);
        refs.setResultServerPort(extractResultServerPort(transportDeps));
        refs.setExchangeTransport(transportDeps.exchangeTransport());
        TaskManagerContext deps =
                new TaskManagerContext(
                        reporter,
                        executor,
                        memoryDeps,
                        transportDeps,
                        retainedOrchestrationExecutor(config, ownership));
        TaskManager tm = new TaskManager(deps, config.workerId(), 0.0, refs.admissionIncarnation());
        ownership.publish(tm);
        refs.setTaskManager(tm);
        configureTaskManager(config, refs, tm, ownership);
        registerNativeWriter(refs);
        LOG.info("event=task_manager_initialized transport=native");
    }

    private static void configureTaskManager(
            WorkerBootstrapConfig config,
            WorkerComponentRefs refs,
            TaskManager tm,
            WorkerTaskOwnership ownership) {
        if (config.coordinatorUrl() != null && !config.coordinatorUrl().isBlank()) {
            // The task endpoint may become reachable before the first control
            // handshake completes; reject every task during that window.
            tm.requireControlEpochHandshake();
        }
        // F-CHAIN-CACHE-02: wire the recent-file-access tracker so every task
        // submission feeds split paths into the heartbeat-reported sample.
        // Reuse via refs so the heartbeat phase reads the same tracker.
        org.pangrova.worker.cache.RecentFileAccessTracker tracker = refs.recentFileAccessTracker();
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
        LOG.info(
                "event=task_manager_components_wired retryExecutor=true cancellationPropagator=true");
        java.util.concurrent.ScheduledExecutorService cleanupExecutor =
                org.pangrova.common.concurrent.ManagedExecutorFactory
                        .newSingleThreadScheduledExecutor("worker-task-cleanup");
        retainExecutor(cleanupExecutor, ownership);
        ((java.util.concurrent.ScheduledThreadPoolExecutor) cleanupExecutor)
                .setRemoveOnCancelPolicy(true);
        // Retention is measured from terminal-time (see TaskCleanupScheduler)
        // rather than start-time. 15 minutes gives long-running JDBC result
        // fetchers ample time to drain buffered batches before the
        // task entry is evicted, while still bounding worker memory growth.
        tm.startPeriodicCleanup(
                cleanupExecutor,
                java.time.Duration.ofMinutes(TASK_CLEANUP_INTERVAL_MINUTES),
                java.time.Duration.ofMinutes(1));
    }

    /**
     * Creates a bounded orchestration executor that is separate from the native driver pool. Task
     * 188 fix: the completion monitor must not share threads with the native folly executor, as
     * blocking orchestration (poll-until-finished) on driver pool threads breaks morsel-driven
     * scheduling and causes deadlock under fan-out.
     *
     * <p>The pool size is set to 2x the number of concurrent native task slots to allow headroom
     * for pipeline overlap without risking unbounded thread creation (which was the previous
     * CachedThreadPool behavior).
     */
    static java.util.concurrent.ExecutorService createOrchestrationExecutor(
            WorkerBootstrapConfig config) {
        // Default to 2 * available processors as max concurrent tasks
        int maxConcurrentTasks = Runtime.getRuntime().availableProcessors() * 2;
        java.util.concurrent.ExecutorService orchestrationExecutor =
                new java.util.concurrent.ThreadPoolExecutor(
                        2, // core threads
                        maxConcurrentTasks,
                        MONITOR_KEEP_ALIVE_SECONDS,
                        java.util.concurrent.TimeUnit.SECONDS,
                        new java.util.concurrent.LinkedBlockingQueue<>(maxConcurrentTasks * 2),
                        r -> {
                            Thread t = new Thread(r, "orchestration-monitor");
                            t.setDaemon(true);
                            return t;
                        },
                        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        LOG.info(
                "event=orchestration_executor_created maxThreads={} "
                        + "detail=separate_from_native_driver_pool",
                maxConcurrentTasks);
        return orchestrationExecutor;
    }

    /**
     * Creates native transport SPI implementations. Native transport is mandatory -- failure to
     * start is fatal.
     */
    static TransportRefs createTransportRefs(
            TaskExecutor executor, WorkerBootstrapConfig config, WorkerTaskOwnership ownership) {
        String workerHost = WorkerHttpBootstrap.resolveAdvertisedHost(config);
        String tlsIdentity =
                config.transportSecurityConfig().enabled()
                        ? config.transportSecurityConfig().tlsIdentity()
                        : "";
        String shuffleHost = resolveShuffleAdvertisedHost(config, workerHost);
        var shuffleTransport = new NativeShuffleTransport(executor, shuffleHost, tlsIdentity);
        ownership.retain(shuffleTransport);
        String resultBindHost = config.httpHost() == null || config.httpHost().isBlank()
                ? "0.0.0.0" : config.httpHost().trim();
        var resultTransport = new NativeResultTransport(executor, workerHost, resultBindHost);
        ownership.retain(resultTransport);
        resultTransport.startServer(config.transportPort());
        var exchangeTransport =
                config.shuffleMode() == ShuffleMode.REMOTE
                        ? new NativeExchangeTransport(
                                executor, createRemoteShuffleClientPool(config, ownership))
                        : new NativeExchangeTransport(executor);
        ownership.retain(exchangeTransport);
        LOG.info(
                "event=transport_spi_selected shuffle=native result=native exchange=native resultPort={}",
                resultTransport.boundPort());
        return new TransportRefs(shuffleTransport, resultTransport, exchangeTransport);
    }

    static String resolveShuffleAdvertisedHost(WorkerBootstrapConfig config, String workerHost) {
        String configured = config.shuffleAdvertisedHost();
        if (configured == null || configured.isBlank()) {
            return workerHost;
        }
        if (WorkerHttpBootstrap.isWildcardAddress(configured)) {
            throw new IllegalArgumentException("worker.shuffle.advertised-host must be a concrete address");
        }
        return configured;
    }

    /**
     * Extracts the native result server bound port from the result transport. Returns -1 if the
     * transport is not a NativeResultTransport.
     */
    private static int extractResultServerPort(TransportRefs transportDeps) {
        if (transportDeps.resultTransport() instanceof NativeResultTransport nfrt) {
            return nfrt.boundPort();
        }
        return -1;
    }

    static TaskExecutor createTaskExecutor(
            boolean nativeAvailable, WorkerBootstrapConfig config, WorkerTaskOwnership ownership) {
        if (nativeAvailable) {
            if (config.shuffleMode() == ShuffleMode.REMOTE) {
                RemoteShuffleByteEgress egress =
                        new RemoteShuffleByteEgress(
                                createRemoteShuffleClientPool(config, ownership));
                ownership.retain(egress);
                org.pangrova.worker.nativebridge.jni.NativeRuntime.nativeRegisterEgress(egress);
                var executor = new NativeTaskExecutor(egress);
                ownership.retain(executor);
                return executor;
            }
            var executor = new NativeTaskExecutor();
            ownership.retain(executor);
            return executor;
        }
        throw new org.pangrova.common.lifecycle.BootstrapFailedException(
                "TaskExecutor",
                new IllegalStateException(
                        "Native library unavailable — worker cannot execute queries without native engine"));
    }

    private static RemoteShuffleClientPool createRemoteShuffleClientPool(
            WorkerBootstrapConfig config, WorkerTaskOwnership ownership) {
        final long writeTimeoutMillis = 30_000L;
        final long readTimeoutMillis = 30_000L;
        RemoteShuffleClientPool pool;
        var transportSecurity = config.transportSecurityConfig();
        if (transportSecurity.enabled()) {
            var pem =
                    new RemoteShuffleClientPool.PemTlsConfig(
                            java.nio.file.Path.of(transportSecurity.certificateChainPath()),
                            java.nio.file.Path.of(transportSecurity.privateKeyPath()),
                            java.nio.file.Path.of(transportSecurity.rootCaPath()));
            pool =
                    new RemoteShuffleClientPool(
                            writeTimeoutMillis,
                            readTimeoutMillis,
                            RemoteShuffleClientPool.PoolOptions.mutualTls(pem),
                            ownership.channelAdmission());
        } else {
            pool =
                    new RemoteShuffleClientPool(
                            writeTimeoutMillis,
                            readTimeoutMillis,
                            RemoteShuffleClientPool.PoolOptions.defaults(),
                            ownership.channelAdmission());
        }
        ownership.retain(pool);
        return pool;
    }

    private static java.util.concurrent.ExecutorService retainedOrchestrationExecutor(
            WorkerBootstrapConfig config, WorkerTaskOwnership ownership) {
        var executor = createOrchestrationExecutor(config);
        retainExecutor(executor, ownership);
        return executor;
    }

    private static void retainExecutor(
            java.util.concurrent.ExecutorService executor, WorkerTaskOwnership ownership) {
        ownership.retain(
                () -> {
                    executor.shutdownNow();
                    try {
                        if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "Task bootstrap executor has not exited");
                        }
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw failure;
                    }
                });
    }

    /**
     * Registers the native write executor with the C++ runtime via JNI. Enables native write tasks
     * to call back into Java for connector page sink operations. Native-capable startup fails
     * closed if no catalog resolver was injected.
     */
    static void registerNativeWriter(WorkerComponentRefs refs) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=native_writer reason=native_unavailable");
            return;
        }
        var catalogResolver = refs.catalogResourceResolver();
        if (catalogResolver == null) {
            throw new org.pangrova.common.lifecycle.BootstrapFailedException(
                    "NativeWriter",
                    new IllegalStateException("Worker catalog runtime is not configured"));
        }
        var writer = new org.pangrova.worker.nativebridge.jni.write.NativeWriteExecutor(catalogResolver);
        org.pangrova.worker.nativebridge.jni.NativeRuntime.nativeRegisterWriter(writer);
        refs.setNativeWriteExecutor(writer);
        if (refs.taskManager() != null) {
            refs.taskManager().setWriteExecutor(writer);
        }
        LOG.info("event=native_writer_registered");
    }

    /**
     * Injects HMAC key(s) for shuffle ticket verification into the native side. A process-local
     * 256-bit key is generated when no externally managed key was supplied. Tickets are minted and
     * verified by the same producer process, so sharing this secret with the Coordinator or
     * consumers is unnecessary and would enlarge the trust boundary.
     */
    static void injectShuffleHmacKey(WorkerComponentRefs refs) {
        if (!refs.nativeAvailable()) {
            LOG.info("event=phase_skipped phase=shuffle_hmac_key reason=native_unavailable");
            return;
        }
        ShuffleHmacKeys keys =
                resolveShuffleHmacKeys(refs.shuffleHmacCurrentKey(), refs.shuffleHmacPreviousKey());
        if (refs.shuffleHmacCurrentKey() == null || refs.shuffleHmacCurrentKey().length == 0) {
            LOG.info(
                    "event=shuffle_hmac_key_generated scope=worker_process keyLength={}",
                    keys.current().length);
        }
        refs.setShuffleHmacCurrentKey(keys.current());
        refs.setShuffleHmacPreviousKey(keys.previous());
        org.pangrova.worker.nativebridge.jni.NativeRuntime.nativeSetHmacKey(keys.current(), keys.previous());
        LOG.info("event=shuffle_hmac_key_injected keyLength={}", keys.current().length);
    }

    static ShuffleHmacKeys resolveShuffleHmacKeys(byte[] currentKey, byte[] previousKey) {
        byte[] resolvedCurrent = currentKey;
        if (resolvedCurrent == null || resolvedCurrent.length == 0) {
            resolvedCurrent = new byte[32];
            SHUFFLE_KEY_RANDOM.nextBytes(resolvedCurrent);
        } else if (resolvedCurrent.length < 32) {
            throw new IllegalArgumentException("shuffle HMAC key must contain at least 32 bytes");
        }
        byte[] resolvedPrevious = previousKey == null ? new byte[0] : previousKey;
        if (resolvedPrevious.length > 0 && resolvedPrevious.length < 32) {
            throw new IllegalArgumentException(
                    "previous shuffle HMAC key must contain at least 32 bytes");
        }
        return new ShuffleHmacKeys(resolvedCurrent, resolvedPrevious);
    }

    record ShuffleHmacKeys(byte[] current, byte[] previous) {
        ShuffleHmacKeys {
            current = current.clone();
            previous = previous.clone();
        }

        @Override
        public byte[] current() {
            return current.clone();
        }

        @Override
        public byte[] previous() {
            return previous.clone();
        }
    }

    static RemoteTaskStatusReporter createStatusReporter(
            WorkerBootstrapConfig config, WorkerObservabilitySetup observability) {
        String coordinatorUrl = config.coordinatorUrl();
        if (coordinatorUrl == null || coordinatorUrl.isBlank()) {
            throw new org.pangrova.common.lifecycle.BootstrapFailedException(
                    "TaskStatusReporter",
                    new IllegalStateException(
                            "coordinatorUrl must not be null or blank — "
                                    + "set worker.coordinator-url in config YAML or "
                                    + "PANGROVA_WORKER_COORDINATOR_URL env var"));
        }
        warnIfSingleHostUrl(coordinatorUrl);
        return parseReporterFromUrl(coordinatorUrl, observability, config.internalAuthSecret());
    }

    static void warnIfSingleHostUrl(String coordinatorUrl) {
        try {
            java.net.URI uri = java.net.URI.create(coordinatorUrl);
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return;
            }
            if (looksLikeSingleHost(host)) {
                LOG.warn(
                        "event=coordinator_url_single_host_detected coordinatorUrl={} "
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

    private static RemoteTaskStatusReporter parseReporterFromUrl(
            String url, WorkerObservabilitySetup observability, String internalAuthSecret) {
        try {
            return new RemoteTaskStatusReporter(
                    url, observability.tracingProvider(), internalAuthSecret);
        } catch (org.pangrova.common.lifecycle.BootstrapFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new org.pangrova.common.lifecycle.BootstrapFailedException(
                    "TaskStatusReporter",
                    new IllegalArgumentException("Failed to parse coordinatorUrl: " + url, e));
        }
    }
}
