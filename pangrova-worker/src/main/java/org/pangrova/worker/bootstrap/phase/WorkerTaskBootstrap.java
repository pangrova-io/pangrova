package org.pangrova.worker.bootstrap.phase;

import org.pangrova.config.enums.ShuffleMode;
import org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.ownership.WorkerTaskOwnership;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.result.NativeResultTransport;
import org.pangrova.worker.shuffle.NativeShuffleProducerRegistration;
import org.pangrova.worker.task.QueryCancellationPropagator;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;
import org.pangrova.worker.task.TenantExecutionQuotaEnforcer;
import org.pangrova.worker.task.TransportRefs;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.report.RemoteTaskStatusReporter;
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
    private static final int MAX_CLEANUP_INTERVAL_SECONDS = 60;

    private static final Logger LOG = LoggerFactory.getLogger(WorkerTaskBootstrap.class);
    private static final java.security.SecureRandom SHUFFLE_KEY_RANDOM = new java.security.SecureRandom();

    /** Idle keep-alive for surplus orchestration-monitor threads. */
    private static final long MONITOR_KEEP_ALIVE_SECONDS = 60L;

    private WorkerTaskBootstrap() {}

    /**
     * Creates and initializes TaskManager with native transport SPIs. Throws {@link
     * org.pangrova.common.exception.BootstrapFailedException} when native engine is unavailable
     * — worker cannot operate without it.
     */
    public static void startTaskManager(
            WorkerBootstrapConfig config,
            WorkerComponentRefs refs,
            WorkerObservabilitySetup observability) {
        var ownership = new WorkerTaskOwnership(openShuffle(config));
        refs.installTaskOwnership(ownership);
        TaskExecutor executor = createTaskExecutor(refs.nativeAvailable(), config, ownership);
        if (executor instanceof NativeTaskExecutor nativeExecutor
                && refs.catalogResourceResolver()
                        instanceof
                        org.pangrova.worker.catalog.WorkerCatalogRuntime catalogs) {
            nativeExecutor.setCatalogRuntime(catalogs);
        }
        var workerBackend = refs.workerExecutedTaskBackend();
        if (workerBackend != null) {
            var composite = new org.pangrova.worker.execution.CompositeTaskExecutor(
                    executor, workerBackend);
            ownership.retain(composite);
            executor = composite;
        }
        injectShuffleHmacKey(refs);
        RemoteTaskStatusReporter reporter = createStatusReporter(config, observability);
        refs.setStatusReporter(reporter);
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
        QueryCancellationPropagator cancellationPropagator = new QueryCancellationPropagator(tm);
        tm.setCancellationPropagator(cancellationPropagator);
        LOG.info(
                "event=task_manager_components_wired cancellationPropagator=true");
        startTaskCleanup(config, tm, ownership);
    }

    /** The task registry owns retention; the bootstrap owner closes its single timer. */
    static void startTaskCleanup(WorkerBootstrapConfig config, TaskManager tm, WorkerTaskOwnership ownership) {
        java.util.concurrent.ScheduledExecutorService cleanupExecutor =
                org.pangrova.common.concurrent.ManagedExecutorFactory
                        .newSingleThreadScheduledExecutor("worker-task-cleanup");
        retainExecutor(cleanupExecutor, ownership);
        ((java.util.concurrent.ScheduledThreadPoolExecutor) cleanupExecutor)
                .setRemoveOnCancelPolicy(true);
        // Terminal age bounds registry retention, not running-task execution. Earlier
        // authority expiry or actual drain evidence still takes precedence in the owner.
        tm.startPeriodicCleanup(
                cleanupExecutor,
                java.time.Duration.ofSeconds(config.resultRetentionSeconds()),
                java.time.Duration.ofSeconds(Math.min(MAX_CLEANUP_INTERVAL_SECONDS, config.resultRetentionSeconds())));
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
        var shuffleProducers = new NativeShuffleProducerRegistration(executor::executeCommand, shuffleHost, tlsIdentity,
                config.shuffleMode() == ShuffleMode.LOCAL);
        String resultBindHost = config.httpHost() == null || config.httpHost().isBlank()
                ? "0.0.0.0" : config.httpHost().trim();
        var resultTransport = new NativeResultTransport(executor, workerHost, resultBindHost);
        ownership.retain(resultTransport);
        resultTransport.startServer(config.transportPort());
        NativeExchangeTransport exchangeTransport;
        if (config.shuffleMode() == ShuffleMode.REMOTE) {
            exchangeTransport = ownership.retain(ownership.shuffle().readers(),
                    readers -> new NativeExchangeTransport(executor, readers));
        } else {
            exchangeTransport = new NativeExchangeTransport(executor);
            ownership.retain(exchangeTransport);
        }
        LOG.info(
                "event=transport_spi_selected shuffle=native result=native exchange=native resultPort={}",
                resultTransport.boundPort());
        return new TransportRefs(shuffleProducers, resultTransport, exchangeTransport);
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
                return ownership.retain(ownership.shuffle().producers(), egress -> {
                    var executor = new NativeTaskExecutor(egress);
                    org.pangrova.worker.jni.NativeRuntime.nativeRegisterEgress(egress);
                    return executor;
                });
            }
            var executor = new NativeTaskExecutor();
            ownership.retain(executor);
            return executor;
        }
        throw new org.pangrova.common.exception.BootstrapFailedException(
                "TaskExecutor",
                new IllegalStateException(
                        "Native library unavailable — worker cannot execute queries without native engine"));
    }

    static NativeShuffleWorkerFactory.Session openShuffle(WorkerBootstrapConfig config) {
        if (config.shuffleMode() != ShuffleMode.REMOTE) return null;
        var required = java.util.Set.of(org.pangrova.shuffle.api.enums.ShuffleCapability.BLOCKING_READ,
                org.pangrova.shuffle.api.enums.ShuffleCapability.PARTITION_READ);
        var factories = new java.util.HashMap<String, NativeShuffleWorkerFactory>();
        for (var factory : java.util.ServiceLoader.load(NativeShuffleWorkerFactory.class,
                WorkerTaskBootstrap.class.getClassLoader())) {
            var id = factory.providerId();
            if (id == null || !id.matches("[a-z][a-z0-9-]*")
                    || factory.apiVersion() != NativeShuffleWorkerFactory.API_VERSION
                    || factories.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Invalid or duplicate Worker shuffle factory: " + id);
            }
        }
        var selected = config.resourceConfig().shuffleProvider();
        var id = selected.id();
        var factory = factories.get(id);
        if (factory == null || !factory.capabilities().capabilities().containsAll(required)) {
            throw new IllegalArgumentException("Missing or incapable Worker shuffle provider: " + id);
        }
        var security = config.transportSecurityConfig();
        var tls = security.enabled() ? new NativeShuffleWorkerFactory.Tls(
                java.nio.file.Path.of(security.certificateChainPath()), java.nio.file.Path.of(
                        security.privateKeyPath()),
                java.nio.file.Path.of(security.rootCaPath())) : null;
        var session = java.util.Objects.requireNonNull(factory.open(new NativeShuffleWorkerFactory.Configuration(
                config.resourceConfig().shuffleChannelLimits(), tls, selected.properties())), "Worker shuffle session");
        try {
            var actual = session.capabilities().capabilities();
            if (!actual.containsAll(required) || !factory.capabilities().capabilities().containsAll(actual)) {
                throw new IllegalStateException("Shuffle provider session lacks the configured Worker capabilities");
            }
            return session;
        } catch (RuntimeException | Error failure) {
            try {
                session.seal();
                session.requireDrained();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
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
            throw new org.pangrova.common.exception.BootstrapFailedException(
                    "NativeWriter",
                    new IllegalStateException("Worker catalog runtime is not configured"));
        }
        var writer = new org.pangrova.worker.write.NativeWriteExecutor(catalogResolver);
        org.pangrova.worker.jni.NativeRuntime.nativeRegisterWriter(writer);
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
        org.pangrova.worker.jni.NativeRuntime.nativeSetHmacKey(keys.current(), keys.previous());
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
            throw new org.pangrova.common.exception.BootstrapFailedException(
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
                    url, observability.tracingProvider(), observability.metricsProvider(), internalAuthSecret);
        } catch (org.pangrova.common.exception.BootstrapFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new org.pangrova.common.exception.BootstrapFailedException(
                    "TaskStatusReporter",
                    new IllegalArgumentException("Failed to parse coordinatorUrl: " + url, e));
        }
    }
}
