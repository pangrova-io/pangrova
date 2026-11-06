package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.query.core.task.DispatchResult;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.RuntimeInitConfig;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.memory.RootMemoryPool;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.worker.bootstrap.NativeCapabilityAdvertisement;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.resource.memory.ShuffleServiceMemoryLease;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.nativebridge.jni.NativeFunctionConfig;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.pangrova.worker.memory.NativeMemoryCoordinator;
import org.pangrova.worker.result.NativeResultTransport;
import org.pangrova.worker.result.ResultReadTickets;
import org.pangrova.shuffle.adapter.nativebridge.NativeShuffleTransport;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.TaskManagerContext;
import org.pangrova.worker.task.TransportRefs;
import org.pangrova.worker.task.memory.MemoryRefs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Real source-free HTTP/JNI execution. One fixture exclusively owns the JVM Native runtime. */
public final class SourceFreeNativeWorkerFixture implements AutoCloseable {
    private static final Object OWNERSHIP = new Object();
    private static boolean owned;
    public static final long MEMORY_BYTES = 1024L * 1024 * 1024;
    private static final String LOOPBACK_HOST = "127.0.0.1";
    private final String workerId;
    private final MemoryArbitrator arbitrator = new MemoryArbitrator(MEMORY_BYTES, 64L * 1024 * 1024);
    private final ThreadBudgetTracker threads = new ThreadBudgetTracker(4, 2, 1);
    private final ExecutorService dispatchExecutor = Executors.newCachedThreadPool();
    private final ExecutorService monitors = Executors.newCachedThreadPool();
    private final AtomicReference<Consumer<TaskStatusReport>> sink = new AtomicReference<>();
    private final CopyOnWriteArrayList<TaskStatusReport> reports = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<QueryManager> ownedQueries = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<WorkerNode> workers = new CopyOnWriteArrayList<>();
    private final java.util.concurrent.ConcurrentHashMap<String, List<List<Object>>> rows =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicReference<Throwable> reportFailure = new AtomicReference<>();
    private final WorkerComponentRefs nativeRefs = new WorkerComponentRefs();
    private Path admissionDirectory;
    private final java.util.List<org.pangrova.common.lifecycle.ProcessIncarnationStore> admissionIncarnations =
            new java.util.ArrayList<>();
    private ShuffleServiceMemoryLease serviceLease;
    private NativeTaskExecutor executor;
    private NativeResultTransport results;
    private ResultReadTickets resultTickets;
    private TaskManager manager;
    private WorkerHttpServer server;
    private MemoryRefs memory;
    private TaskStatusReceiver receiver;
    private boolean closed;

    public SourceFreeNativeWorkerFixture(String workerId, Path directory) throws Exception {
        this.workerId = java.util.Objects.requireNonNull(workerId);
        synchronized (OWNERSHIP) {
            if (owned || NativeRuntime.isInitialized()) {
                throw new IllegalStateException("Source-free fixture cannot borrow another Native runtime owner");
            }
            owned = true;
        }
        try {
            admissionDirectory = directory.resolve("worker-admission");
            initialize(directory);
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void initialize(Path directory) throws Exception {
        Files.createDirectories(directory);
        var node = new RootMemoryPool("source-free-root", MEMORY_BYTES)
                .createNodePool("source-free-node", MEMORY_BYTES);
        serviceLease = ShuffleServiceMemoryLease.reserve(
                workerId, 16L * 1024 * 1024, 16L * 1024 * 1024, node, arbitrator);
        var config = RuntimeInitConfig.newBuilder().setMaxMemoryBytes(MEMORY_BYTES).setIoThreadCount(2)
                .setShuffleRetentionBytes(16L * 1024 * 1024).setShuffleTransitBytes(16L * 1024 * 1024)
                .setShuffleSpillBytes(MEMORY_BYTES).setShuffleSpillFiles(256)
                .setShuffleSpillDirectory(directory.toAbsolutePath().toString()).build();
        NativeRuntime.initNativeRuntime(config.toByteArray(), new NativeMemoryCoordinator(MEMORY_BYTES, arbitrator));
        nativeRefs.setNativeAvailable(true);
        // Match the advertised core-only configuration and publish the actual runtime-owned catalogue
        // before any capability read, transport server or task admission.
        NativeFunctionBootstrap.initialize(nativeRefs, NativeFunctionConfig.DISABLED);
        byte[] key = new byte[32];
        new java.security.SecureRandom().nextBytes(key);
        NativeRuntime.nativeSetHmacKey(key, new byte[0]);
        resultTickets = new ResultReadTickets(key);
        executor = new NativeTaskExecutor();
        results = new NativeResultTransport(executor, LOOPBACK_HOST, LOOPBACK_HOST);
        results.startServer(0);
        memory = new MemoryRefs(new ResourceManager(arbitrator, threads, new SpillManager()), node, null);
        WorkerNode primary = addWorker(workerId);
        manager = primary.taskManager();
        server = primary.httpServer();
    }

    /** Additional real HTTP routing node, not another process or independent Native capacity. */
    public WorkerNode addWorker(String id) throws Exception {
        if (workers.stream().anyMatch(worker -> worker.workerId().equals(id))) {
            throw new IllegalArgumentException("Duplicate fixture worker: " + id);
        }
        var exchange = new NativeExchangeTransport(executor);
        var context = new TaskManagerContext(this::report, executor, memory,
                new TransportRefs(new NativeShuffleTransport(executor::executeCommand, LOOPBACK_HOST, ""), results, exchange), monitors);
        var admissionIncarnation = org.pangrova.common.lifecycle.ProcessIncarnationStore.open(
                admissionDirectory.resolve(java.net.URLEncoder.encode(id, java.nio.charset.StandardCharsets.UTF_8)));
        admissionIncarnations.add(admissionIncarnation);
        var tasks = new TaskManager(context, id, 0.0, admissionIncarnation);
        tasks.requireControlEpochHandshake();
        tasks.observeControlEpoch(1L);
        var http = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(0, LOOPBACK_HOST, results.boundPort())
                        .withBindHost(LOOPBACK_HOST),
                tasks, exchange, null, null, resultTickets);
        var worker = new WorkerNode(id, tasks, http);
        workers.add(worker);
        http.start();
        return worker;
    }

    public record WorkerNode(String workerId, TaskManager taskManager, WorkerHttpServer httpServer) { }

    private void report(TaskStatusReport report) {
        reports.add(report);
        try {
            java.util.Objects.requireNonNull(sink.get(), "Status receiver must be connected before dispatch").accept(report);
        } catch (RuntimeException | Error failure) {
            reportFailure.compareAndSet(null, failure);
            throw failure;
        }
    }

    public WorkerHttpServer httpServer() { return server; }
    public TaskManager taskManager() { return manager; }
    public TaskStatusReceiver statusReceiver() { return java.util.Objects.requireNonNull(receiver); }

    /** Uses the actual producer-local signer to check the exact issued result authority. */
    public String resultTicket(org.pangrova.protocol.v2.TaskRequest request) {
        return java.util.Objects.requireNonNull(resultTickets).issue(request);
    }

    public WorkerInfo workerInfo() {
        return workerInfo(workers.get(0));
    }

    public WorkerInfo workerInfo(WorkerNode worker) {
        var advertisement = NativeCapabilityAdvertisement.load(NativeFunctionConfig.DISABLED);
        return new WorkerInfo(worker.workerId(), LOOPBACK_HOST, worker.httpServer().getPort(), results.boundPort(), results.boundPort(),
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0, MEMORY_BYTES,
                Instant.now(), 1.0, null, 2, null, List.of(), null, advertisement.inventory(),
                worker.taskManager().workerExecutionIdentity());
    }

    public QueryManager connectAsync(QueryDispatcher dispatcher, CoordinatorTaskTracker tracker, WorkerManager workers) {
        var queries = new QueryManager(QueryManagerTestContexts.minimalAsync(dispatcher, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), dispatchExecutor));
        ownedQueries.add(queries);
        connect(dispatcher, tracker, workers, queries);
        return queries;
    }

    /** Replaces only report delivery; the collector still creates every report from the real task. */
    public void deliverReportsTo(Consumer<TaskStatusReport> receiver) {
        sink.set(java.util.Objects.requireNonNull(receiver));
    }

    public QueryManager connect(QueryDispatcher dispatcher, CoordinatorTaskTracker tracker, WorkerManager workers) {
        var queries = new QueryManager(QueryManagerTestContexts.minimal(dispatcher, TestObservability.TRACING));
        ownedQueries.add(queries);
        connect(dispatcher, tracker, workers, queries);
        return queries;
    }

    public void connect(QueryDispatcher dispatcher, CoordinatorTaskTracker tracker,
                        WorkerManager workers, QueryManager queries) {
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(queries, tracker, workers,
                null, null, null, null, null, null, dispatcher.dagEngine(), null, new org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal()));
        sink.set(report -> {
            if (!receiver.onTaskStatus(report)) {
                throw new IllegalStateException("Real Worker report was rejected: " + report.taskId());
            }
        });
    }

    /** Runs dispatch concurrently with the real client whose EOF permits Native OUTPUT completion. */
    public DispatchResult execute(QueryDispatcher dispatcher, QueryStateMachine state,
                                  String sql, QuerySession session) {
        var future = dispatchExecutor.submit(() -> dispatcher.execute(state, sql, session));
        try {
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30))
                    .until(() -> state.getResultEndpoint().isPresent() || future.isDone());
            if (state.getResultEndpoint().isPresent()) {
                consume(state.getResultEndpoint().orElseThrow());
            }
            DispatchResult result = future.get(30, TimeUnit.SECONDS);
            if (reportFailure.get() != null) throw new IllegalStateException("Native report delivery failed", reportFailure.get());
            return result;
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            throw new IllegalStateException("Native dispatch failed", failure.getCause());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Native source-free execution was interrupted", interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("Native source-free execution did not complete", failure);
        } finally {
            if (!future.isDone()) future.cancel(true);
        }
    }

    public List<TaskStatusReport> reports() { return List.copyOf(reports); }
    public List<List<Object>> rows(DispatchResult result) { return rows.get(result.resultEndpoint().ticket()); }
    public List<List<Object>> rows(ResultEndpoint endpoint) { return rows.get(endpoint.ticket()); }

    /** Observes the actual async dispatch executor; does not complete any query or task. */
    public void awaitDispatchesDrained() {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                ((java.util.concurrent.ThreadPoolExecutor) dispatchExecutor).getActiveCount() == 0);
        if (reportFailure.get() != null) {
            throw new IllegalStateException("Native report delivery failed", reportFailure.get());
        }
    }

    public void consume(ResultEndpoint endpoint) {
        var location = org.apache.arrow.flight.Location.forGrpcInsecure(endpoint.host(), endpoint.port());
        try (var allocator = new org.apache.arrow.memory.RootAllocator(16L * 1024 * 1024);
                var client = org.apache.arrow.flight.FlightClient.builder(allocator, location).build();
                var stream = client.getStream(new org.apache.arrow.flight.Ticket(
                        endpoint.ticket().getBytes(StandardCharsets.UTF_8)),
                        org.apache.arrow.flight.CallOptions.timeout(30, TimeUnit.SECONDS))) {
            var consumed = new java.util.ArrayList<List<Object>>();
            while (stream.next()) {
                // A real consumer must read through EOF; availability is not task completion.
                var root = stream.getRoot();
                for (int row = 0; row < root.getRowCount(); row++) {
                    var values = new java.util.ArrayList<Object>();
                    for (var vector : root.getFieldVectors()) values.add(vector.getObject(row));
                    consumed.add(java.util.Collections.unmodifiableList(values));
                }
            }
            rows.put(endpoint.ticket(), List.copyOf(consumed));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Native result cleanup was interrupted", interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("Native result consumption or cleanup failed", failure);
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) return;
        closed = true;
        var failures = new java.util.ArrayList<Exception>();
        try {
            cleanup(failures, this::drainOwnedLifecycle);
            cleanup(failures, () -> { if (results != null) results.close(); });
            cleanup(failures, () -> {
                if (!workers.isEmpty()) workers.forEach(worker -> worker.taskManager().close());
                else if (executor != null) executor.close();
            });
            cleanup(failures, this::stopOrchestrationThreads);
        } finally {
            cleanup(failures, () -> NativeFunctionBootstrap.close(nativeRefs));
            cleanup(failures, NativeRuntime::shutdownNativeRuntime);
            cleanup(failures, () -> { if (serviceLease != null) serviceLease.close(); });
            cleanup(failures, threads::shutdown);
            cleanup(failures, arbitrator::clearAllocations);
            synchronized (OWNERSHIP) { owned = NativeRuntime.isInitialized(); }
        }
        if (!failures.isEmpty()) {
            Exception first = failures.get(0);
            failures.stream().skip(1).forEach(first::addSuppressed);
            throw first;
        }
        for (var incarnation : admissionIncarnations) incarnation.closeAfterDrain(() -> {});
    }

    private void drainOwnedLifecycle() throws Exception {
        ownedQueries.forEach(queries -> queries.setAcceptingQueries(false));
        for (var worker : workers) worker.httpServer().stop();
        for (var worker : workers) {
            worker.taskManager().cancelAllRunningTasks();
            worker.taskManager().drainAndWait(5);
            worker.taskManager().releaseTerminalTaskExecutors();
        }
        // Task-state drain precedes the final collector report: let reporters finish before interruption.
        monitors.shutdown();
        if (!monitors.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Native completion/report threads did not drain");
        }
        for (var queries : ownedQueries) {
            queries.getActiveQueryIds().forEach(queries::cancelQuery);
            if (!queries.awaitAllQueries(5_000L)) {
                throw new IllegalStateException("Fixture-owned QueryManager did not drain");
            }
            queries.cleanupTerminalQueries(Duration.ZERO);
        }
    }

    private void stopOrchestrationThreads() throws InterruptedException {
        monitors.shutdownNow();
        dispatchExecutor.shutdown();
        // Dispatch joins are not interruptible. Await their real 30s query deadline, never fake completion.
        if (!monitors.awaitTermination(5, TimeUnit.SECONDS)
                || !dispatchExecutor.awaitTermination(35, TimeUnit.SECONDS)) {
            dispatchExecutor.shutdownNow();
            throw new IllegalStateException("Source-free orchestration threads did not stop");
        }
    }

    private static void cleanup(List<Exception> failures, Cleanup action) {
        try {
            action.run();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failures.add(interrupted);
        } catch (Exception failure) {
            failures.add(failure);
        }
    }

    @FunctionalInterface private interface Cleanup { void run() throws Exception; }
}
