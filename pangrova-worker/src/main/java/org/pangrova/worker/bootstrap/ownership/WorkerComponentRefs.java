package org.pangrova.worker.bootstrap.ownership;

import org.pangrova.common.concurrency.NotThreadSafe;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.RootMemoryPool;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.PangrovaWorker;
import org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient;
import org.pangrova.worker.buffer.ShuffleBufferManager;
import org.pangrova.worker.heartbeat.HeartbeatSender;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.catalog.CatalogResourceResolver;
import org.pangrova.worker.write.NativeWriteExecutor;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.task.report.RemoteTaskStatusReporter;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPublisher;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Mutable holder for components created during Worker bootstrap phases. Each sub-bootstrap writes
 * its outputs here; downstream phases read them.
 *
 * <p>Assembly and cleanup writes are serialized by WorkerBootstrapLifecycle. Readiness and
 * heartbeat callbacks read installed owners after publication. Dynamic registration belongs to
 * the heartbeat scheduler, which must drain before cleanup mutates these references.
 *
 * <p>Internal to Worker assembly; not a public extension API.
 */
@NotThreadSafe
public final class WorkerComponentRefs {

    private org.pangrova.common.lifecycle.ProcessIncarnationStore admissionIncarnation;

    public org.pangrova.common.lifecycle.ProcessIncarnationStore admissionIncarnation() { return admissionIncarnation; }
    public void setAdmissionIncarnation(org.pangrova.common.lifecycle.ProcessIncarnationStore value) {
        admissionIncarnation = value;
    }

    private boolean nativeAvailable;
    private ResourceManager resourceManager;
    private RootMemoryPool rootMemoryPool;
    private NodeMemoryPool nodeMemoryPool;
    private org.pangrova.worker.resource.memory.ShuffleServiceMemoryLease shuffleServiceMemoryLease;
    private TaskManager taskManager;
    private WorkerTaskOwnership taskOwnership;
    private Runnable closeStatusReporter;
    private WorkerHttpServer httpServer;
    private HeartbeatSender heartbeatSender;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService metricsExecutor;
    private PangrovaWorker worker;
    private WorkerRegistrationClient registrationClient;
    private NativeMetricsPublisher nativeMetricsPublisher;
    private ShuffleBufferManager shuffleBufferManager;
    private AutoCloseable cdcConsumer;
    private org.pangrova.worker.execution.WorkerExecutedTaskBackend workerExecutedTaskBackend;
    private volatile int resultServerPort = -1;

    // -- Accessors --

    public boolean nativeAvailable() {
        return nativeAvailable;
    }

    public void setNativeAvailable(boolean v) {
        this.nativeAvailable = v;
    }

    public ResourceManager resourceManager() {
        return resourceManager;
    }

    public void setResourceManager(ResourceManager v) {
        this.resourceManager = v;
    }


    public RootMemoryPool rootMemoryPool() {
        return rootMemoryPool;
    }

    public void setRootMemoryPool(RootMemoryPool v) {
        this.rootMemoryPool = v;
    }

    public NodeMemoryPool nodeMemoryPool() {
        return nodeMemoryPool;
    }

    public void setNodeMemoryPool(NodeMemoryPool v) {
        this.nodeMemoryPool = v;
    }

    public org.pangrova.worker.resource.memory.ShuffleServiceMemoryLease
            shuffleServiceMemoryLease() {
        return shuffleServiceMemoryLease;
    }

    public void setShuffleServiceMemoryLease(
            org.pangrova.worker.resource.memory.ShuffleServiceMemoryLease v) {
        this.shuffleServiceMemoryLease = v;
    }

    public TaskManager taskManager() {
        return taskManager;
    }

    public void retainStatusReporter(RemoteTaskStatusReporter reporter) {
        closeStatusReporter = reporter::close;
    }

    public void closeStatusReporter() {
        if (closeStatusReporter == null) return;
        closeStatusReporter.run();
        closeStatusReporter = null;
    }

    public void setTaskManager(TaskManager v) {
        this.taskManager = v;
    }

    public WorkerTaskOwnership taskOwnership() {
        return taskOwnership;
    }

    /** Retains the new task bootstrap owner only after the previous owner has drained. */
    public void installTaskOwnership(WorkerTaskOwnership ownership) {
        if (taskOwnership != null && !taskOwnership.isDrained()) {
            throw new IllegalStateException("Previous task bootstrap owners have not drained");
        }
        taskOwnership = java.util.Objects.requireNonNull(ownership, "ownership");
    }

    public WorkerHttpServer httpServer() {
        return httpServer;
    }

    public void setHttpServer(WorkerHttpServer v) {
        this.httpServer = v;
    }

    public HeartbeatSender heartbeatSender() {
        return heartbeatSender;
    }

    public void setHeartbeatSender(HeartbeatSender v) {
        this.heartbeatSender = v;
    }

    public ScheduledExecutorService heartbeatExecutor() {
        return heartbeatExecutor;
    }

    public void setHeartbeatExecutor(ScheduledExecutorService v) {
        this.heartbeatExecutor = v;
    }

    public ScheduledExecutorService metricsExecutor() {
        return metricsExecutor;
    }

    public void setMetricsExecutor(ScheduledExecutorService v) {
        this.metricsExecutor = v;
    }

    public PangrovaWorker worker() {
        return worker;
    }

    public void setWorker(PangrovaWorker v) {
        this.worker = v;
    }

    public WorkerRegistrationClient registrationClient() {
        return registrationClient;
    }

    public void setRegistrationClient(WorkerRegistrationClient v) {
        this.registrationClient = v;
    }

    public NativeMetricsPublisher nativeMetricsPublisher() {
        return nativeMetricsPublisher;
    }

    public void setNativeMetricsPublisher(NativeMetricsPublisher v) {
        this.nativeMetricsPublisher = v;
    }


    public ShuffleBufferManager shuffleBufferManager() {
        return shuffleBufferManager;
    }

    public void setShuffleBufferManager(ShuffleBufferManager v) {
        this.shuffleBufferManager = v;
    }




    public AutoCloseable cdcConsumer() {
        return cdcConsumer;
    }

    public void setCdcConsumer(AutoCloseable v) {
        this.cdcConsumer = v;
    }

    /** Returns the pending JVM backend until task bootstrap takes ownership. */
    public org.pangrova.worker.execution.WorkerExecutedTaskBackend workerExecutedTaskBackend() {
        return workerExecutedTaskBackend;
    }

    public void setWorkerExecutedTaskBackend(
            org.pangrova.worker.execution.WorkerExecutedTaskBackend v) {
        this.workerExecutedTaskBackend = v;
    }

    public int resultServerPort() {
        return resultServerPort;
    }

    public void setResultServerPort(int v) {
        this.resultServerPort = v;
    }

    // F-CHAIN-CACHE-02: bounded LRU of recently-accessed file paths,
    // sampled into the heartbeat so the coordinator scheduler can route
    // splits to workers that have already touched the corresponding files.
    private org.pangrova.worker.cache.RecentFileAccessTracker recentFileAccessTracker;

    public org.pangrova.worker.cache.RecentFileAccessTracker recentFileAccessTracker() {
        return recentFileAccessTracker;
    }

    public void setRecentFileAccessTracker(org.pangrova.worker.cache.RecentFileAccessTracker v) {
        this.recentFileAccessTracker = v;
    }

    private org.pangrova.worker.function.NativeFunctionCallbacks nativeFunctionCallbacks;
    private final NativeFunctionResources nativeFunctionResources = new NativeFunctionResources();

    public org.pangrova.worker.function.NativeFunctionCallbacks nativeFunctionCallbacks() {
        return nativeFunctionCallbacks;
    }

    public void setNativeFunctionCallbacks(org.pangrova.worker.function.NativeFunctionCallbacks v) {
        this.nativeFunctionCallbacks = v;
    }

    public java.util.List<AutoCloseable> nativeFunctionResources() {
        return nativeFunctionResources.snapshot();
    }

    /** Retains a discovered factory before invoking any of its initialization methods. */
    public void retainNativeFunctionResource(AutoCloseable resource) {
        nativeFunctionResources.retain(resource);
    }

    /** Removes only successfully closed factories; failed owners remain retryable. */
    public void closeNativeFunctionResources() {
        nativeFunctionResources.close();
    }

    private ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport;

    private NativeWriteExecutor nativeWriteExecutor;

    private CatalogResourceResolver catalogResourceResolver;

    // Base64-encoded canonical v2 CapabilityInventory. Kept as the exact
    // protobuf payload for Coordinator scheduling and HA registry recovery.
    private volatile String nativeCapabilityInventory;

    public ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport() {
        return exchangeTransport;
    }

    public void setExchangeTransport(ExchangeTransport<TaskAttemptId, ExchangeInputBinding> v) {
        this.exchangeTransport = v;
    }

    public NativeWriteExecutor nativeWriteExecutor() {
        return nativeWriteExecutor;
    }

    public void setNativeWriteExecutor(NativeWriteExecutor v) {
        this.nativeWriteExecutor = v;
    }

    public CatalogResourceResolver catalogResourceResolver() {
        return catalogResourceResolver;
    }

    public void setCatalogResourceResolver(CatalogResourceResolver v) {
        this.catalogResourceResolver = v;
    }

    public String nativeCapabilityInventory() {
        return nativeCapabilityInventory;
    }

    public void setNativeCapabilityInventory(String v) {
        this.nativeCapabilityInventory = v;
    }

    // Process-local HMAC key material for producer-issued shuffle tickets.
    // The Coordinator and consuming workers never receive these raw bytes.
    private byte[] shuffleHmacCurrentKey;
    private byte[] shuffleHmacPreviousKey;

    public byte[] shuffleHmacCurrentKey() {
        return shuffleHmacCurrentKey == null ? null : shuffleHmacCurrentKey.clone();
    }

    public void setShuffleHmacCurrentKey(byte[] v) {
        this.shuffleHmacCurrentKey = v == null ? null : v.clone();
    }

    public byte[] shuffleHmacPreviousKey() {
        return shuffleHmacPreviousKey == null ? null : shuffleHmacPreviousKey.clone();
    }

    public void setShuffleHmacPreviousKey(byte[] v) {
        this.shuffleHmacPreviousKey = v == null ? null : v.clone();
    }

    private TenantResourceTracker tenantResourceTracker;

    public TenantResourceTracker tenantResourceTracker() {
        return tenantResourceTracker;
    }

    public void setTenantResourceTracker(TenantResourceTracker v) {
        this.tenantResourceTracker = v;
    }

    // Task 148: shared MemoryArbitrator so NativeMemoryCoordinator and
    // WorkerResourceBootstrap use a single unified memory budget.
    private org.pangrova.worker.resource.memory.MemoryArbitrator memoryArbitrator;

    public org.pangrova.worker.resource.memory.MemoryArbitrator memoryArbitrator() {
        return memoryArbitrator;
    }

    public void setMemoryArbitrator(org.pangrova.worker.resource.memory.MemoryArbitrator v) {
        this.memoryArbitrator = v;
    }
}
