package org.pangrova.worker.bootstrap;

import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.ResourceMonitor;
import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.resource.tenant.TenantResourceTracker;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.PangrovaWorker;
import org.pangrova.worker.bootstrap.lifecycle.WorkerRegistrationClient;
import org.pangrova.worker.buffer.ActiveOutputBuffer;
import org.pangrova.worker.buffer.ResultExpirationScheduler;
import org.pangrova.worker.buffer.ShuffleBufferManager;
import org.pangrova.worker.heartbeat.HeartbeatSender;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.jni.write.CatalogResourceResolver;
import org.pangrova.worker.jni.write.NativeWriteExecutor;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.transport.TransportServer;
import org.pangrova.observability.metrics.CacheMetricsPublisher;
import org.pangrova.observability.metrics.NativeMetricsPublisher;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Mutable holder for components created during Worker bootstrap phases.
 * Each sub-bootstrap writes its outputs here; downstream phases read them.
 *
 * <p>This class is intentionally mutable and NOT thread-safe — it is only
 * accessed sequentially during the single-threaded bootstrap sequence.
 * After bootstrap completes, the refs are read-only.</p>
 *
 * <p>Package-private — not part of the public API.</p>
 */
public final class WorkerComponentRefs {

    private boolean nativeAvailable;
    private ResourceManager resourceManager;
    private ResourceMonitor resourceMonitor;
    private RootMemoryPool rootMemoryPool;
    private NodeMemoryPool nodeMemoryPool;
    private TaskManager taskManager;
    private WorkerHttpServer httpServer;
    private HeartbeatSender heartbeatSender;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService metricsExecutor;
    private PangrovaWorker worker;
    private WorkerRegistrationClient registrationClient;
    private NativeMetricsPublisher nativeMetricsPublisher;
    private CacheMetricsPublisher cacheMetricsPublisher;
    private ShuffleBufferManager shuffleBufferManager;
    private ActiveOutputBuffer activeOutputBuffer;
    private ResultExpirationScheduler resultExpirationScheduler;
    private TransportServer transportServer;
    private AutoCloseable cdcConsumer;
    private volatile int resultServerPort = -1;

    // -- Accessors --

    public boolean nativeAvailable() { return nativeAvailable; }
    public void setNativeAvailable(boolean v) { this.nativeAvailable = v; }

    public ResourceManager resourceManager() { return resourceManager; }
    public void setResourceManager(ResourceManager v) { this.resourceManager = v; }

    public ResourceMonitor resourceMonitor() { return resourceMonitor; }
    public void setResourceMonitor(ResourceMonitor v) { this.resourceMonitor = v; }

    public RootMemoryPool rootMemoryPool() { return rootMemoryPool; }
    public void setRootMemoryPool(RootMemoryPool v) { this.rootMemoryPool = v; }

    public NodeMemoryPool nodeMemoryPool() { return nodeMemoryPool; }
    public void setNodeMemoryPool(NodeMemoryPool v) { this.nodeMemoryPool = v; }

    public TaskManager taskManager() { return taskManager; }
    public void setTaskManager(TaskManager v) { this.taskManager = v; }

    public WorkerHttpServer httpServer() { return httpServer; }
    public void setHttpServer(WorkerHttpServer v) { this.httpServer = v; }

    public HeartbeatSender heartbeatSender() { return heartbeatSender; }
    public void setHeartbeatSender(HeartbeatSender v) { this.heartbeatSender = v; }

    public ScheduledExecutorService heartbeatExecutor() { return heartbeatExecutor; }
    public void setHeartbeatExecutor(ScheduledExecutorService v) { this.heartbeatExecutor = v; }

    public ScheduledExecutorService metricsExecutor() { return metricsExecutor; }
    public void setMetricsExecutor(ScheduledExecutorService v) { this.metricsExecutor = v; }

    public PangrovaWorker worker() { return worker; }
    public void setWorker(PangrovaWorker v) { this.worker = v; }

    public WorkerRegistrationClient registrationClient() { return registrationClient; }
    public void setRegistrationClient(WorkerRegistrationClient v) { this.registrationClient = v; }

    public NativeMetricsPublisher nativeMetricsPublisher() { return nativeMetricsPublisher; }
    public void setNativeMetricsPublisher(NativeMetricsPublisher v) { this.nativeMetricsPublisher = v; }

    public CacheMetricsPublisher cacheMetricsPublisher() { return cacheMetricsPublisher; }
    public void setCacheMetricsPublisher(CacheMetricsPublisher v) { this.cacheMetricsPublisher = v; }

    public ShuffleBufferManager shuffleBufferManager() { return shuffleBufferManager; }
    public void setShuffleBufferManager(ShuffleBufferManager v) { this.shuffleBufferManager = v; }

    public ActiveOutputBuffer activeOutputBuffer() { return activeOutputBuffer; }
    public void setActiveOutputBuffer(ActiveOutputBuffer v) { this.activeOutputBuffer = v; }

    public ResultExpirationScheduler resultExpirationScheduler() { return resultExpirationScheduler; }
    public void setResultExpirationScheduler(ResultExpirationScheduler v) { this.resultExpirationScheduler = v; }

    public TransportServer transportServer() { return transportServer; }
    public void setTransportServer(TransportServer v) { this.transportServer = v; }

    public AutoCloseable cdcConsumer() { return cdcConsumer; }
    public void setCdcConsumer(AutoCloseable v) { this.cdcConsumer = v; }

    public int resultServerPort() { return resultServerPort; }
    public void setResultServerPort(int v) { this.resultServerPort = v; }

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

    private org.pangrova.worker.jni.NativeFunctionBridge nativeFunctionBridge;

    public org.pangrova.worker.jni.NativeFunctionBridge nativeFunctionBridge() { return nativeFunctionBridge; }
    public void setNativeFunctionBridge(org.pangrova.worker.jni.NativeFunctionBridge v) { this.nativeFunctionBridge = v; }

    private ExchangeTransport exchangeTransport;

    private NativeWriteExecutor nativeWriteExecutor;

    private CatalogResourceResolver catalogResourceResolver;

    private volatile java.util.List<String> nativeCapabilities = java.util.List.of();

    public ExchangeTransport exchangeTransport() { return exchangeTransport; }
    public void setExchangeTransport(ExchangeTransport v) { this.exchangeTransport = v; }

    public NativeWriteExecutor nativeWriteExecutor() { return nativeWriteExecutor; }
    public void setNativeWriteExecutor(NativeWriteExecutor v) { this.nativeWriteExecutor = v; }

    public CatalogResourceResolver catalogResourceResolver() { return catalogResourceResolver; }
    public void setCatalogResourceResolver(CatalogResourceResolver v) { this.catalogResourceResolver = v; }

    public java.util.List<String> nativeCapabilities() { return nativeCapabilities; }
    public void setNativeCapabilities(java.util.List<String> v) {
        this.nativeCapabilities = v == null ? java.util.List.of() : java.util.List.copyOf(v);
    }

    // HMAC key(s) for shuffle ticket verification, received from Coordinator
    // during worker registration. Raw bytes (not base64).
    private byte[] shuffleHmacCurrentKey;
    private byte[] shuffleHmacPreviousKey;

    public byte[] shuffleHmacCurrentKey() { return shuffleHmacCurrentKey; }
    public void setShuffleHmacCurrentKey(byte[] v) { this.shuffleHmacCurrentKey = v; }

    public byte[] shuffleHmacPreviousKey() { return shuffleHmacPreviousKey; }
    public void setShuffleHmacPreviousKey(byte[] v) { this.shuffleHmacPreviousKey = v; }

    private TenantResourceTracker tenantResourceTracker;

    public TenantResourceTracker tenantResourceTracker() { return tenantResourceTracker; }
    public void setTenantResourceTracker(TenantResourceTracker v) { this.tenantResourceTracker = v; }

    // Task 148: shared MemoryArbitrator so NativeMemoryCoordinator and
    // WorkerResourceBootstrap use a single unified memory budget.
    private org.pangrova.resource.memory.MemoryArbitrator memoryArbitrator;

    public org.pangrova.resource.memory.MemoryArbitrator memoryArbitrator() { return memoryArbitrator; }
    public void setMemoryArbitrator(org.pangrova.resource.memory.MemoryArbitrator v) { this.memoryArbitrator = v; }
}
