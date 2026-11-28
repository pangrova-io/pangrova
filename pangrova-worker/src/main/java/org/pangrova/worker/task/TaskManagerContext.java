package org.pangrova.worker.task;

import org.pangrova.worker.task.report.TaskStatusReporter;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import org.pangrova.worker.result.ResultTransport;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.shuffle.NativeShuffleProducerRegistrar;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.observability.api.tracing.TracingProvider;

import java.util.concurrent.ExecutorService;

/**
 * Groups all TaskManager dependencies into a single parameter object.
 * Uses {@link TransportRefs} for shuffle/result/exchange via the native data exchange layer.
 */
public final class TaskManagerContext {

    private final TaskStatusReporter reporter;
    private final TaskExecutor taskExecutor;
    private final MemoryRefs memoryDeps;
    private final TransportRefs transportDeps;
    private final ExecutorService completionMonitorExecutor;

    /** Native transport mode constructor (production path). */
    public TaskManagerContext(
            TaskStatusReporter reporter,
            TaskExecutor taskExecutor,
            MemoryRefs memoryDeps,
            TransportRefs transportDeps,
            ExecutorService completionMonitorExecutor) {
        this.reporter = checkNotNull(reporter, "reporter must not be null");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.memoryDeps = checkNotNull(memoryDeps, "memoryDeps must not be null");
        this.transportDeps = checkNotNull(transportDeps, "transportDeps must not be null");
        this.completionMonitorExecutor = checkNotNull(completionMonitorExecutor,
                "completionMonitorExecutor must not be null");
    }

    public TaskStatusReporter reporter() { return reporter; }
    TaskExecutor taskExecutor() { return taskExecutor; }
    public MemoryRefs memoryDeps() { return memoryDeps; }
    public TransportRefs transportDeps() { return transportDeps; }
    public ExecutorService completionMonitorExecutor() { return completionMonitorExecutor; }

    /** Unified resource lifecycle manager (memory, threads, spill). */
    public ResourceManager resourceManager() {
        return memoryDeps.resourceManager();
    }

    /** Distributed tracing provider (nullable). */
    public TracingProvider tracingProvider() {
        return memoryDeps.tracingProvider();
    }

    /** Hierarchical node memory pool (nullable). */
    public NodeMemoryPool nodeMemoryPool() {
        return memoryDeps.nodeMemoryPool();
    }

    /** Task-owned Native serving adapter. */
    public NativeShuffleProducerRegistrar shuffleProducers() {
        return transportDeps.shuffleProducers();
    }

    /** Result transport SPI. */
    public ResultTransport resultTransport() {
        return transportDeps.resultTransport();
    }

    /** Exchange transport SPI. */
    public ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport() {
        return transportDeps.exchangeTransport();
    }
}
