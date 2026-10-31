package org.pangrova.worker.bootstrap.lifecycle;

import org.pangrova.observability.sdk.config.TelemetryInitializer;
import org.pangrova.observability.sdk.metrics.nativebridge.NativeMetricsPublisher;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;
import org.pangrova.worker.PangrovaWorker;
import org.pangrova.worker.bootstrap.ShutdownContext;
import org.pangrova.worker.heartbeat.HeartbeatSender;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.transport.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates the Worker shutdown sequence. Extracted from WorkerBootstrap to keep class size
 * within 500-line limit.
 */
public final class WorkerShutdownSequence {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerShutdownSequence.class);

    private WorkerShutdownSequence() {}

    /** Sends worker deregistration to the coordinator. */
    public static void deregister(WorkerRegistrationClient client, String workerId) {
        if (client != null) {
            client.deregister(workerId);
            LOG.info("event=deregister_sent workerId={}", workerId);
        }
    }

    /** Stops the heartbeat sender and its scheduler. */
    public static void stopHeartbeat(HeartbeatSender sender, ScheduledExecutorService executor) {
        var steps = new WorkerCleanupSteps();
        steps.attempt(
                () -> {
                    if (sender != null) sender.stop();
                });
        steps.attempt(() -> stopExecutor(executor));
        steps.requireSuccess();
    }

    /** Drains running tasks before shutdown. */
    public static void drainTasks(PangrovaWorker worker, TaskManager taskManager) {
        if (worker != null) {
            worker.stop();
        } else if (taskManager != null) {
            taskManager.drainAndWait();
        }
    }

    public static void stopTransport(TransportServer server) {
        if (server != null) {
            server.stop();
        }
    }

    /** Stops the worker HTTP server. */
    public static void stopHttp(WorkerHttpServer httpServer) {
        if (httpServer == null) return;
        try {
            httpServer.stop();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to stop Worker HTTP server", e);
        }
    }

    /** Releases shared resources held by the shutdown context. */
    public static void releaseResources(ShutdownContext ctx, Runnable nativeReleased) {
        var dependents = new WorkerCleanupSteps();
        dependents.attempt(() -> closeShuffleBufferManager(ctx.shuffleBufferManager()));
        dependents.attempt(
                () -> stopResultExpiration(ctx.schedulerRefs().resultExpirationExecutor()));
        dependents.attempt(
                () ->
                        stopMetrics(
                                ctx.schedulerRefs().metricsExecutor(),
                                ctx.resourceRefs().nativeMetricsPublisher()));
        dependents.requireSuccess();
        shutdownNative(ctx.resourceRefs().nativeAvailable(), nativeReleased);
        var resources = new WorkerCleanupSteps();
        resources.attempt(
                () -> {
                    if (ctx.resourceRefs().shuffleServiceMemoryLease() != null) {
                        ctx.resourceRefs().shuffleServiceMemoryLease().close();
                    }
                });
        resources.attempt(() -> closeObservability(ctx.resourceRefs().otelInitializer()));
        resources.requireSuccess();
        closeResourceManager(ctx.resourceRefs().resourceManager());
        LOG.info("event=resources_released");
    }

    private static void closeShuffleBufferManager(
            org.pangrova.worker.buffer.ShuffleBufferManager manager) {
        if (manager != null) {
            manager.close();
            LOG.info("event=shuffle_buffer_manager_shutdown");
        }
    }

    private static void stopResultExpiration(ScheduledExecutorService executor) {
        stopExecutor(executor);
    }

    /** Closes the resource manager and its thread pools. */
    public static void closeResourceManager(ResourceManager resourceManager) {
        if (resourceManager == null) return;
        ThreadBudgetTracker threadPool = resourceManager.getThreadPoolManager();
        if (threadPool != null) {
            threadPool.shutdown();
        }
        MemoryArbitrator arbitrator = resourceManager.getMemoryArbitrator();
        if (arbitrator != null) {
            arbitrator.clearAllocations();
        }
        LOG.info("event=resource_manager_closed");
    }

    private static void stopMetrics(
            ScheduledExecutorService executor, NativeMetricsPublisher publisher) {
        stopExecutor(executor);
        if (publisher != null) {
            publisher.stop();
        }
    }

    private static void stopExecutor(ScheduledExecutorService executor) {
        if (executor == null) return;
        if (Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("Worker scheduler drain interrupted");
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Worker scheduler has not exited");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Worker scheduler drain interrupted", failure);
        }
    }

    private static void shutdownNative(boolean nativeAvailable, Runnable nativeReleased) {
        if (!nativeAvailable) {
            nativeReleased.run();
            return;
        }
        try {
            NativeRuntime.shutdownNativeRuntime();
            nativeReleased.run();
            LOG.info("event=native_runtime_shutdown");
        } catch (RuntimeException | Error e) {
            LOG.error(
                    "event=native_runtime_shutdown_error lease_retained=true error={}",
                    e.getMessage(),
                    e);
            throw e;
        }
    }

    private static void closeObservability(TelemetryInitializer initializer) {
        if (initializer != null) {
            initializer.close();
        }
    }
}
