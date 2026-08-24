package org.pangrova.worker.bootstrap.lifecycle;

import org.pangrova.observability.config.TelemetryInitializer;
import org.pangrova.observability.metrics.NativeMetricsPublisher;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.worker.PangrovaWorker;
import org.pangrova.worker.bootstrap.ShutdownContext;
import org.pangrova.worker.heartbeat.HeartbeatSender;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.worker.transport.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Encapsulates the Worker shutdown sequence.
 * Extracted from WorkerBootstrap to keep class size within 500-line limit.
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
    public static void stopHeartbeat(HeartbeatSender sender,
                               ScheduledExecutorService executor) {
        if (sender != null) {
            sender.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
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
            LOG.warn("event=http_server_stop_error error={}", e.getMessage(), e);
        }
    }

    /** Releases shared resources held by the shutdown context. */
    public static void releaseResources(ShutdownContext ctx) {
        closeShuffleBufferManager(ctx.shuffleBufferManager());
        stopResultExpiration(ctx.schedulerRefs().resultExpirationExecutor());
        stopMetrics(ctx.schedulerRefs().metricsExecutor(), ctx.resourceRefs().nativeMetricsPublisher());
        shutdownNative(ctx.resourceRefs().nativeAvailable());
        closeResourceManager(ctx.resourceRefs().resourceManager());
        closeObservability(ctx.resourceRefs().otelInitializer());
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
        if (executor != null) {
            executor.shutdownNow();
            LOG.info("event=result_expiration_executor_shutdown");
        }
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

    private static void stopMetrics(ScheduledExecutorService executor,
                                     NativeMetricsPublisher publisher) {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (publisher != null) {
            publisher.stop();
        }
    }

    private static void shutdownNative(boolean nativeAvailable) {
        if (!nativeAvailable) {
            return;
        }
        try {
            NativeRuntime.shutdownNativeRuntime();
            LOG.info("event=native_runtime_shutdown");
        } catch (Exception e) {
            LOG.warn("event=native_runtime_shutdown_error error={}", e.getMessage());
        }
    }

    private static void closeObservability(TelemetryInitializer initializer) {
        if (initializer != null) {
            initializer.close();
        }
    }
}
