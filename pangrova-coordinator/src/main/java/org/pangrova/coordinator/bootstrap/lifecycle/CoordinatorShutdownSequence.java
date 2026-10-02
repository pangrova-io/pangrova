package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.PangrovaCoordinator;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryTimeoutEnforcer;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerHealthScheduler;
import org.pangrova.observability.config.TelemetryInitializer;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.spi.transport.TransportServerLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;

/**
 * Encapsulates the Coordinator shutdown sequence and dead-worker handling.
 * Extracted from CoordinatorBootstrap to keep class size within 500-line limit.
 */
final class CoordinatorShutdownSequence {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorShutdownSequence.class);

    private CoordinatorShutdownSequence() {}

    static void stopTimeoutEnforcer(QueryTimeoutEnforcer enforcer) {
        if (enforcer != null) {
            enforcer.stop();
            LOG.info("Phase: QueryTimeoutEnforcer — stopped");
        }
    }

    static void drainQueries(PangrovaCoordinator coordinator) {
        if (coordinator != null) {
            coordinator.stop();
        }
    }

    static void closeAuditEmitter(AuditEventEmitter emitter) {
        if (emitter != null) {
            emitter.close();
        }
    }

    static void stopCoordinatorTransport(TransportServerLifecycle server) {
        if (server != null) {
            server.stop();
            LOG.info("Phase: CoordinatorTransport — stopped");
        }
    }

    static void closeTransportAllocator(org.apache.arrow.memory.BufferAllocator allocator) {
        if (allocator == null) {
            return;
        }
        try {
            allocator.close();
            LOG.info("event=coordinator_transport_allocator_closed");
        } catch (Exception e) {
            LOG.warn("event=coordinator_transport_allocator_close_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * Shuts down the scheduler executor as a fallback during shutdown.
     * Prevents thread leak if HA bootstrap fails before normal executor shutdown.
     */
    static void shutdownSchedulerExecutor(ScheduledExecutorService executor) {
        if (executor == null) {
            return;
        }
        try {
            executor.shutdownNow();
            LOG.info("event=scheduler_executor_shutdown_fallback");
        } catch (Exception e) {
            LOG.warn("event=scheduler_executor_shutdown_failed error={}", e.getMessage(), e);
        }
    }

    static void stopHealthScheduler(WorkerHealthScheduler scheduler,
                                     ScheduledExecutorService executor) {
        if (scheduler != null) {
            scheduler.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    static void stopHttp(CoordinatorHttpServer httpServer) {
        if (httpServer == null) return;
        try {
            httpServer.stop();
        } catch (Exception e) {
            LOG.warn("Error stopping HTTP server", e);
        }
    }

    static void closeModules(ModuleIntegrationBootstrap wiring) {
        if (wiring != null) {
            wiring.close();
        }
    }

    static void closeQueryResultCache(org.pangrova.planner.cache.QueryResultCache cache) {
        if (cache == null) return;
        try {
            cache.close();
        } catch (Exception failure) {
            LOG.warn("event=query_cache_close_failed error={}", failure.getMessage(), failure);
        }
    }

    static void closeStreamingBootstrap(StreamingBootstrap streaming) {
        if (streaming == null) return;
        try {
            streaming.close();
            LOG.info("event=phase_closed phase=streaming_bootstrap");
        } catch (Exception e) {
            LOG.warn("event=phase_close_failed phase=streaming_bootstrap error={}", e.getMessage(), e);
        }
    }

    static void closeBranchCleanup(org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask cleanup) {
        if (cleanup == null) return;
        try {
            cleanup.close();
            LOG.info("event=phase_closed phase=branch_ttl_cleanup");
        } catch (Exception e) {
            LOG.warn("event=phase_close_failed phase=branch_ttl_cleanup error={}", e.getMessage(), e);
        }
    }

    static void closeMetadata(MetadataBootstrap bootstrap,
                               PluginConnectorHandleFactory factory,
                               DataSource dataSource) {
        if (bootstrap != null) {
            bootstrap.shutdown();
        }
        if (factory != null) {
            factory.shutdown();
        }
        closeDataSource(dataSource);
        LOG.info("Phase: metadata — closed");
    }

    static void closeDataSource(DataSource dataSource) {
        if (dataSource instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("Error closing metadata DataSource", e);
            }
        }
    }

    static void closeObservability(TelemetryInitializer initializer) {
        if (initializer != null) {
            initializer.close();
        }
        LOG.info("Phase: observability — closed");
    }

    // ── Dead worker handling ──

    static void onDeadWorker(String workerId,
                              CoordinatorTaskTracker taskTracker,
                              QueryManager queryManager) {
        Set<String> affectedQueries = drainDeadWorkerQueries(workerId, taskTracker);
        markDeadWorkerTasksFailed(workerId, taskTracker);
        failAffectedQueries(workerId, affectedQueries, queryManager);
        logDeadWorkerDrain(workerId, affectedQueries);
    }

    /**
     * Marks all non-terminal tasks on the dead worker as FAILED in the tracker.
     * This ensures internal state consistency: tasks on a dead worker should not
     * remain in RUNNING state after the worker is declared dead.
     */
    private static void markDeadWorkerTasksFailed(String workerId,
                                                   CoordinatorTaskTracker taskTracker) {
        if (taskTracker == null) return;
        Map<String, Set<String>> tasksByQuery = taskTracker.getActiveTasksByQueryForWorker(workerId);
        int count = 0;
        for (Map.Entry<String, Set<String>> entry : tasksByQuery.entrySet()) {
            String queryId = entry.getKey();
            for (String taskId : entry.getValue()) {
                taskTracker.updateTaskState(queryId, taskId, "FAILED");
                count++;
            }
        }
        if (count > 0) {
            LOG.info("Marked {} tasks as FAILED for dead worker {}", count, workerId);
        }
    }

    private static Set<String> drainDeadWorkerQueries(String workerId,
                                                       CoordinatorTaskTracker taskTracker) {
        if (taskTracker == null) return Set.of();
        return taskTracker.getQueriesForWorker(workerId);
    }

    private static void failAffectedQueries(String workerId,
                                              Set<String> queryIds,
                                              QueryManager queryManager) {
        if (queryManager == null || queryIds.isEmpty()) return;
        ErrorDetail error = new ErrorDetail(
                "WORKER_LOST", "WORKER_LOST",
                "Worker " + workerId + " declared dead — failing query");
        for (String queryId : queryIds) {
            queryManager.onTaskFailed(queryId, error);
        }
    }

    private static void logDeadWorkerDrain(String workerId, Set<String> queryIds) {
        int count = queryIds.size();
        if (count == 0) {
            LOG.warn("Dead worker detected: {} — no in-flight queries affected", workerId);
            return;
        }
        LOG.error("Dead worker {} — failing {} affected queries: {}",
                workerId, count, queryIds);
        LOG.info("METRIC coordinator.worker.dead_removal_queries_failed count={}", count);
    }

    // ── Task timeout handling ──

    static void checkTaskTimeouts(CoordinatorTaskTracker taskTracker,
                                   QueryManager queryManager) {
        if (taskTracker == null || queryManager == null) return;
        Map<String, String> timedOut = taskTracker.getTimedOutTasks(java.time.Instant.now());
        if (timedOut.isEmpty()) return;
        failTimedOutTasks(timedOut, taskTracker, queryManager);
    }

    private static void failTimedOutTasks(Map<String, String> timedOut,
                                           CoordinatorTaskTracker taskTracker,
                                           QueryManager queryManager) {
        Set<String> failedQueryIds = new java.util.HashSet<>();
        for (Map.Entry<String, String> entry : timedOut.entrySet()) {
            String taskId = entry.getKey();
            String queryId = entry.getValue();
            taskTracker.removeDeadline(taskId);
            var request = taskTracker.getTaskRequest(taskId);
            if (request != null && (request.taskRequest().getTaskKind()
                    == org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_BUILD
                    || request.taskRequest().getTaskKind()
                    == org.pangrova.protocol.v2.TaskKind.TASK_KIND_BACKGROUND_EXPORT)) {
                taskTracker.updateTaskState(queryId, taskId, "FAILED");
                taskTracker.signalTaskTerminal(queryId, taskId);
                LOG.error("METRIC coordinator.artifact_task.timeout_total taskId={} jobId={}",
                        taskId, queryId);
                continue;
            }
            if (failedQueryIds.add(queryId)) {
                ErrorDetail error = new ErrorDetail(
                        "TASK_TIMEOUT", "TASK_TIMEOUT",
                        "Task " + taskId + " exceeded deadline for query " + queryId);
                queryManager.onTaskFailed(queryId, error);
            }
            LOG.error("METRIC coordinator.task.timeout_total taskId={} queryId={}", taskId, queryId);
        }
        LOG.warn("Task timeout check: {} tasks timed out across {} queries",
                timedOut.size(), failedQueryIds.size());
    }
}
