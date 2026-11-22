package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.lifecycle.ResourceCloser;

import org.pangrova.server.coordinator.lifecycle.PangrovaCoordinator;
import org.pangrova.server.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission;
import org.pangrova.server.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.lifecycle.QueryTimeoutScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerHealthScheduler;
import org.pangrova.observability.sdk.config.TelemetryInitializer;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.protocol.transport.TransportServerLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.pangrova.common.lifecycle.ResourceCloser.present;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;

/**
 * Encapsulates the Coordinator shutdown sequence and dead-worker handling.
 * Extracted from CoordinatorBootstrap to keep class size within 500-line limit.
 */
final class CoordinatorShutdownSequence {
    private static final long DRAIN_TIMEOUT_MS = 5_000;

    record ShutdownOwners(QueryBootstrap query, HighAvailabilityBootstrap ha,
            MaintenanceLeadershipBootstrap maintenanceLeadership,
            org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask branchTtlCleanupTask,
            StreamingBootstrap streamingBootstrap, JournaledCdcRuntime cdcRuntime,
            HttpBootstrap http, ModuleIntegrationBootstrap moduleWiring,
            NativeDatasetControlBootstrap nativeDatasetControl, MetadataBootstrap metadataBootstrap,
            PluginConnectorHandleFactory connectorHandleFactory,
            org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority executionAuthority,
            DataSource metadataDataSource, SecurityBootstrap security, ObservabilityBootstrap observability,
            org.pangrova.persistence.coordination.api.provider.CoordinationProvider coordinationProvider) {}

    static void closeOwnedResources(CoordinatorLifecycleAdmission lifecycle,
            ShutdownOwners owners) {
        drainIndependentOwners(lifecycle, owners);
        lifecycle.step("http", present(owners.http(), HttpBootstrap::shutdown));
        Throwable semanticFailure = closeQueryOwners(lifecycle, owners.query());
        try {
            closeDrainedDependencies(lifecycle, owners, semanticFailure);
        } catch (RuntimeException | Error failure) {
            if (semanticFailure == null) throw failure;
            if (semanticFailure != failure) semanticFailure.addSuppressed(failure);
        }
        if (semanticFailure instanceof RuntimeException runtime) throw runtime;
        if (semanticFailure instanceof Error error) throw error;
    }

    private static void drainIndependentOwners(
            CoordinatorLifecycleAdmission lifecycle, ShutdownOwners owners) {
        if (owners.query() != null) owners.query().sealAdmission();
        if (owners.ha() != null) owners.ha().stopQueryScheduling();
        ResourceCloser.closeAll(null,
                () -> lifecycle.step("maintenance",
                        present(owners.maintenanceLeadership(), MaintenanceLeadershipBootstrap::close)),
                () -> lifecycle.step("branch",
                        present(owners.branchTtlCleanupTask(), CoordinatorShutdownSequence::closeBranchCleanup)),
                () -> lifecycle.step("cdc-runtime",
                        present(owners.cdcRuntime(), JournaledCdcRuntime::close)),
                () -> lifecycle.step("streaming",
                        present(owners.streamingBootstrap(), CoordinatorShutdownSequence::closeStreamingBootstrap)),
                () -> lifecycle.step("query-drain", present(owners.query(), QueryBootstrap::drainQueries)));
        if (owners.query() != null) owners.query().joinSchedulingOwners();
    }

    private static Throwable closeQueryOwners(
            CoordinatorLifecycleAdmission lifecycle, QueryBootstrap query) {
        try {
            lifecycle.step("query", present(query, QueryBootstrap::shutdown));
        } catch (RuntimeException | Error failure) {
            if (query == null || !query.isDrained()) throw failure;
            return failure;
        }
        return null;
    }

    private static void closeDrainedDependencies(CoordinatorLifecycleAdmission lifecycle,
            ShutdownOwners owners, Throwable primary) {
        // All query writers/readers and HTTP entries have actually exited before any shared backend retires.
        lifecycle.step("ha", present(owners.ha(), HighAvailabilityBootstrap::shutdown));
        lifecycle.step("modules", present(owners.moduleWiring(), CoordinatorShutdownSequence::closeModules));
        lifecycle.step("dataset-control", present(owners.nativeDatasetControl(), NativeDatasetControlBootstrap::close));
        lifecycle.step("metadata", present(owners.metadataBootstrap(), MetadataBootstrap::shutdown));
        lifecycle.step("connectors", present(owners.connectorHandleFactory(), PluginConnectorHandleFactory::shutdown));
        lifecycle.step("execution-authority", present(owners.executionAuthority(),
                org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority::close));
        ResourceCloser.closeAll(primary,
                () -> lifecycle.step("coordinationProvider", present(owners.coordinationProvider(),
                        org.pangrova.persistence.coordination.api.provider.CoordinationProvider::close)),
                () -> lifecycle.step("datasource",
                        present(owners.metadataDataSource(), CoordinatorShutdownSequence::closeDataSource)),
                () -> lifecycle.step("security", present(owners.security(), SecurityBootstrap::shutdown)),
                () -> lifecycle.step("observability",
                        present(owners.observability(), ObservabilityBootstrap::shutdown)));
    }

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorShutdownSequence.class);

    private CoordinatorShutdownSequence() {}

    static void stopTimeoutScheduler(QueryTimeoutScheduler enforcer) {
        if (enforcer != null) {
            enforcer.stop();
            LOG.info("Phase: QueryTimeoutScheduler — stopped");
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
        allocator.close();
        LOG.info("event=coordinator_transport_allocator_closed");
    }

    /**
     * Shuts down the scheduler executor as a fallback during shutdown.
     * Prevents thread leak if HA bootstrap fails before normal executor shutdown.
     */
    static void shutdownSchedulerExecutor(ScheduledExecutorService executor) {
        if (executor == null) {
            return;
        }
        org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(executor, DRAIN_TIMEOUT_MS);
        LOG.info("event=scheduler_executor_actually_terminated");
    }

    static void stopHealthScheduler(WorkerHealthScheduler scheduler,
                                     ScheduledExecutorService executor) {
        if (scheduler != null) {
            scheduler.stop();
        }
        if (executor != null) {
            org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(executor, DRAIN_TIMEOUT_MS);
        }
    }

    static void stopHttp(CoordinatorHttpServer httpServer) {
        if (httpServer == null) return;
        try {
            httpServer.stop();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("Coordinator HTTP owner has not stopped", failure);
        }
    }

    static void closeModules(ModuleIntegrationBootstrap wiring) {
        if (wiring != null) {
            wiring.close();
        }
    }


    static void closeStreamingBootstrap(StreamingBootstrap streaming) {
        if (streaming == null) return;
        streaming.close();
        LOG.info("event=phase_closed phase=streaming_bootstrap");
    }

    static void closeBranchCleanup(org.pangrova.coordinator.ai.branch.BranchTtlCleanupTask cleanup) {
        if (cleanup == null) return;
        cleanup.close();
        LOG.info("event=phase_closed phase=branch_ttl_cleanup");
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
            } catch (RuntimeException | Error failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IllegalStateException("Metadata DataSource has not closed", failure);
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

    /** Admits and dispatches one replacement task after its worker was declared dead. */
    @FunctionalInterface
    public interface WorkerTaskRetrier {
        /** @return true when a replacement was dispatched */
        boolean retry(String queryId, String taskId, String deadWorkerId);
    }

    /**
     * Handles a dead worker. Each task is first offered to task-level retry; only
     * queries that still have an unrecoverable task fail. Retried tasks keep the
     * Stage slot owned by their replacement, so a stale report from the dead
     * attempt cannot complete or fail the Stage.
     */
    static void onDeadWorker(String workerId,
                              CoordinatorTaskTracker taskTracker,
                              QueryManager queryManager) {
        onDeadWorker(workerId, taskTracker, queryManager, null);
    }

    static void onDeadWorker(String workerId,
                              CoordinatorTaskTracker taskTracker,
                              QueryManager queryManager,
                              WorkerTaskRetrier retrier) {
        Set<String> affectedQueries = drainDeadWorkerQueries(workerId, taskTracker);
        Set<String> unrecoveredQueries = new LinkedHashSet<>();
        int retried = retryOrFailDeadWorkerTasks(
                workerId, taskTracker, retrier, unrecoveredQueries);
        failAffectedQueries(workerId, unrecoveredQueries, queryManager);
        logDeadWorkerDrain(workerId, affectedQueries, retried, unrecoveredQueries);
    }

    private static int retryOrFailDeadWorkerTasks(String workerId,
                                                   CoordinatorTaskTracker taskTracker,
                                                   WorkerTaskRetrier retrier,
                                                   Set<String> unrecoveredQueries) {
        if (taskTracker == null) return 0;
        int retried = 0;
        for (Map.Entry<String, Set<String>> entry
                : taskTracker.getActiveTasksByQueryForWorker(workerId).entrySet()) {
            String queryId = entry.getKey();
            boolean recovered = true;
            for (String taskId : entry.getValue()) {
                boolean replacement = retrier != null
                        && retrier.retry(queryId, taskId, workerId);
                // A final Worker report can beat loss detection after the active-task snapshot.
                // A successful physical completion wins that race; only an actual failed task
                // without a replacement makes the query unrecoverable.
                boolean failed = taskTracker.markLostTaskFailed(queryId, taskId);
                if (replacement) {
                    retried++;
                } else if (failed) {
                    recovered = false;
                }
            }
            if (!recovered) {
                unrecoveredQueries.add(queryId);
            }
        }
        return retried;
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
        QueryFailure error = new QueryFailure(
                org.pangrova.common.enums.PangrovaErrorCode.WORKER_LOST.getCode(), "WORKER_LOST",
                "Worker " + workerId + " declared dead — failing query");
        for (String queryId : queryIds) {
            queryManager.onTaskFailed(queryId, error);
        }
    }

    private static void logDeadWorkerDrain(String workerId, Set<String> affectedQueries,
                                            int retriedTasks, Set<String> failedQueries) {
        if (failedQueries.isEmpty()) {
            LOG.warn("Dead worker detected: {} — {} tasks retried, no query failed",
                    workerId, retriedTasks);
            LOG.info("METRIC coordinator.worker.dead_removal_queries_failed count=0");
            LOG.info("METRIC coordinator.worker.dead_removal_tasks_retried count={}", retriedTasks);
            return;
        }
        LOG.error("Dead worker {} — failing {} affected queries (retried tasks={}): {}",
                workerId, failedQueries.size(), retriedTasks, failedQueries);
        LOG.info("METRIC coordinator.worker.dead_removal_queries_failed count={}",
                failedQueries.size());
        LOG.info("METRIC coordinator.worker.dead_removal_tasks_retried count={}", retriedTasks);
    }

    // ── Task timeout handling ──

    static void checkTaskTimeouts(CoordinatorTaskTracker taskTracker,
                                   QueryManager queryManager) {
        if (taskTracker == null || queryManager == null) return;
        Map<String, String> timedOut = taskTracker.getTimedOutTasks(java.time.Instant.now());
        if (!timedOut.isEmpty()) {
            failTimedOutTasks(timedOut, taskTracker, queryManager);
        }
        queryManager.retryPendingTaskCancellations();
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
                QueryFailure error = new QueryFailure(
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
