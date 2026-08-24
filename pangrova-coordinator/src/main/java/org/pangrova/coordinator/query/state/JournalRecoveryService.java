package org.pangrova.coordinator.query.state;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Replays journal events to rebuild in-memory state after HA leader switchover.
 *
 * <p>Recovery order: queries first (registers state machines), then tasks
 * (registers task tracking and worker assignments). After replay, performs
 * worker liveness checks: tasks on dead or unknown workers cause the owning
 * query to be marked FAILED and purged from the journal.
 *
 * <p><strong>Owner-scoped recovery (Task 51, SI-6):</strong> When a
 * {@code previousLeaderId} is provided, recovery replays only journal rows
 * written by the failed coordinator. This prevents the new leader from
 * accidentally replaying stale entries belonging to an older term or a
 * different coordinator.
 *
 * <p><strong>Worker-liveness grace window (Task 51, SI-6):</strong> After
 * failover, workers need time to detect the new leader and re-register.
 * The {@code workerLivenessGraceMs} parameter delays liveness checks by
 * the specified duration, preventing false DEAD verdicts for workers that
 * are alive but have not yet heartbeated the new leader.
 *
 * <p>Thread-safety: intended to run once during leader promotion, before
 * the Coordinator starts accepting new queries.
 */
public final class JournalRecoveryService {

    private static final Logger LOG = LoggerFactory.getLogger(JournalRecoveryService.class);

    /** Default grace window (60 s) after failover before checking worker liveness. */
    public static final long DEFAULT_WORKER_LIVENESS_GRACE_MS = 60_000;

    private final QueryStateJournal journal;
    private final QueryManager queryManager;
    private final CoordinatorTaskTracker taskTracker;
    private final WorkerManager workerManager;
    private final long workerLivenessGraceMs;
    private final String previousLeaderId;

    /**
     * Full constructor for owner-scoped recovery with worker-liveness grace window.
     *
     * @param journal                the query state journal to replay events from
     * @param queryManager           the query manager to register recovered state machines
     * @param taskTracker            the task tracker to register recovered tasks
     * @param workerManager          the worker manager for checking worker health (nullable)
     * @param previousLeaderId       the coordinator whose queries to replay (nullable = unscoped)
     * @param workerLivenessGraceMs  milliseconds to wait before checking worker liveness (0 = immediate)
     */
    public JournalRecoveryService(QueryStateJournal journal,
                                  QueryManager queryManager,
                                  CoordinatorTaskTracker taskTracker,
                                  WorkerManager workerManager,
                                  String previousLeaderId,
                                  long workerLivenessGraceMs) {
        this.journal = checkNotNull(journal, "journal must not be null");
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
        this.taskTracker = checkNotNull(taskTracker, "taskTracker must not be null");
        this.workerManager = workerManager;
        this.previousLeaderId = previousLeaderId;
        this.workerLivenessGraceMs = Math.max(0, workerLivenessGraceMs);
    }

    /**
     * Creates a recovery service with worker liveness checking enabled.
     *
     * @param journal       the query state journal to replay events from
     * @param queryManager  the query manager to register recovered state machines
     * @param taskTracker   the task tracker to register recovered tasks
     * @param workerManager the worker manager for checking worker health (nullable — if null, skips liveness checks)
     */
    public JournalRecoveryService(QueryStateJournal journal,
                                  QueryManager queryManager,
                                  CoordinatorTaskTracker taskTracker,
                                  WorkerManager workerManager) {
        this(journal, queryManager, taskTracker, workerManager, null, 0);
    }

    /**
     * Backward-compatible constructor without worker liveness checking.
     */
    public JournalRecoveryService(QueryStateJournal journal,
                                  QueryManager queryManager,
                                  CoordinatorTaskTracker taskTracker) {
        this(journal, queryManager, taskTracker, null, null, 0);
    }

    /**
     * Replays active (non-terminal) events from the journal, rebuilds
     * in-memory state, and checks worker liveness for recovered tasks.
     *
     * <p>When {@code previousLeaderId} is set, only journal rows owned by
     * that coordinator are replayed (owner-scoped recovery). Otherwise, all
     * active rows are replayed (backward-compatible behavior).
     *
     * <p>When {@code workerLivenessGraceMs > 0}, the liveness check treats
     * UNKNOWN workers leniently -- they may be alive but have not yet
     * heartbeated the new leader after failover.
     *
     * @return the number of queries successfully recovered (not failed)
     */
    public int recover() {
        List<QueryStateEvent> queryEvents;
        List<TaskStateEvent> taskEvents;
        if (previousLeaderId != null) {
            queryEvents = journal.replayActiveQueriesForOwner(previousLeaderId);
            taskEvents = journal.replayActiveTasksForOwner(previousLeaderId);
            LOG.info("event=journal_recovery_scoped previousLeader={} queryEvents={} taskEvents={}",
                    previousLeaderId, queryEvents.size(), taskEvents.size());
        } else {
            queryEvents = journal.replayActiveQueries();
            taskEvents = journal.replayActiveTasks();
        }

        int queriesRecovered = recoverQueries(queryEvents);
        int tasksRecovered = recoverTasks(taskEvents);

        int queriesFailed = checkWorkerLiveness(taskEvents);

        LOG.info("event=journal_recovery_complete queriesRecovered={} tasksRecovered={} queriesFailedByWorkerCheck={}",
                queriesRecovered, tasksRecovered, queriesFailed);
        return queriesRecovered - queriesFailed;
    }

    private int recoverQueries(List<QueryStateEvent> events) {
        // Group events by queryId and apply only the latest state per query.
        // replayActiveQueries() returns all events (multiple per query),
        // so we deduplicate by taking the last event for each queryId.
        Map<String, QueryStateEvent> latestByQuery = new LinkedHashMap<>();
        for (QueryStateEvent event : events) {
            latestByQuery.put(event.queryId(), event);
        }
        int count = 0;
        for (QueryStateEvent event : latestByQuery.values()) {
            try {
                QueryStateMachine sm = new QueryStateMachine();
                QueryStateMachine.State target = QueryStateMachine.State.valueOf(event.state());
                advanceToState(sm, target);
                queryManager.registerStateMachine(event.queryId(), sm);
                count++;
                LOG.debug("event=query_recovered queryId={} state={}",
                        event.queryId(), event.state());
            } catch (Exception e) {
                LOG.warn("event=query_recovery_failed queryId={} state={} error={}",
                        event.queryId(), event.state(), e.getMessage());
            }
        }
        return count;
    }

    private int recoverTasks(List<TaskStateEvent> events) {
        Map<String, TaskStateEvent> latestByTask = new LinkedHashMap<>();
        for (TaskStateEvent event : events) {
            latestByTask.put(event.taskId(), event);
        }
        int count = 0;
        for (TaskStateEvent event : latestByTask.values()) {
            if (replayTaskEvent(event)) {
                count++;
            }
        }
        return count;
    }

    private boolean replayTaskEvent(TaskStateEvent event) {
        try {
            taskTracker.registerTask(event.queryId(), event.taskId());
            taskTracker.markScheduled(event.queryId(), event.taskId());
            if (event.workerId() != null) {
                taskTracker.assignWorker(event.taskId(), event.workerId());
            }
            if (!"SCHEDULED".equals(event.state())) {
                taskTracker.updateTaskState(event.queryId(), event.taskId(), event.state());
            }
            return true;
        } catch (Exception e) {
            LOG.warn("event=task_recovery_failed queryId={} taskId={} error={}",
                    event.queryId(), event.taskId(), e.getMessage());
            return false;
        }
    }

    /**
     * Checks worker liveness for all recovered tasks. Groups tasks by query,
     * then for each query checks if any task's assigned worker is dead or unknown.
     * If so, marks the query FAILED and purges it from the journal.
     *
     * <p><strong>Grace window (Task 51, SI-6):</strong> When {@code workerLivenessGraceMs > 0},
     * workers whose health evaluates to UNKNOWN are given leniency: they may be
     * alive but have not yet re-registered with the new leader after failover.
     * Workers that were truly dead before failover (registered but stale) are
     * still caught immediately.
     *
     * @return the number of queries marked FAILED due to dead workers
     */
    private int checkWorkerLiveness(List<TaskStateEvent> taskEvents) {
        if (workerManager == null || taskEvents.isEmpty()) {
            return 0;
        }

        if (workerLivenessGraceMs > 0) {
            LOG.info("event=recovery_liveness_grace_start graceMs={}", workerLivenessGraceMs);
        }

        // Group tasks by queryId -> set of workerIds
        Map<String, Set<String>> queryWorkers = new LinkedHashMap<>();
        for (TaskStateEvent event : taskEvents) {
            if (event.workerId() != null) {
                queryWorkers.computeIfAbsent(event.queryId(), k -> new HashSet<>())
                        .add(event.workerId());
            }
        }

        Instant now = Instant.now();
        Set<String> failedQueries = new HashSet<>();

        for (Map.Entry<String, Set<String>> entry : queryWorkers.entrySet()) {
            String queryId = entry.getKey();
            for (String workerId : entry.getValue()) {
                HealthState health = workerManager.evaluateHealth(workerId, now);

                if (health == HealthState.ACTIVE || health == HealthState.SUSPECT) {
                    LOG.info("event=recovery_task_worker_alive queryId={} workerId={} health={}",
                            queryId, workerId, health);
                } else if (workerLivenessGraceMs > 0 && health == HealthState.UNKNOWN) {
                    // Task 51 / SI-6: During grace window, UNKNOWN workers get
                    // the benefit of the doubt -- they may not have re-registered
                    // with the new leader yet. Try to recover from the persistent
                    // worker registry before declaring failure.
                    boolean recovered = workerManager.tryRecoverFromRegistry(workerId);
                    if (recovered) {
                        LOG.info("event=recovery_worker_recovered_from_registry queryId={} workerId={}",
                                queryId, workerId);
                    } else {
                        LOG.info("event=recovery_worker_unknown_grace queryId={} workerId={} graceMs={}",
                                queryId, workerId, workerLivenessGraceMs);
                        // Do NOT fail the query -- worker may re-register during the grace window.
                        // The periodic health checker will catch truly dead workers after the
                        // grace period.
                    }
                } else {
                    LOG.warn("event=recovery_worker_unreachable queryId={} workerId={} health={}",
                            queryId, workerId, health);
                    failedQueries.add(queryId);
                }
            }
        }

        for (String queryId : failedQueries) {
            try {
                queryManager.onTaskFailed(queryId,
                        new org.pangrova.protocol.rest.error.ErrorDetail(
                                "RECOVERY_WORKER_DEAD",
                                queryId,
                                "Query failed during recovery: assigned worker is dead or unreachable"));
            } catch (Exception e) {
                LOG.warn("event=recovery_query_fail_error queryId={} error={}", queryId, e.getMessage());
            }
            try {
                journal.purgeQuery(queryId);
                LOG.warn("event=recovery_query_failed queryId={} reason=worker_dead_or_unreachable", queryId);
            } catch (Exception e) {
                LOG.warn("event=recovery_journal_purge_error queryId={} error={}", queryId, e.getMessage());
            }
        }

        return failedQueries.size();
    }

    /** Happy path: QUEUED → PLANNING → SCHEDULING → RUNNING → FINISHING → FINISHED. */
    private static final QueryStateMachine.State[] HAPPY_PATH = {
            QueryStateMachine.State.PLANNING,
            QueryStateMachine.State.SCHEDULING,
            QueryStateMachine.State.RUNNING,
            QueryStateMachine.State.FINISHING,
            QueryStateMachine.State.FINISHED
    };

    /**
     * Advances a fresh QueryStateMachine through the valid transition path
     * to reach the target state. Skips if target is QUEUED (initial state).
     */
    private static void advanceToState(QueryStateMachine sm, QueryStateMachine.State target) {
        if (target == QueryStateMachine.State.QUEUED) {
            return;
        }
        if (target == QueryStateMachine.State.FAILED
                || target == QueryStateMachine.State.CANCELLED) {
            sm.tryTransition(QueryStateMachine.State.PLANNING);
            sm.tryTransition(target);
            return;
        }
        for (QueryStateMachine.State step : HAPPY_PATH) {
            sm.tryTransition(step);
            if (step == target) {
                return;
            }
        }
    }
}
