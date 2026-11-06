package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.query.core.task.StageDispatchRecord;

import java.util.List;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Pushes upstream commit notifications to downstream workers when a
 * stage finishes. Only sends notifications for BLOCKING edges —
 * STREAMING sources deliver data immediately and need no commit signal.
 */
public final class CommitNotificationHandler implements StageLifecycleHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CommitNotificationHandler.class);

    private final TaskDispatcher dispatcher;
    private final List<StageDispatchRecord> dispatched;

    public CommitNotificationHandler(TaskDispatcher dispatcher,
                                     List<StageDispatchRecord> dispatched) {
        this.dispatcher = checkNotNull(dispatcher, "dispatcher must not be null");
        this.dispatched = checkNotNull(dispatched, "dispatched must not be null");
    }

    /**
     * No-op: commit notifications are only relevant after stage completion.
     */
    @Override
    public void onScheduling(StageLifecycleEvent event) {
        // intentional no-op — commit notifications apply only on FINISHED
    }

    @Override
    public void onFinished(StageLifecycleEvent event) {
        Stage finished = event.stage();
        StageDAG dag = event.dag();
        String queryId = event.queryId();

        for (Stage downstream : dag.stageEntities()) {
            ExchangeReadMode mode = dag.getEdgeReadMode(
                    finished.stageId(), downstream.stageId());
            if (mode != ExchangeReadMode.BLOCKING) {
                continue;
            }
            notifyDownstreamTasks(queryId, finished, downstream);
        }
    }

    private void notifyDownstreamTasks(String queryId, Stage finished, Stage downstream) {
        for (String taskId : downstream.taskIds()) {
            StageDispatchRecord rec = findRecord(taskId);
            if (rec == null) {
                continue;
            }
            LOG.debug("event=commit_notification_push queryId={} upstreamStageId={} "
                            + "downstreamStageId={} workerHost={}",
                    queryId, finished.stageId(), downstream.stageId(),
                    rec.result().workerHost());
            dispatcher.notifyUpstreamCommit(
                    rec.result().workerHost(), rec.workerHttpPort(),
                    queryId, finished.stageId(), () ->
                            LOG.error("event=commit_notify_exhausted queryId={} "
                                            + "upstreamStageId={} downstreamStageId={}",
                                    queryId, finished.stageId(), downstream.stageId()));
        }
    }

    private StageDispatchRecord findRecord(String taskId) {
        for (StageDispatchRecord rec : dispatched) {
            if (rec.result().taskId().equals(taskId)) {
                return rec;
            }
        }
        return null;
    }
}
