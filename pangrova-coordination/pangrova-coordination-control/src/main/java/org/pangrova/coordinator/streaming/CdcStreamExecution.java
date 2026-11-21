package org.pangrova.coordinator.streaming;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes one planned CDC batch through dispatch, terminal reports, journaled
 * publication, and post-publication offset acknowledgement.
 */
public final class CdcStreamExecution {

    /** Persists the exact source end offsets only after the journal records publication. */
    @FunctionalInterface
    public interface OffsetCommitter {
        void commit(
                CdcSourceIdentity source,
                Map<TopicPartitionOffset, Long> endOffsets,
                String commitFingerprint);
    }

    private final CdcTaskDispatchPort dispatcher;
    private final CdcTaskCompletionRegistry completions;
    private final WriteCommitJournal journal;
    private final CdcWriteCommitCoordinator.Publisher publisher;
    private final OffsetCommitter offsetCommitter;
    private final Duration taskTimeout;

    /** Creates one bounded execution with explicit publish and offset-ack ports. */
    public CdcStreamExecution(
            CdcTaskDispatchPort dispatcher,
            CdcTaskCompletionRegistry completions,
            WriteCommitJournal journal,
            CdcWriteCommitCoordinator.Publisher publisher,
            OffsetCommitter offsetCommitter,
            Duration taskTimeout) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.completions = Objects.requireNonNull(completions, "completions");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.offsetCommitter = Objects.requireNonNull(offsetCommitter, "offsetCommitter");
        if (taskTimeout == null || taskTimeout.isNegative() || taskTimeout.isZero()) {
            throw new IllegalArgumentException("CDC task timeout must be positive");
        }
        this.taskTimeout = taskTimeout;
    }

    /** Executes every planned task and commits exactly one durable CDC intent. */
    public WriteCommitEntry execute(
            CdcTaskPlanner.CdcTaskPlan plan,
            CdcWriteAuthority.CdcWritePlan writePlan,
            StreamState stream,
            ConnectorSession session,
            CdcSourceIdentity source,
            long baseSnapshotId,
            long timestampMs,
            String ownerId,
            String ownerIncarnation,
            long executionFencingToken) {
        if (plan.tasks().isEmpty()) {
            throw new IllegalArgumentException("CDC execution has no planned tasks");
        }
        List<byte[]> fragments = new ArrayList<>();
        for (TaskRequest task : plan.tasks()) {
            dispatcher.dispatch(task);
            TaskStatusReport report = await(task.getAttempt().getTaskId());
            if (!"FINISHED".equals(report.state())) {
                throw new IllegalStateException(
                        "CDC task did not finish: task=" + task.getAttempt().getTaskId()
                                + " state=" + report.state());
            }
            if (report.writeResult() != null) {
                fragments.addAll(report.writeResult().commitFragments());
            }
            completions.remove(task.getAttempt().getTaskId());
        }
        WriteCommitEntry intent = CdcWriteIntentFactory.create(
                plan.tasks().get(0).getAttempt().getQueryOrJobId(), stream, session,
                writePlan, source, fragments, baseSnapshotId, timestampMs,
                ownerId, ownerIncarnation, executionFencingToken);
        new CdcWriteCommitCoordinator(journal, publisher,
                (actualSource, fingerprint) -> offsetCommitter.commit(
                        actualSource, plan.endOffsets(), fingerprint))
                .commit(intent);
        return intent;
    }

    private TaskStatusReport await(String taskId) {
        try {
            return completions.await(taskId).get(taskTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting CDC task " + taskId, interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("CDC task did not report terminal state: " + taskId, failure);
        }
    }
}
