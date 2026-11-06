package org.pangrova.protocol.execution;

import org.pangrova.query.api.identity.QueryId;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Describes a task to be submitted to the execution engine.
 *
 * <p>Contains all information needed to execute a fragment of a query plan,
 * including the query it belongs to, the plan fragment to execute, data
 * splits to process, task identity, and resource limits.</p>
 *
 * @param queryId           the identifier of the parent query, must not be null
 * @param planFragment      the plan fragment to execute, must not be null
 * @param splits            the data splits assigned to this task, must not be null
 * @param sessionProperties session properties for this task, must not be null
 * @param stageId           the stage identifier within the query plan
 * @param partitionId       the partition identifier within the stage
 * @param taskType          the task type (e.g., "COMPUTE", "OUTPUT"), must not be null
 * @param resourceLimit     resource limits for this task, must not be null
 * @param outputRouting     optional output routing for autonomous stage execution
 * @param attemptGeneration the retry generation counter; Workers reject tasks whose
 *                          generation does not match the current stage generation,
 *                          preventing stale tasks from wasting resources
 */
public record TaskDescriptor(
        QueryId queryId,
        PlanFragment planFragment,
        List<Split> splits,
        Map<String, String> sessionProperties,
        int stageId,
        int partitionId,
        String taskType,
        TaskResourceLimit resourceLimit,
        Optional<OutputRouting> outputRouting,
        int attemptGeneration) {

    public TaskDescriptor {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(planFragment, "planFragment must not be null");
        splits = List.copyOf(checkNotNull(splits, "splits must not be null"));
        sessionProperties = Map.copyOf(checkNotNull(sessionProperties, "sessionProperties must not be null"));
        checkNotNull(taskType, "taskType must not be null");
        checkNotNull(resourceLimit, "resourceLimit must not be null");
        if (outputRouting == null) {
            outputRouting = Optional.empty();
        }
    }

    /**
     * Constructor without attemptGeneration (defaults to 0).
     */
    public TaskDescriptor(
            QueryId queryId,
            PlanFragment planFragment,
            List<Split> splits,
            Map<String, String> sessionProperties,
            int stageId,
            int partitionId,
            String taskType,
            TaskResourceLimit resourceLimit,
            Optional<OutputRouting> outputRouting) {
        this(queryId, planFragment, splits, sessionProperties,
                stageId, partitionId, taskType, resourceLimit, outputRouting, 0);
    }

    /**
     * Backward-compatible constructor without outputRouting.
     * Defaults: stageId, partitionId, taskType, resourceLimit as specified, no output routing.
     */
    public TaskDescriptor(
            QueryId queryId,
            PlanFragment planFragment,
            List<Split> splits,
            Map<String, String> sessionProperties,
            int stageId,
            int partitionId,
            String taskType,
            TaskResourceLimit resourceLimit) {
        this(queryId, planFragment, splits, sessionProperties,
                stageId, partitionId, taskType, resourceLimit, Optional.empty(), 0);
    }

    /**
     * Backward-compatible constructor for callers that don't need full task identity.
     * Defaults: stageId=0, partitionId=0, taskType="COMPUTE", resourceLimit=NONE, no output routing.
     */
    public TaskDescriptor(
            QueryId queryId,
            PlanFragment planFragment,
            List<Split> splits,
            Map<String, String> sessionProperties) {
        this(queryId, planFragment, splits, sessionProperties,
                0, 0, "COMPUTE", TaskResourceLimit.NONE, Optional.empty(), 0);
    }

    /**
     * Returns true if this task has native output routing configured,
     * meaning Java data threads are not needed.
     */
    public boolean hasNativeOutputRouting() {
        return outputRouting.isPresent();
    }
}
