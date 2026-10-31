package org.pangrova.shuffle.security;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Exact operation, resource, and execution owner authorized by a capability. */
public record CapabilityScope(
        CapabilityOperation operation,
        String subject,
        String ownerId,
        String ownerIncarnation,
        long ownerFencingToken,
        TaskAttemptId taskAttempt) {

    public CapabilityScope {
        if (!validResource(operation, subject)
                || !validOwner(ownerId, ownerIncarnation, ownerFencingToken)) {
            throw new IllegalArgumentException("capability scope is incomplete");
        }
        if (taskAttempt != null && (!taskAttempt.hasExecutionOwner()
                || !taskAttempt.getExecutionOwner().getOwnerId().equals(ownerId)
                || !taskAttempt.getExecutionOwner().getOwnerIncarnation()
                        .equals(ownerIncarnation)
                || taskAttempt.getExecutionOwner().getFencingToken()
                        != ownerFencingToken
                || taskAttempt.getQueryOrJobId().isBlank()
                || taskAttempt.getStageId() < 0
                || taskAttempt.getPartitionId() < 0
                || taskAttempt.getPlanEpoch() <= 0
                || taskAttempt.getFencingToken() <= 0
                || taskAttempt.getTaskId().isBlank()
                || taskAttempt.getControlEpoch() <= 0)) {
            throw new IllegalArgumentException("task-bound capability attempt is incomplete");
        }
        validateWorkerIdentity(taskAttempt);
    }

    private static void validateWorkerIdentity(TaskAttemptId taskAttempt) {
        if (taskAttempt != null) {
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity.from(
                    taskAttempt.getWorkerExecutionIdentity());
        }
    }

    private static boolean validResource(CapabilityOperation operation, String subject) {
        return operation != null && subject != null && !subject.isBlank();
    }

    private static boolean validOwner(String ownerId, String incarnation, long fence) {
        return ownerId != null && !ownerId.isBlank()
                && incarnation != null && !incarnation.isBlank() && fence > 0;
    }

    public static CapabilityScope forShuffle(
            CapabilityOperation operation,
            String shuffleId,
            ExecutionOwnerIdentity owner) {
        return new CapabilityScope(
                operation, shuffleId, owner.getOwnerId(),
                owner.getOwnerIncarnation(), owner.getFencingToken(), null);
    }

    /** Data-plane authorization for one exact fenced task attempt. */
    public static CapabilityScope forTask(
            CapabilityOperation operation,
            String shuffleId,
            ExecutionOwnerIdentity owner,
            TaskAttemptId taskAttempt) {
        if (operation != CapabilityOperation.READ
                && operation != CapabilityOperation.WRITE) {
            throw new IllegalArgumentException("task scope requires READ or WRITE");
        }
        if (taskAttempt == null) {
            throw new IllegalArgumentException("task scope requires an exact attempt");
        }
        return new CapabilityScope(operation, shuffleId,
                owner.getOwnerId(), owner.getOwnerIncarnation(),
                owner.getFencingToken(), taskAttempt);
    }

    public static CapabilityScope forQuery(
            String queryId,
            ExecutionOwnerIdentity owner) {
        return new CapabilityScope(
                CapabilityOperation.CLEANUP, queryId, owner.getOwnerId(),
                owner.getOwnerIncarnation(), owner.getFencingToken(), null);
    }
}
