package org.pangrova.shuffle.nativewire;

import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.identity.ExchangeAttemptFence;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

/** Preserves execution fencing and Worker incarnation across the Native shuffle wire boundary. */
public final class NativeExchangeAttempts {
    private NativeExchangeAttempts() {}

    /** Encodes the exact task fence, owner and admission incarnation for Native commands. */
    public static TaskAttemptId encode(ExchangeTaskAttempt attempt) {
        var owner = attempt.executionOwner();
        return TaskAttemptId.newBuilder()
                .setQueryOrJobId(attempt.queryId())
                .setStageId(attempt.stageId())
                .setPartitionId(attempt.taskPartitionId())
                .setTaskId(attempt.taskId())
                .setControlEpoch(attempt.controlEpoch())
                .setPlanEpoch(attempt.fence().planEpoch())
                .setAttemptId(attempt.fence().attemptId())
                .setFencingToken(attempt.fence().fencingToken())
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(owner.ownerId())
                        .setOwnerIncarnation(owner.ownerIncarnation())
                        .setFencingToken(owner.fencingToken()))
                .setWorkerExecutionIdentity(org.pangrova.protocol.v2.WorkerExecutionIdentity.newBuilder()
                        .setAdmissionDomainId(attempt.workerAdmissionDomainId())
                        .setIncarnationId(attempt.workerIncarnationId()))
                .build();
    }

    /** Converts a canonical fenced attempt to its runtime-neutral exchange identity. */
    public static ExchangeTaskAttempt decode(TaskAttemptId attempt) {
        if (!attempt.hasExecutionOwner()) {
            throw new IllegalArgumentException(
                    "external shuffle consumer requires execution owner");
        }
        var owner = attempt.getExecutionOwner();
        return new ExchangeTaskAttempt(
                attempt.getQueryOrJobId(), attempt.getStageId(),
                attempt.getPartitionId(), attempt.getTaskId(),
                attempt.getControlEpoch(),
                new ExchangeAttemptFence(
                        attempt.getPlanEpoch(), attempt.getAttemptId(),
                        attempt.getFencingToken()),
                new ShuffleExecutionOwner(
                        owner.getOwnerId(), owner.getOwnerIncarnation(),
                        owner.getFencingToken()),
                attempt.getWorkerExecutionIdentity().getAdmissionDomainId(),
                attempt.getWorkerExecutionIdentity().getIncarnationId());
    }
}
