package org.pangrova.coordinator;

import com.google.protobuf.ByteString;

import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

/** Test support for constructing the real canonical v2 task protocol. */
public final class CanonicalTaskRequestTestFixture {

    private CanonicalTaskRequestTestFixture() {}

    public static TaskRequest.Builder builder(
            String taskId, String queryId, int stageId, int partitionId,
            TaskKind taskKind, byte[] plan) {
        return TaskRequest.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId(queryId)
                        .setTaskId(taskId)
                        .setStageId(stageId)
                        .setPartitionId(partitionId)
                        .setPlanEpoch(1)
                        .setFencingToken((1L << 32) | 1L)
                        .setControlEpoch(1)
                        .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId("coordinator")
                                .setOwnerIncarnation("test")
                                .setFencingToken(1)))
                .setTaskKind(taskKind)
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId("tenant")
                        .setPrincipal("test-user"))
                .setSubstraitPlan(ByteString.copyFrom(plan))
                .setSubstraitPlanChecksum(CanonicalTaskRequests.sha256(plan))
                .setDeadlineEpochMs(4_102_444_800_000L);
    }

    public static SubmitTaskRequest request(
            String taskId, String queryId, int stageId, int partitionId,
            TaskKind taskKind, byte[] plan) {
        return SubmitTaskRequest.from(builder(
                taskId, queryId, stageId, partitionId, taskKind, plan).build());
    }
}
