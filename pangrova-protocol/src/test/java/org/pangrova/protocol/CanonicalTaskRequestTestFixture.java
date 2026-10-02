package org.pangrova.protocol;

import com.google.protobuf.ByteString;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

/** Test fixture that constructs the real canonical v2 task contract. */
public final class CanonicalTaskRequestTestFixture {

    private CanonicalTaskRequestTestFixture() {}

    public static TaskRequest.Builder builder(
            String taskId, String queryId, TaskKind taskKind, byte[] plan) {
        return TaskRequest.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId(queryId)
                        .setTaskId(taskId)
                        .setPlanEpoch(1)
                        .setFencingToken(1L << 32 | 1L)
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
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setCpuSlots(2).setNativeMemoryBytes(1_048_576))
                .setDeadlineEpochMs(4_102_444_800_000L);
    }
}
