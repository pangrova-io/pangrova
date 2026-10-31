package org.pangrova.testing.integration.e2e.jni;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import io.substrait.proto.Plan;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import java.time.Instant;
import java.util.UUID;

/** Real canonical task payload for packaged JNI handle lifecycle checks. */
final class CanonicalNativeTask {

    private CanonicalNativeTask() {}

    static byte[] valuesRequest() {
        Plan.Builder plan = Plan.newBuilder();
        var root = plan.addRelationsBuilder().getRootBuilder();
        root.addNames("value");
        var read = root.getInputBuilder().getReadBuilder();
        read.getBaseSchemaBuilder().addNames("value").getStructBuilder()
                .addTypesBuilder().getI64Builder()
                .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        read.getVirtualTableBuilder()
                .addExpressionsBuilder().addFieldsBuilder().getLiteralBuilder().setI64(42);
        byte[] planBytes = plan.build().toByteArray();
        String id = UUID.randomUUID().toString();
        return TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId("jni-lifecycle-" + id).setTaskId(id)
                        .setPlanEpoch(1).setFencingToken((1L << 32) | 1L)
                        .setControlEpoch(1)
                        .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId("e2e").setOwnerIncarnation("jni-lifecycle")
                                .setFencingToken(1)))
                .setTaskKind(TaskKind.TASK_KIND_READ)
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId("e2e").setPrincipal("jni-lifecycle"))
                .setSubstraitPlan(ByteString.copyFrom(planBytes))
                .setSubstraitPlanChecksum(ProtocolChecksums.sha256(planBytes))
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId(id).setCpuSlots(1).setNativeMemoryBytes(64L * 1024 * 1024))
                .setDeadlineEpochMs(Instant.now().plusSeconds(60).toEpochMilli())
                .build().toByteArray();
    }
}
