package org.pangrova.worker.ai.context;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TraceContext;
import org.pangrova.worker.CanonicalTaskRequestTestFixture;

/** Canonical authority for component tests; this does not execute a Native task. */
public final class AiTestTaskAuthority {
    private AiTestTaskAuthority() {}

    public static AiExecutionContext context(String tenant, String principal, String query,
            String task, String trace, long deadline, long mediaBytes) {
        return AiExecutionContext.fromTaskRequest(request(new Identity(
                tenant, principal, query, task, trace, deadline), StageResourceGrant.newBuilder()
                .setGrantId(task + ":grant").setCpuSlots(1).setNativeMemoryBytes(1048576)
                .setProviderConcurrency(2).setProviderItems(1024).setProviderAttempts(4096)
                .setProviderRequestBytes(268435456).setProviderResponseBytes(1073741824)
                .setMediaBytes(mediaBytes).build()));
    }

    public static TaskRequest request(Identity identity, StageResourceGrant grant) {
        return CanonicalTaskRequestTestFixture.builder(identity.task(), identity.query(), 0, 0,
                TaskKind.TASK_KIND_READ, plan())
                .setSecurity(SecurityContext.newBuilder().setTenantId(identity.tenant()).setPrincipal(identity.principal()))
                .setTraceContext(TraceContext.newBuilder().setTraceId(identity.trace()))
                .setDeadlineEpochMs(identity.deadline()).setResourceGrant(grant).build();
    }

    private static byte[] plan() {
        var schema = NamedStruct.newBuilder().addNames("value").setStruct(Type.Struct.newBuilder()
                .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED))));
        var read = ReadRel.newBuilder().setBaseSchema(schema).setVirtualTable(ReadRel.VirtualTable.newBuilder()
                .addValues(Expression.Literal.Struct.newBuilder().addFields(Expression.Literal.newBuilder().setI64(1))));
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .addNames("value").setInput(Rel.newBuilder().setRead(read)))).build().toByteArray();
    }

    public record Identity(String tenant, String principal, String query, String task, String trace, long deadline) {}
}
