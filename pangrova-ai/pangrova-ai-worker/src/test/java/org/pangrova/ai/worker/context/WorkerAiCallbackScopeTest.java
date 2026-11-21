package org.pangrova.ai.worker.context;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerAiCallbackScopeTest {

    private final WorkerAiCallbackScope scope = new WorkerAiCallbackScope();

    @Test
    void bindsCanonicalAuthorityForTheCallbackAndRestoresTheEnclosingContext() {
        byte[] authority = callbackAuthority("tenant-a", "alice", "query-a", "task-a");
        var observed = new AtomicReference<AiExecutionContext>();

        String result = scope.call(authority, () -> {
            observed.set(AiExecutionContext.current().orElseThrow());
            return observed.get().tenantId();
        });

        assertThat(result).isEqualTo("tenant-a");
        assertThat(observed.get().principal()).isEqualTo("alice");
        assertThat(AiExecutionContext.current()).isEmpty();
    }

    @Test
    void restoresTheEnclosingContextWhenTheCallbackFails() {
        byte[] authority = callbackAuthority("tenant-a", "alice", "query-a", "task-a");
        AiExecutionContext enclosing = AiExecutionContext.fromCanonicalCallback(
                callbackAuthority("tenant-b", "bob", "query-b", "task-b"));

        assertThatThrownBy(() -> AiExecutionContext.callWith(enclosing,
                () -> scope.call(authority, () -> {
                    assertThat(AiExecutionContext.current().orElseThrow().tenantId())
                            .isEqualTo("tenant-a");
                    throw new IllegalStateException("callback failed");
                })))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("callback failed");
        assertThat(AiExecutionContext.current()).isEmpty();
    }

    private static byte[] callbackAuthority(
            String tenant, String principal, String queryId, String taskId) {
        var request = CanonicalTaskRequestTestFixture.builder(
                        taskId, queryId, 0, 0, TaskKind.TASK_KIND_READ, new byte[]{1})
                .setSecurity(org.pangrova.protocol.v2.SecurityContext.newBuilder()
                        .setTenantId(tenant).setPrincipal(principal))
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setCpuSlots(1)
                        .setNativeMemoryBytes(1048576))
                .build();
        return CanonicalTaskRequests.callbackFor(request).toByteArray();
    }
}
