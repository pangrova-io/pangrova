package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.worker.context.AiExecutionContext;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiExecutionContextTest {

    @Test
    void callWithRestoresEnclosingContextAfterFailure() {
        var outer = context("outer", 4_102_444_800_000L);
        var inner = context("inner", 4_102_444_800_001L);

        assertThatThrownBy(() -> AiExecutionContext.callWith(outer, () ->
                AiExecutionContext.callWith(inner, () -> {
                    assertThat(AiExecutionContext.current()).contains(inner);
                    throw new IllegalStateException("boom");
                })))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
        assertThat(AiExecutionContext.current()).isEmpty();
    }

    @Test
    void deadlineAndGovernedIdentityAreValidated() {
        var context = context("tenant-a", 4_102_444_800_000L);

        context.requireGovernedIdentity();

        assertThat(context.deadline()).contains(Instant.ofEpochMilli(4_102_444_800_000L));
    }

    @Test
    void missingIdentityFailsClosed() {
        var context = new AiExecutionContext(
                "", "", "q", "t", "trace", 1, 1, 1, 1, 1, 1, 0,
                org.pangrova.protocol.v2.TaskAttemptId.newBuilder().setQueryOrJobId("q").setTaskId("t").build());

        assertThatThrownBy(context::requireGovernedIdentity)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tenantId");
    }

    private static AiExecutionContext context(String tenantId, long deadline) {
        return org.pangrova.ai.worker.context.AiTestTaskAuthority.context(
                tenantId, "alice", "query-a", "task-a", "trace-a", deadline,
                1_000_000);
    }
}
