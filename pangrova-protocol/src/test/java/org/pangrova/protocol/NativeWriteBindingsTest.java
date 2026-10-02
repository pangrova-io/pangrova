package org.pangrova.protocol;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeWriteBindingsTest {
    @Test
    void exactAttemptAndOwnerProduceDistinctStagingPaths() {
        TaskAttemptId attempt = TaskAttemptId.newBuilder().setQueryOrJobId("query-1")
                .setTaskId("task-1").setPlanEpoch(1).setFencingToken(2).setControlEpoch(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner-1")
                        .setOwnerIncarnation("incarnation-1").setFencingToken(4)).build();
        String prefix = NativeWriteBindings.attemptPrefix("staging/write-1", attempt);
        assertThat(prefix).startsWith("staging/write-1/attempt-").hasSize(88);
        assertThat(NativeWriteBindings.attemptPrefix("staging/write-1", attempt)).isEqualTo(prefix);
        assertThat(NativeWriteBindings.attemptPrefix("staging/write-1",
                attempt.toBuilder().setAttemptId(1).build())).isNotEqualTo(prefix);
        assertThat(NativeWriteBindings.attemptPrefix("staging/write-1",
                attempt.toBuilder().setControlEpoch(4).build())).isNotEqualTo(prefix);
        assertThatThrownBy(() -> NativeWriteBindings.attemptPrefix("staging/write-1",
                attempt.toBuilder().clearExecutionOwner().build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresEveryFencedIdentityBeforeSerializingTheStagingPath() {
        var attempt = TaskAttemptId.newBuilder().setTaskId("task-1").setPlanEpoch(1)
                .setFencingToken(2).setControlEpoch(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner-1"))
                .build();
        for (var invalid : List.of(attempt.toBuilder().clearExecutionOwner().build(),
                attempt.toBuilder().clearPlanEpoch().build(), attempt.toBuilder().clearFencingToken().build(),
                attempt.toBuilder().clearControlEpoch().build(), attempt.toBuilder().clearTaskId().build())) {
            assertThatThrownBy(() -> NativeWriteBindings.attemptPrefix("staging", invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fenced attempt");
        }
        for (String prefix : new String[] {null, "", " "}) {
            assertThatThrownBy(() -> NativeWriteBindings.attemptPrefix(prefix, attempt))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("allocation prefix");
        }
    }
}
