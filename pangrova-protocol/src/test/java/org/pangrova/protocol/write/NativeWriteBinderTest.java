package org.pangrova.protocol.write;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.PhysicalFileAllocation;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WriteTaskOptions;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeWriteBinderTest {
    @Test
    void repeatedTransportBindingAcceptsOnlyTheExactSealedPhysicalAttempt() {
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId("query-1")
                .setTaskId("task-1").setPlanEpoch(1).setFencingToken(2).setControlEpoch(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner-1")
                        .setOwnerIncarnation("incarnation-1").setFencingToken(4)).build();
        var physical = PhysicalFileWriteBinding.newBuilder().setAllocationPrefix("objects/write")
                .addFiles(PhysicalFileWriteBindingFactory.sealed(PhysicalFileAllocation.newBuilder()
                        .setFileName("data.lance").setOpaqueLocation("objects/write/data.lance")));
        var request = TaskRequest.newBuilder().setAttempt(attempt)
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setWrite(WriteTaskOptions.newBuilder()
                        .setOutputBinding(WriteOutputBinding.newBuilder().setPhysical(physical)))).build();

        var bound = NativeWriteBinder.bindAttempt(request);
        assertThat(NativeWriteBinder.bindAttempt(bound)).isEqualTo(bound);
        assertThatThrownBy(() -> NativeWriteBinder.bindAttempt(bound.toBuilder()
                .setAttempt(attempt.toBuilder().setAttemptId(1)).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different task attempt");
        var tampered = bound.toBuilder().setExecutionOptions(bound.getExecutionOptions().toBuilder()
                .setWrite(bound.getExecutionOptions().getWrite().toBuilder().setOutputBinding(
                        bound.getExecutionOptions().getWrite().getOutputBinding().toBuilder().setPhysical(
                                bound.getExecutionOptions().getWrite().getOutputBinding().getPhysical().toBuilder()
                                        .setFiles(0, bound.getExecutionOptions().getWrite().getOutputBinding()
                                                .getPhysical().getFiles(0).toBuilder()
                                                .setFileSource("forged")))))).build();
        assertThatThrownBy(() -> NativeWriteBinder.bindAttempt(tampered))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sealed task attempt");
    }

    @Test
    void exactAttemptAndOwnerProduceDistinctStagingPaths() {
        TaskAttemptId attempt = TaskAttemptId.newBuilder().setQueryOrJobId("query-1")
                .setTaskId("task-1").setPlanEpoch(1).setFencingToken(2).setControlEpoch(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner-1")
                        .setOwnerIncarnation("incarnation-1").setFencingToken(4)).build();
        String prefix = NativeWriteBinder.attemptPrefix("staging/write-1", attempt);
        assertThat(prefix).startsWith("staging/write-1/attempt-").hasSize(88);
        assertThat(NativeWriteBinder.attemptPrefix("staging/write-1", attempt)).isEqualTo(prefix);
        assertThat(NativeWriteBinder.attemptPrefix("staging/write-1",
                attempt.toBuilder().setAttemptId(1).build())).isNotEqualTo(prefix);
        assertThat(NativeWriteBinder.attemptPrefix("staging/write-1",
                attempt.toBuilder().setControlEpoch(4).build())).isNotEqualTo(prefix);
        assertThatThrownBy(() -> NativeWriteBinder.attemptPrefix("staging/write-1",
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
            assertThatThrownBy(() -> NativeWriteBinder.attemptPrefix("staging", invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fenced attempt");
        }
        for (String prefix : new String[] {null, "", " "}) {
            assertThatThrownBy(() -> NativeWriteBinder.attemptPrefix(prefix, attempt))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("allocation prefix");
        }
    }
}
