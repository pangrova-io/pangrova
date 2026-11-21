package org.pangrova.protocol.write;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.PhysicalFileAllocation;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.protocol.v2.PhysicalFileWriteFragment;
import org.pangrova.protocol.v2.TaskAttemptId;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Codec validation only; these specimens do not assert physical publication or object existence. */
class PhysicalFileWriteReceiptsTest {

    @Test
    void bindsEmptyFragmentToEveryTrustedAttemptField() {
        var expected = attempt();
        var bound = NativeWriteBindings.bindPhysical(template(), expected);
        var fragment = emptyFragment(expected);
        assertThat(PhysicalFileWriteReceipts.validateTaskFragment(bound, expected, fragment.toByteArray()))
                .isEqualTo(fragment);
        for (var foreign : List.of(
                expected.toBuilder().setAttemptId(9).build(),
                expected.toBuilder().setTaskId("other-task").build(),
                expected.toBuilder().setQueryOrJobId("other-query").build(),
                expected.toBuilder().setStageId(9).build(),
                expected.toBuilder().setPartitionId(9).build(),
                expected.toBuilder().setControlEpoch(9).build(),
                expected.toBuilder().setPlanEpoch(9).build(),
                expected.toBuilder().setFencingToken(9).build(),
                expected.toBuilder().setExecutionOwner(expected.getExecutionOwner().toBuilder()
                        .setOwnerIncarnation("other-process")).build())) {
            assertThatThrownBy(() -> PhysicalFileWriteReceipts.validateTaskFragment(
                    bound, expected, emptyFragment(foreign).toByteArray()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dispatched task attempt");
        }
    }

    @Test
    void rejectsTemplateOrAllocationBoundToAnotherAttempt() {
        var expected = attempt();
        var fragment = emptyFragment(expected).toByteArray();
        assertThatThrownBy(() -> PhysicalFileWriteReceipts.validateTaskFragment(template(), expected, fragment))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dispatched task attempt");
        var foreign = expected.toBuilder().setAttemptId(9).build();
        var bound = NativeWriteBindings.bindPhysical(template(), foreign);
        assertThatThrownBy(() -> PhysicalFileWriteReceipts.validateTaskFragment(bound, expected, fragment))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dispatched task attempt");
        var expectedBound = NativeWriteBindings.bindPhysical(template(), expected);
        var mismatchedFile = expectedBound.toBuilder().setFiles(0,
                expectedBound.getFiles(0).toBuilder().setTaskAttempt(foreign)).build();
        assertThatThrownBy(() -> PhysicalFileWriteReceipts.validateTaskFragment(mismatchedFile, expected, fragment))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dispatched task attempt");
    }

    @Test
    void rejectsMissingMalformedOversizedOrRowClaimingFragments() {
        var expected = attempt();
        var bound = NativeWriteBindings.bindPhysical(template(), expected);
        for (byte[] bytes : new byte[][] {null, new byte[0], {0}, new byte[16 * 1024 * 1024 + 1]}) {
            assertThatThrownBy(() -> PhysicalFileWriteReceipts.validateTaskFragment(bound, expected, bytes))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> PhysicalFileWriteReceipts.validateTaskFragment(bound, expected,
                emptyFragment(expected).toBuilder().setInputRows(1).build().toByteArray()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rows differ");
    }

    private static PhysicalFileWriteBinding template() {
        return PhysicalFileWriteBinding.newBuilder().setWriteId("write").setGeneration("generation")
                .setAllocationPrefix("objects/write")
                .setPhysicalSchemaChecksum(ProtocolChecksums.sha256(new byte[] {1, 2}))
                .addFiles(PhysicalFileAllocation.newBuilder().setFileName("data.lance")
                        .setOpaqueLocation("objects/write/data.lance")).build();
    }

    private static PhysicalFileWriteFragment emptyFragment(TaskAttemptId attempt) {
        return PhysicalFileWriteFragment.newBuilder().setContractVersion(PhysicalFileWriteReceipts.CONTRACT_VERSION)
                .setWriteId(template().getWriteId()).setGeneration(template().getGeneration())
                .setPhysicalSchemaChecksum(template().getPhysicalSchemaChecksum()).setTaskAttempt(attempt).build();
    }

    private static TaskAttemptId attempt() {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId("task")
                .setStageId(2).setPartitionId(3).setPlanEpoch(4).setAttemptId(5)
                .setControlEpoch(6).setFencingToken(7).setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("owner").setOwnerIncarnation("process").setFencingToken(8)).build();
    }
}
