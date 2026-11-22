package org.pangrova.protocol.rest.task;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.http.TaskProgress;
import org.pangrova.protocol.http.TaskRole;
import org.pangrova.protocol.http.TaskStatusDelivery;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskStatusWireTest {
    private static final TaskAttemptId ATTEMPT = TaskAttemptId.newBuilder().setTaskId("task")
            .setQueryOrJobId("query").setPlanEpoch(1).setFencingToken(2).setControlEpoch(3).build();

    @Test
    void terminalWireRetainsExactEvidenceAndOmitsDuplicateStats() throws Exception {
        var stats = NativeTaskStats.newBuilder().setRowsRead(80).setCacheMissBytes(22).build();
        var terminal = TaskReport.newBuilder().setAttempt(ATTEMPT)
                .setState(TaskState.TASK_STATE_FINISHED).setNativeTaskStats(stats)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
        // Protobuf uses the last value of a singular field. Preserve the actual
        // published bytes even when reserializing the parsed object differs.
        var bytes = ByteString.newOutput();
        var prefix = CodedOutputStream.newInstance(bytes);
        prefix.writeEnum(TaskReport.STATE_FIELD_NUMBER, TaskState.TASK_STATE_RUNNING_VALUE);
        terminal.writeTo(prefix);
        prefix.flush();
        byte[] exact = bytes.toByteString().toByteArray();
        var report = TaskStatusReport.fromAttempt(ATTEMPT, "OUTPUT", "worker", "FINISHED",
                TaskStats.fromNative(stats), null, null, 5, 1000, exact);

        byte[] encoded = TaskStatusWire.encode(report);
        var wire = TaskStatusDelivery.parseFrom(encoded);
        assertThat(wire.hasSupplementalStats()).isFalse();
        assertThat(wire.hasProgress()).isFalse();
        var decoded = TaskStatusWire.decode(encoded);
        assertThat(decoded.reportedAttempt()).isEqualTo(ATTEMPT);
        assertThat(decoded.canonicalStatus()).isSameAs(decoded.canonicalStatus()).isEqualTo(terminal);
        assertThat(decoded.stats().rowsRead()).isEqualTo(80);
        assertThat(decoded.canonicalTaskReport()).containsExactly(exact);
        assertThat(TaskStatusWire.encode(decoded)).containsExactly(encoded);
    }

    @Test
    void writeDeliveryPreservesOpaqueCommitEvidenceAndSupplementalStats() throws Exception {
        var terminal = TaskReport.newBuilder().setAttempt(ATTEMPT)
                .setState(TaskState.TASK_STATE_FINISHED).build();
        var write = new WriteResultDescriptor(List.of(), 80, 512, new byte[]{1, 2},
                List.of(new byte[]{5, (byte) 0x80}, new byte[]{7}));
        var stats = TaskStats.fromNative(NativeTaskStats.newBuilder().setRowsWritten(80).build());
        var report = TaskStatusReport.fromAttempt(ATTEMPT, "WRITE", "worker", "FINISHED",
                stats, null, write, 6, 1001, terminal.toByteArray());

        var decoded = TaskStatusWire.decode(TaskStatusWire.encode(report));
        assertThat(decoded.writeResult()).isEqualTo(write);
        assertThat(decoded.stats()).isEqualTo(stats);
    }

    @Test
    void progressCannotClaimATerminalStateOrSupplyEmptyTerminalEvidence() {
        var frame = TaskStatusDelivery.newBuilder().setWorkerId("worker")
                .setRole(TaskRole.TASK_ROLE_COMPUTE).setSequence(4);
        byte[] falseProgress = frame.setProgress(TaskProgress.newBuilder().setAttempt(ATTEMPT)
                .setState(TaskState.TASK_STATE_FINISHED)).build().toByteArray();
        assertThatThrownBy(() -> TaskStatusWire.decode(falseProgress))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid task progress");

        byte[] missingEvidence = frame.setTerminalReport(ByteString.EMPTY).build().toByteArray();
        assertThatThrownBy(() -> TaskStatusWire.decode(missingEvidence))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("report size");
    }
}
