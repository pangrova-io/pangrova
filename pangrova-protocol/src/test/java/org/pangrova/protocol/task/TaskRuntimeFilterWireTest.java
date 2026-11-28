package org.pangrova.protocol.task;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TypedValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskRuntimeFilterWireTest {
    @Test
    void keepsTheExactAttemptAndOneTypedUpdate() throws Exception {
        var attempt = attempt();
        var update = update();
        byte[] encoded = TaskRuntimeFilterWire.encode(attempt, update);
        var decoded = TaskRuntimeFilterWire.decode(encoded);
        assertThat(decoded.getAttempt()).isEqualTo(attempt);
        assertThat(decoded.getUpdate()).isEqualTo(update);
        assertThat(encoded.length).isEqualTo(decoded.getSerializedSize());
    }

    @Test
    void acceptsOnlyAnExplicitSoleScanSelector() throws Exception {
        var attempt = attempt();
        var sole = update().toBuilder().setSoleTableScan(true).build();
        assertThat(TaskRuntimeFilterWire.decode(TaskRuntimeFilterWire.encode(attempt, sole)).getUpdate())
                .isEqualTo(sole);
        assertThatThrownBy(() -> TaskRuntimeFilterWire.encode(attempt, sole.toBuilder().setSoleTableScan(false).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scan");
    }

    @Test
    void rejectsMalformedOversizedOrUnboundUpdates() {
        var attempt = attempt();
        var update = update();
        assertThatThrownBy(() -> TaskRuntimeFilterWire.decode(new byte[] {1}))
                .isInstanceOf(InvalidProtocolBufferException.class);
        assertThatThrownBy(() -> TaskRuntimeFilterWire.decode(new byte[TaskRuntimeFilterWire.MAX_BODY_BYTES + 1]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("size");
        assertThatThrownBy(() -> TaskRuntimeFilterWire.encode(attempt.toBuilder().clearWorkerExecutionIdentity().build(),
                update)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exact Worker delivery");
        assertThatThrownBy(() -> TaskRuntimeFilterWire.encode(attempt, update.toBuilder().clearPlanNodeId().build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scan");
    }

    @Test
    void rejectsPartitionPruningUpdatesAtTheWorkerBoundary() {
        var update = update();
        var partitionOnly = update.getFilters(0).toBuilder()
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_PARTITION_PRUNING);
        assertThatThrownBy(() -> TaskRuntimeFilterWire.encode(attempt(),
                update.toBuilder().setFilters(0, partitionOnly).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("typed predicates");
    }

    private static TaskAttemptId attempt() {
        var worker = new WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002");
        return CanonicalTaskRequestTestFixture.builder("task", "query", TaskKind.TASK_KIND_READ, new byte[] {1})
                .getAttempt().toBuilder().setWorkerExecutionIdentity(worker.toProto()).build();
    }

    private static RuntimeFilterBinding update() {
        return RuntimeFilterBinding.newBuilder().setPlanNodeId("scan")
                .addFilters(RuntimeFilter.newBuilder().setColumnName("id")
                        .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_ROW_FILTER)
                        .setRange(RangeFilter.newBuilder()
                                .setLower(TypedValue.newBuilder().setInt64Value(2))
                                .setUpper(TypedValue.newBuilder().setInt64Value(9))
                                .setLowerInclusive(true).setUpperInclusive(true))).build();
    }
}
