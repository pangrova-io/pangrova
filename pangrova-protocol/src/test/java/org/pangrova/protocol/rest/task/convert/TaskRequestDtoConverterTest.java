package org.pangrova.protocol.rest.task.convert;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;

/** Verifies canonical v2 task-to-SPI projections shared by coordinator and worker. */
class TaskRequestDtoConverterTest {

    private static final byte[] PLAN = "test-plan".getBytes(StandardCharsets.UTF_8);

    @Test
    void toTaskDescriptorPreservesCanonicalFields() {
        var canonical = CanonicalTaskRequestTestFixture.builder(
                        "task-1", "q-1", TaskKind.TASK_KIND_READ, PLAN)
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId("q-1").setTaskId("task-1")
                        .setStageId(2).setPartitionId(3).setPlanEpoch(1)
                        .setFencingToken((1L << 32) | 1L).setControlEpoch(1)
                        .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId("coordinator").setOwnerIncarnation("test")
                                .setFencingToken(1)))
                .addSplits(DataSplit.newBuilder().setSplitId("split-1"))
                .setResourceGrant(grant(1024, 4, 2048))
                .setExecutionOptions(TaskExecutionOptions.newBuilder()
                        .putAllSessionProperties(Map.of("key", "val"))
                        .setDirectResultOutput(true))
                .build();

        TaskDescriptor descriptor = TaskRequestDtoConverter.toTaskDescriptor(
                SubmitTaskRequest.from(canonical));

        assertThat(descriptor.queryId().id()).isEqualTo("q-1");
        assertThat(descriptor.stageId()).isEqualTo(2);
        assertThat(descriptor.partitionId()).isEqualTo(3);
        assertThat(descriptor.taskType()).isEqualTo("OUTPUT");
        assertThat(descriptor.splits()).hasSize(1);
        assertThat(descriptor.sessionProperties()).containsEntry("key", "val");
        assertThat(descriptor.resourceLimit().maxMemoryBytes()).isEqualTo(1024);
        var serialized = (TaskRequestDtoConverter.SerializedPlanFragment)
                descriptor.planFragment();
        assertThat(serialized.planBytes()).isEqualTo(PLAN);
    }

    @Test
    void readTaskWithoutDirectOutputProjectsToCompute() {
        var canonical = CanonicalTaskRequestTestFixture.builder(
                "task-1", "q-2", TaskKind.TASK_KIND_READ, PLAN).build();

        TaskDescriptor descriptor = TaskRequestDtoConverter.toTaskDescriptor(
                SubmitTaskRequest.from(canonical));

        assertThat(descriptor.taskType()).isEqualTo("COMPUTE");
        assertThat(descriptor.sessionProperties()).isEmpty();
    }

    @Test
    void cdcIngestProjectsToItsWorkerTaskTypeWithoutAPlan() {
        var canonical = CanonicalTaskRequestTestFixture.builder(
                "task-cdc", "q-cdc", TaskKind.TASK_KIND_CDC_INGEST, new byte[0]).build();

        TaskDescriptor descriptor = TaskRequestDtoConverter.toTaskDescriptor(
                SubmitTaskRequest.from(canonical));

        assertThat(descriptor.taskType()).isEqualTo("CDC_INGEST");
        assertThat(descriptor.splits()).isEmpty();
        assertThat(((TaskRequestDtoConverter.SerializedPlanFragment)
                descriptor.planFragment()).planBytes()).isEmpty();
    }

    @Test
    void toResourceLimitMapsCanonicalGrantAndNeverAcceptsCoordinatorPath() {
        TaskResourceLimit result = TaskRequestDtoConverter.toResourceLimit(
                grant(4096, 8, 8192));

        assertThat(result.maxMemoryBytes()).isEqualTo(4096);
        assertThat(result.maxThreads()).isEqualTo(8);
        assertThat(result.maxSpillBytes()).isEqualTo(8192);
        assertThat(result.spillDirectory()).isEmpty();
    }

    @Test
    void absentResourceGrantProjectsToNoLimit() {
        assertThat(TaskRequestDtoConverter.toResourceLimit(null))
                .isEqualTo(TaskResourceLimit.NONE);
    }

    @Test
    void serializedPlanFragmentProvidesStableId() {
        var fragment = new TaskRequestDtoConverter.SerializedPlanFragment(new byte[]{1});

        assertThat(fragment.getId().toString()).isEqualTo("serialized-fragment");
        assertThat(fragment.getChildFragmentIds()).isEmpty();
    }

    private static StageResourceGrant grant(long memory, int cpu, long spill) {
        return StageResourceGrant.newBuilder()
                .setGrantId("task-1:grant")
                .setNativeMemoryBytes(memory)
                .setCpuSlots(cpu)
                .setSpillBytes(spill)
                .build();
    }
}
