package org.pangrova.protocol.rest.task.convert;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.Split;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.junit.jupiter.api.Test;

/**
 * Verifies the shared DTO→SPI conversion logic in {@link TaskRequestDtoConverter}.
 * Both coordinator and worker delegate to this class.
 */
class TaskRequestDtoConverterTest {

    private static final String PLAN_B64 =
            Base64.getEncoder().encodeToString("test-plan".getBytes());
    private static final String SPLIT_B64 =
            Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});

    @Test
    void toTaskDescriptor_preservesAllFields() {
        SubmitTaskRequest request = new SubmitTaskRequest(
                "task-1", "q-1", 2, 3, "OUTPUT", PLAN_B64,
                List.of(SPLIT_B64), null,
                new ResourceLimitPayload(1024, 4, 2048, "/spill"),
                Map.of("key", "val"), null, null,
                null, null, null, null, null, null, null);

        TaskDescriptor desc = TaskRequestDtoConverter.toTaskDescriptor(request);

        assertThat(desc.queryId().id()).isEqualTo("q-1");
        assertThat(desc.stageId()).isEqualTo(2);
        assertThat(desc.partitionId()).isEqualTo(3);
        assertThat(desc.taskType()).isEqualTo("OUTPUT");
        assertThat(desc.splits()).hasSize(1);
        assertThat(desc.sessionProperties()).containsEntry("key", "val");
        assertThat(desc.resourceLimit().maxMemoryBytes()).isEqualTo(1024);
    }

    @Test
    void toTaskDescriptor_defaultsTaskTypeToCompute() {
        SubmitTaskRequest request = new SubmitTaskRequest(
                "t-1", "q-2", 0, 0, null, PLAN_B64,
                null, null, null, null, null, null,
                null, null, null, null, null, null, null);

        TaskDescriptor desc = TaskRequestDtoConverter.toTaskDescriptor(request);

        assertThat(desc.taskType()).isEqualTo("COMPUTE");
    }

    @Test
    void toResourceLimit_mapsAllFields() {
        ResourceLimitPayload limit = new ResourceLimitPayload(4096, 8, 8192, "/spill");

        TaskResourceLimit result = TaskRequestDtoConverter.toResourceLimit(limit);

        assertThat(result.maxMemoryBytes()).isEqualTo(4096);
        assertThat(result.maxThreads()).isEqualTo(8);
        assertThat(result.maxSpillBytes()).isEqualTo(8192);
        assertThat(result.spillDirectory()).isEqualTo("/spill");
    }

    @Test
    void toResourceLimit_nullReturnsNone() {
        TaskResourceLimit result = TaskRequestDtoConverter.toResourceLimit(null);

        assertThat(result).isEqualTo(TaskResourceLimit.NONE);
    }

    @Test
    void toResourceLimit_nullSpillDirectoryDefaultsToEmpty() {
        ResourceLimitPayload limit = new ResourceLimitPayload(100, 2, 200, null);

        TaskResourceLimit result = TaskRequestDtoConverter.toResourceLimit(limit);

        assertThat(result.spillDirectory()).isEmpty();
    }

    @Test
    void toPlanFragment_decodesBase64() {
        PlanFragment fragment = TaskRequestDtoConverter.toPlanFragment(PLAN_B64);

        assertThat(fragment).isInstanceOf(TaskRequestDtoConverter.SerializedPlanFragment.class);
        var serialized = (TaskRequestDtoConverter.SerializedPlanFragment) fragment;
        assertThat(serialized.planBytes()).isEqualTo("test-plan".getBytes());
    }

    @Test
    void toPlanFragment_nullReturnsEmptyBytes() {
        PlanFragment fragment = TaskRequestDtoConverter.toPlanFragment(null);

        var serialized = (TaskRequestDtoConverter.SerializedPlanFragment) fragment;
        assertThat(serialized.planBytes()).isEmpty();
    }

    @Test
    void toSplits_decodesBase64List() {
        List<Split> splits = TaskRequestDtoConverter.toSplits(List.of(SPLIT_B64));

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0)).isInstanceOf(TaskRequestDtoConverter.SerializedSplit.class);
    }

    @Test
    void toSplits_nullReturnsEmptyList() {
        assertThat(TaskRequestDtoConverter.toSplits(null)).isEmpty();
    }

    @Test
    void toSplits_emptyListReturnsEmptyList() {
        assertThat(TaskRequestDtoConverter.toSplits(List.of())).isEmpty();
    }

    @Test
    void serializedPlanFragment_providesStableId() {
        var fragment = new TaskRequestDtoConverter.SerializedPlanFragment(new byte[]{1});

        assertThat(fragment.getId().toString()).isEqualTo("serialized-fragment");
        assertThat(fragment.getChildFragmentIds()).isEmpty();
    }

    @Test
    void toTaskDescriptor_nullSessionPropertiesDefaultsToEmpty() {
        SubmitTaskRequest request = new SubmitTaskRequest(
                "t-1", "q-3", 0, 0, "COMPUTE", PLAN_B64,
                null, null, null, null, null, null,
                null, null, null, null, null, null, null);

        TaskDescriptor desc = TaskRequestDtoConverter.toTaskDescriptor(request);

        assertThat(desc.sessionProperties()).isEmpty();
    }
}
