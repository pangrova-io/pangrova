package org.pangrova.coordinator.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.junit.jupiter.api.Test;

/**
 * Verifies that SubmitTaskRequestConverter preserves ALL fields during
 * DTO→SPI conversion — no field silently dropped or defaulted.
 *
 * <p>Validates: Requirements REQ-M7 (acceptance criterion 43)</p>
 */
class SubmitTaskRequestConverterFieldPreservationTest {

    private static final String PLAN_B64 = Base64.getEncoder().encodeToString("test-plan".getBytes());
    private static final String SPLIT_B64 = Base64.getEncoder().encodeToString("split-data".getBytes());

    @Test
    void preservesStageIdAndPartitionId() {
        SubmitTaskRequest request = buildFullRequest(3, 7, "OUTPUT");

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.stageId()).isEqualTo(3);
        assertThat(descriptor.partitionId()).isEqualTo(7);
    }

    @Test
    void preservesTaskType() {
        SubmitTaskRequest request = buildFullRequest(1, 0, "OUTPUT");

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.taskType()).isEqualTo("OUTPUT");
    }

    @Test
    void preservesResourceLimit() {
        ResourceLimitPayload limit = new ResourceLimitPayload(256_000_000L, 8, 512_000_000L, "/tmp/spill");
        SubmitTaskRequest request = buildRequestWithResourceLimit(limit);

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        TaskResourceLimit spiLimit = descriptor.resourceLimit();
        assertThat(spiLimit.maxMemoryBytes()).isEqualTo(256_000_000L);
        assertThat(spiLimit.maxThreads()).isEqualTo(8);
        assertThat(spiLimit.maxSpillBytes()).isEqualTo(512_000_000L);
        assertThat(spiLimit.spillDirectory()).isEqualTo("/tmp/spill");
    }

    @Test
    void nullResourceLimitDefaultsToNone() {
        SubmitTaskRequest request = buildRequestWithResourceLimit(null);

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.resourceLimit()).isEqualTo(TaskResourceLimit.NONE);
    }

    @Test
    void nullTaskTypeDefaultsToCompute() {
        SubmitTaskRequest request = buildFullRequest(0, 0, null);

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.taskType()).isEqualTo("COMPUTE");
    }

    @Test
    void preservesAllCoreFields() {
        SubmitTaskRequest request = buildFullRequest(2, 5, "COMPUTE");

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.queryId().toString()).isEqualTo("q-full");
        assertThat(descriptor.planFragment()).isNotNull();
        assertThat(descriptor.splits()).hasSize(1);
        assertThat(descriptor.sessionProperties()).containsEntry("timezone", "UTC");
        assertThat(descriptor.stageId()).isEqualTo(2);
        assertThat(descriptor.partitionId()).isEqualTo(5);
        assertThat(descriptor.taskType()).isEqualTo("COMPUTE");
    }

    @Test
    void toResourceLimitDelegatesCorrectly() {
        ResourceLimitPayload limit = new ResourceLimitPayload(100L, 2, 200L, "/spill");

        TaskResourceLimit result = SubmitTaskRequestConverter.toResourceLimit(limit);

        assertThat(result.maxMemoryBytes()).isEqualTo(100L);
        assertThat(result.maxThreads()).isEqualTo(2);
        assertThat(result.maxSpillBytes()).isEqualTo(200L);
        assertThat(result.spillDirectory()).isEqualTo("/spill");
    }

    @Test
    void toResourceLimitNullReturnsNone() {
        TaskResourceLimit result = SubmitTaskRequestConverter.toResourceLimit(null);

        assertThat(result).isEqualTo(TaskResourceLimit.NONE);
    }

    private static SubmitTaskRequest buildFullRequest(int stageId, int partitionId, String taskType) {
        return new SubmitTaskRequest(
                "t-1", "q-full", stageId, partitionId, taskType,
                PLAN_B64, List.of(SPLIT_B64), List.of(),
                new ResourceLimitPayload(128_000_000L, 4, 0L, ""),
                Map.of("timezone", "UTC"), null, null,
                null, null, null, null, null, null, null);
    }

    private static SubmitTaskRequest buildRequestWithResourceLimit(ResourceLimitPayload limit) {
        return new SubmitTaskRequest(
                "t-2", "q-rl", 0, 0, "COMPUTE",
                PLAN_B64, List.of(), List.of(),
                limit, Map.of(), null, null,
                null, null, null, null, null, null, null);
    }
}
