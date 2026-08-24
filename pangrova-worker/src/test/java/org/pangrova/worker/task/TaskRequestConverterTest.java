package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.worker.task.convert.TaskRequestConverter;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Verifies TaskRequestConverter produces correct SPI types from REST DTOs.
 * No mocks — pure conversion logic.
 */
class TaskRequestConverterTest {

    @Test
    void toTaskDescriptor_preservesAllFields() {
        String planB64 = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});
        String splitB64 = Base64.getEncoder().encodeToString(new byte[]{4, 5});
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("q-1")
                .stageId(2)
                .partitionId(3)
                .taskType("OUTPUT")
                .planFragment(planB64)
                .splits(List.of(splitB64))
                .exchangeSources(null)
                .resourceLimit(new ResourceLimitPayload(1024, 4, 2048, "/tmp/spill"))
                .sessionProperties(Map.of("key", "val"))
                .outputConfig(null)
                .writeConfig(null)
                .build();

        TaskDescriptor desc = TaskRequestConverter.toTaskDescriptor(request);

        assertThat(desc.queryId().id()).isEqualTo("q-1");
        assertThat(desc.stageId()).isEqualTo(2);
        assertThat(desc.partitionId()).isEqualTo(3);
        assertThat(desc.taskType()).isEqualTo("OUTPUT");
        assertThat(desc.splits()).hasSize(1);
        assertThat(desc.sessionProperties()).containsEntry("key", "val");
    }

    @Test
    void toResourceLimit_mapsAllFields() {
        ResourceLimitPayload limit = new ResourceLimitPayload(4096, 8, 8192, "/spill");

        TaskResourceLimit result = TaskRequestConverter.toResourceLimit(limit);

        assertThat(result.maxMemoryBytes()).isEqualTo(4096);
        assertThat(result.maxThreads()).isEqualTo(8);
        assertThat(result.maxSpillBytes()).isEqualTo(8192);
        assertThat(result.spillDirectory()).isEqualTo("/spill");
    }

    @Test
    void toResourceLimit_nullReturnsNone() {
        TaskResourceLimit result = TaskRequestConverter.toResourceLimit(null);

        assertThat(result).isEqualTo(TaskResourceLimit.NONE);
    }

    @Test
    void toTaskDescriptor_defaultsTaskTypeToCompute() {
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("q-1")
                .stageId(0)
                .partitionId(0)
                .taskType(null)
                .planFragment(null)
                .splits(null)
                .exchangeSources(null)
                .resourceLimit(null)
                .sessionProperties(null)
                .outputConfig(null)
                .writeConfig(null)
                .build();

        TaskDescriptor desc = TaskRequestConverter.toTaskDescriptor(request);

        assertThat(desc.taskType()).isEqualTo("COMPUTE");
    }

    @Test
    void toSplits_nullReturnsEmptyList() {
        assertThat(TaskRequestConverter.toSplits(null)).isEmpty();
    }
}
