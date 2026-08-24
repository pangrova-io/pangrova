package org.pangrova.worker.task;

import org.pangrova.protocol.v1.TaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exhaustive mapping test for TaskTypeMapper.
 * Validates: R5.5, AC7, R12.3
 */
class TaskTypeMapperTest {

    private static final List<String> ALL_REST_TASK_TYPES = List.of(
            "COMPUTE", "OUTPUT", "WRITE");

    @ParameterizedTest
    @CsvSource({
            "COMPUTE, TASK_TYPE_READ",
            "OUTPUT,  TASK_TYPE_READ",
            "WRITE,   TASK_TYPE_WRITE"
    })
    void toProtoType_mapsAllRestTaskTypes(String restType, String expectedProto) {
        assertThat(TaskTypeMapper.toProtoType(restType).name()).isEqualTo(expectedProto.trim());
    }

    @Test
    void toProtoType_computeMapsToRead() {
        assertThat(TaskTypeMapper.toProtoType("COMPUTE")).isEqualTo(TaskType.TASK_TYPE_READ);
    }

    @Test
    void toProtoType_outputMapsToRead() {
        assertThat(TaskTypeMapper.toProtoType("OUTPUT")).isEqualTo(TaskType.TASK_TYPE_READ);
    }

    @Test
    void toProtoType_writeMapsToWrite() {
        assertThat(TaskTypeMapper.toProtoType("WRITE")).isEqualTo(TaskType.TASK_TYPE_WRITE);
    }

    @Test
    void everyRestTaskType_mapsToExactlyOneProtoType() {
        for (String restType : ALL_REST_TASK_TYPES) {
            TaskType protoType = TaskTypeMapper.toProtoType(restType);
            assertThat(protoType)
                    .as("REST taskType %s must map to exactly one Protobuf type", restType)
                    .isNotNull();
            assertThat(protoType.name())
                    .startsWith("TASK_TYPE_");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN", "compute", "READ", "", "SHUFFLE"})
    void toProtoType_throwsOnInvalidType(String invalidType) {
        assertThatThrownBy(() -> TaskTypeMapper.toProtoType(invalidType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown REST taskType");
    }

    @Test
    void toProtoType_throwsOnNull() {
        assertThatThrownBy(() -> TaskTypeMapper.toProtoType(null))
                .isInstanceOf(NullPointerException.class);
    }
}
