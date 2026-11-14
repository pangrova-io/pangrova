package org.pangrova.worker.task;

import org.pangrova.protocol.enums.ReportedTaskState;
import org.pangrova.protocol.v2.TaskState;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Task state mapping consistency across all three layers:
 * Protobuf TaskState, SPI ReportedTaskState, and REST DTO state strings.
 *
 * <p>Ensures TaskStateMapper is the single authoritative mapping entry point
 * and that Protobuf TaskState enum values have 1:1 correspondence with SPI TaskState.</p>
 *
 * Validates: Requirements 5.1, 5.6, 12.3
 */
class TaskStateMappingConsistencyTest {

    /**
     * Protobuf TaskState enum values (excluding UNSPECIFIED and UNRECOGNIZED).
     * Must stay in sync with the .proto definition.
     */
    private static final Set<String> PROTOBUF_TASK_STATES = Set.of(
            "TASK_STATE_PENDING",
            "TASK_STATE_RUNNING",
            "TASK_STATE_FLUSHING",
            "TASK_STATE_FINISHED",
            "TASK_STATE_FAILED",
            "TASK_STATE_CANCELLED"
    );

    @Test
    void spiTaskState_hasOneToOneCorrespondenceWithProtobuf() {
        Set<String> spiNames = Arrays.stream(ReportedTaskState.values())
                .map(s -> "TASK_STATE_" + s.name())
                .collect(Collectors.toSet());

        assertThat(spiNames)
                .as("SPI TaskState enum values must match Protobuf TaskState 1:1")
                .isEqualTo(PROTOBUF_TASK_STATES);
    }

    @Test
    void toProtoState_outputsOnlyValidProtobufValues() {
        Set<String> mapped = Arrays.stream(org.pangrova.worker.enums.TaskState.values())
                .map(TaskStateMapper::toProtoState)
                .map(TaskState::name)
                .collect(Collectors.toSet());

        assertThat(PROTOBUF_TASK_STATES)
                .as("Every toProtoState output must be a valid Protobuf TaskState")
                .containsAll(mapped);
    }

    @Test
    void toProtoState_coversAllProtobufValues() {
        Set<String> mapped = Arrays.stream(org.pangrova.worker.enums.TaskState.values())
                .map(TaskStateMapper::toProtoState)
                .map(TaskState::name)
                .collect(Collectors.toSet());

        assertThat(mapped)
                .as("toProtoState must cover all Protobuf TaskState values")
                .isEqualTo(PROTOBUF_TASK_STATES);
    }

    @Test
    void toReportedState_coversAllSpiValues() {
        Set<ReportedTaskState> mapped = Arrays.stream(org.pangrova.worker.enums.TaskState.values())
                .map(TaskStateMapper::toReportedState)
                .collect(Collectors.toSet());

        assertThat(mapped)
                .as("toReportedState must cover all SPI TaskState values")
                .containsExactlyInAnyOrder(ReportedTaskState.values());
    }

    @Test
    void fromRestState_roundTripsWithToProtoState() {
        for (ReportedTaskState spiState : ReportedTaskState.values()) {
            String restName = spiState.name();
            ReportedTaskState roundTripped = TaskStateMapper.fromRestState(restName);
            assertThat(roundTripped)
                    .as("fromRestState(%s) must return the original SPI state", restName)
                    .isEqualTo(spiState);
        }
    }

    @Test
    void taskStateMapper_isPackagePrivate_singleEntryPoint() {
        assertThat(java.lang.reflect.Modifier.isPublic(TaskStateMapper.class.getModifiers()))
                .as("TaskStateMapper must be package-private to enforce single entry point")
                .isFalse();
    }
}
