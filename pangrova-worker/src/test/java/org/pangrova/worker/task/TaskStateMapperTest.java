package org.pangrova.worker.task;

import org.pangrova.protocol.enums.ReportedTaskState;
import org.pangrova.protocol.v2.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exhaustive mapping test for TaskStateMapper.
 * Validates: R5.2, AC7
 */
class TaskStateMapperTest {

    @ParameterizedTest
    @EnumSource(org.pangrova.worker.enums.TaskState.class)
    void toReportedState_returnsNonNullForEveryWorkerState(org.pangrova.worker.enums.TaskState state) {
        ReportedTaskState result = TaskStateMapper.toReportedState(state);
        assertThat(result).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(org.pangrova.worker.enums.TaskState.class)
    void toProtoState_returnsNonNullForEveryWorkerState(org.pangrova.worker.enums.TaskState state) {
        TaskState result = TaskStateMapper.toProtoState(state);
        assertThat(result).isNotNull();
        assertThat(result.name()).isNotBlank();
    }

    @Test
    void toReportedState_receivedMapsToPending() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.RECEIVED))
                .isEqualTo(ReportedTaskState.PENDING);
    }

    @Test
    void toReportedState_initializingMapsToPending() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.INITIALIZING))
                .isEqualTo(ReportedTaskState.PENDING);
    }

    @Test
    void toReportedState_runningMapsToRunning() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.RUNNING))
                .isEqualTo(ReportedTaskState.RUNNING);
    }

    @Test
    void toReportedState_finishingMapsToFlushing() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.FINISHING))
                .isEqualTo(ReportedTaskState.FLUSHING);
    }

    @Test
    void toReportedState_cancellingRemainsNonTerminal() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.CANCELLING))
                .isEqualTo(ReportedTaskState.RUNNING);
    }

    @Test
    void toReportedState_finishedMapsToFinished() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.FINISHED))
                .isEqualTo(ReportedTaskState.FINISHED);
    }

    @Test
    void toReportedState_failedMapsToFailed() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.FAILED))
                .isEqualTo(ReportedTaskState.FAILED);
    }

    @Test
    void toReportedState_cancelledMapsToCancelled() {
        assertThat(TaskStateMapper.toReportedState(org.pangrova.worker.enums.TaskState.CANCELLED))
                .isEqualTo(ReportedTaskState.CANCELLED);
    }

    @Test
    void toProtoState_receivedMapsToPending() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.RECEIVED))
                .isEqualTo(TaskState.TASK_STATE_PENDING);
    }

    @Test
    void toProtoState_initializingMapsToPending() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.INITIALIZING))
                .isEqualTo(TaskState.TASK_STATE_PENDING);
    }

    @Test
    void toProtoState_runningMapsToRunning() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.RUNNING))
                .isEqualTo(TaskState.TASK_STATE_RUNNING);
    }

    @Test
    void toProtoState_finishingMapsToFlushing() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.FINISHING))
                .isEqualTo(TaskState.TASK_STATE_FLUSHING);
    }

    @Test
    void toProtoState_cancellingRemainsNonTerminal() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.CANCELLING))
                .isEqualTo(TaskState.TASK_STATE_RUNNING);
    }

    @Test
    void toProtoState_finishedMapsToFinished() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.FINISHED))
                .isEqualTo(TaskState.TASK_STATE_FINISHED);
    }

    @Test
    void toProtoState_failedMapsToFailed() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.FAILED))
                .isEqualTo(TaskState.TASK_STATE_FAILED);
    }

    @Test
    void toProtoState_cancelledMapsToCancelled() {
        assertThat(TaskStateMapper.toProtoState(org.pangrova.worker.enums.TaskState.CANCELLED))
                .isEqualTo(TaskState.TASK_STATE_CANCELLED);
    }

    @Test
    void everyWorkerState_mapsToExactlyOneSpiState() {
        for (org.pangrova.worker.enums.TaskState state : org.pangrova.worker.enums.TaskState.values()) {
            ReportedTaskState spiState = TaskStateMapper.toReportedState(state);
            assertThat(spiState)
                    .as("Worker state %s must map to exactly one SPI state", state)
                    .isNotNull();
        }
    }

    @Test
    void everyWorkerState_mapsToExactlyOneProtoState() {
        for (org.pangrova.worker.enums.TaskState state : org.pangrova.worker.enums.TaskState.values()) {
            TaskState protoState = TaskStateMapper.toProtoState(state);
            assertThat(protoState)
                    .as("Worker state %s must map to exactly one Protobuf state", state)
                    .isNotNull();
            assertThat(protoState.name()).startsWith("TASK_STATE_");
        }
    }
}
