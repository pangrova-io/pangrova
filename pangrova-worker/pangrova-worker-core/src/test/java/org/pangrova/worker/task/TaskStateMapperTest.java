package org.pangrova.worker.task;

import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.execution.TaskStatus;
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
    @EnumSource(TaskManager.TaskState.class)
    void toSpiState_returnsNonNullForEveryWorkerState(TaskManager.TaskState state) {
        TaskStatus.TaskState result = TaskStateMapper.toSpiState(state);
        assertThat(result).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(TaskManager.TaskState.class)
    void toProtoState_returnsNonNullForEveryWorkerState(TaskManager.TaskState state) {
        TaskState result = TaskStateMapper.toProtoState(state);
        assertThat(result).isNotNull();
        assertThat(result.name()).isNotBlank();
    }

    @Test
    void toSpiState_receivedMapsToPending() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.RECEIVED))
                .isEqualTo(TaskStatus.TaskState.PENDING);
    }

    @Test
    void toSpiState_initializingMapsToPending() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.INITIALIZING))
                .isEqualTo(TaskStatus.TaskState.PENDING);
    }

    @Test
    void toSpiState_runningMapsToRunning() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.RUNNING))
                .isEqualTo(TaskStatus.TaskState.RUNNING);
    }

    @Test
    void toSpiState_finishingMapsToFlushing() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.FINISHING))
                .isEqualTo(TaskStatus.TaskState.FLUSHING);
    }

    @Test
    void toSpiState_cancellingRemainsNonTerminal() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.CANCELLING))
                .isEqualTo(TaskStatus.TaskState.RUNNING);
    }

    @Test
    void toSpiState_finishedMapsToFinished() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.FINISHED))
                .isEqualTo(TaskStatus.TaskState.FINISHED);
    }

    @Test
    void toSpiState_failedMapsToFailed() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.FAILED))
                .isEqualTo(TaskStatus.TaskState.FAILED);
    }

    @Test
    void toSpiState_cancelledMapsToCancelled() {
        assertThat(TaskStateMapper.toSpiState(TaskManager.TaskState.CANCELLED))
                .isEqualTo(TaskStatus.TaskState.CANCELLED);
    }

    @Test
    void toProtoState_receivedMapsToPending() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.RECEIVED))
                .isEqualTo(TaskState.TASK_STATE_PENDING);
    }

    @Test
    void toProtoState_initializingMapsToPending() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.INITIALIZING))
                .isEqualTo(TaskState.TASK_STATE_PENDING);
    }

    @Test
    void toProtoState_runningMapsToRunning() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.RUNNING))
                .isEqualTo(TaskState.TASK_STATE_RUNNING);
    }

    @Test
    void toProtoState_finishingMapsToFlushing() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.FINISHING))
                .isEqualTo(TaskState.TASK_STATE_FLUSHING);
    }

    @Test
    void toProtoState_cancellingRemainsNonTerminal() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.CANCELLING))
                .isEqualTo(TaskState.TASK_STATE_RUNNING);
    }

    @Test
    void toProtoState_finishedMapsToFinished() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.FINISHED))
                .isEqualTo(TaskState.TASK_STATE_FINISHED);
    }

    @Test
    void toProtoState_failedMapsToFailed() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.FAILED))
                .isEqualTo(TaskState.TASK_STATE_FAILED);
    }

    @Test
    void toProtoState_cancelledMapsToCancelled() {
        assertThat(TaskStateMapper.toProtoState(TaskManager.TaskState.CANCELLED))
                .isEqualTo(TaskState.TASK_STATE_CANCELLED);
    }

    @Test
    void everyWorkerState_mapsToExactlyOneSpiState() {
        for (TaskManager.TaskState state : TaskManager.TaskState.values()) {
            TaskStatus.TaskState spiState = TaskStateMapper.toSpiState(state);
            assertThat(spiState)
                    .as("Worker state %s must map to exactly one SPI state", state)
                    .isNotNull();
        }
    }

    @Test
    void everyWorkerState_mapsToExactlyOneProtoState() {
        for (TaskManager.TaskState state : TaskManager.TaskState.values()) {
            TaskState protoState = TaskStateMapper.toProtoState(state);
            assertThat(protoState)
                    .as("Worker state %s must map to exactly one Protobuf state", state)
                    .isNotNull();
            assertThat(protoState.name()).startsWith("TASK_STATE_");
        }
    }
}
