package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.worker.exception.NativeRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Canonical wire state and error decoding at the Native boundary. */
class NativeTaskStatusContractTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6})
    void decodesTheCanonicalTaskState(int state) {
        assertThat(NativeTaskExecutor.requireNativeTaskState(state)).isEqualTo(TaskState.forNumber(state));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 7, Integer.MAX_VALUE})
    void unknownAndUnspecifiedStatesNeverClaimCompletion(int state) {
        assertThatThrownBy(() -> NativeTaskExecutor.requireNativeTaskState(state))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("invalid task state");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void missingReportsAreProtocolFailures(byte[] report) {
        assertThatThrownBy(() -> NativeTaskExecutor.nativeTaskFailure(report))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("empty v2 TaskReport");
    }

    @Test
    void malformedReportsAreProtocolFailures() {
        assertThatThrownBy(() -> NativeTaskExecutor.nativeTaskFailure(new byte[] {10}))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("malformed v2 TaskReport");
    }

    @Test
    void failedReportPreservesTheActualError() {
        byte[] report = TaskReport.newBuilder().setStateValue(5)
                .setError(ExecutionError.newBuilder().setMessage("provider response exceeds grant"))
                .build().toByteArray();
        assertThat(NativeTaskExecutor.nativeTaskFailure(report))
                .hasMessageContaining("provider response exceeds grant");
    }

    @Test
    void failedReportWithoutDetailsStillFails() {
        assertThat(NativeTaskExecutor.nativeTaskFailure(report(TaskState.TASK_STATE_FAILED)))
                .hasMessageContaining("Native task failed");
    }

    @Test
    void failureProbeRequiresAFailedReport() {
        assertThatThrownBy(() -> NativeTaskExecutor.nativeTaskFailure(report(TaskState.TASK_STATE_FINISHED)))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("unexpected task state");
    }

    private static byte[] report(TaskState state) {
        return TaskReport.newBuilder().setState(state).build().toByteArray();
    }
}
