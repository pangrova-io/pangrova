package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.worker.jni.NativeRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real generated report decoding; this is not a Native task execution test. */
class NativeTaskStatusContractTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void admittedNonterminalStatesKeepPolling(int state) {
        assertThat(NativeTaskExecutor.evaluateNativeStatus(report(state), 41)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 6})
    void successfulAndCancelledTerminalStatesStopPolling(int state) {
        assertThat(NativeTaskExecutor.evaluateNativeStatus(report(state), 41)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 7, Integer.MAX_VALUE})
    void unknownAndUnspecifiedStatesNeverClaimCompletion(int state) {
        assertThatThrownBy(() -> NativeTaskExecutor.evaluateNativeStatus(report(state), 41))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("invalid task state");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void missingReportsAreProtocolFailures(byte[] report) {
        assertThatThrownBy(() -> NativeTaskExecutor.evaluateNativeStatus(report, 41))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("empty v2 TaskReport");
    }

    @Test
    void malformedReportsAreProtocolFailures() {
        assertThatThrownBy(() -> NativeTaskExecutor.evaluateNativeStatus(new byte[] {10}, 41))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("malformed v2 TaskReport");
    }

    @Test
    void failedReportPreservesTheActualError() {
        byte[] report = TaskReport.newBuilder().setStateValue(5)
                .setError(ExecutionError.newBuilder().setMessage("provider response exceeds grant"))
                .build().toByteArray();
        assertThatThrownBy(() -> NativeTaskExecutor.evaluateNativeStatus(report, 41))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("provider response exceeds grant");
    }

    @Test
    void failedReportWithoutDetailsStillFails() {
        assertThatThrownBy(() -> NativeTaskExecutor.evaluateNativeStatus(report(5), 41))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("Native task failed");
    }

    private static byte[] report(int state) {
        // Proto3 omits the default enum value. Keep the report nonempty so
        // UNSPECIFIED exercises state validation rather than missing bytes.
        return TaskReport.newBuilder().setReportOriginValue(1)
                .setStateValue(state).build().toByteArray();
    }
}
