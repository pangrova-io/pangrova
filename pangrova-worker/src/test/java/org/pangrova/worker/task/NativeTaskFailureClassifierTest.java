package org.pangrova.worker.task;

import org.pangrova.worker.enums.NativeTaskFailureCategory;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies NativeTaskFailureClassifier correctly routes exceptions
 * to NATIVE_CRASHED, NATIVE_HUNG, or TRANSIENT categories.
 */
class NativeTaskFailureClassifierTest {

    @Test
    void sigsegvException_classifiedAsCrashed() {
        Exception ex = new RuntimeException("SIGSEGV in native task execution");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_CRASHED);
    }

    @Test
    void signalSixMessage_classifiedAsCrashed() {
        Exception ex = new RuntimeException("signal 6 (SIGABRT) received");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_CRASHED);
    }

    @Test
    void segmentationFaultMessage_classifiedAsCrashed() {
        Exception ex = new RuntimeException("Segmentation fault in operator");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_CRASHED);
    }

    @Test
    void nativeCrashMessage_classifiedAsCrashed() {
        Exception ex = new RuntimeException("native crash detected");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_CRASHED);
    }

    @Test
    void crashInCauseChain_classifiedAsCrashed() {
        Exception root = new RuntimeException("SIGSEGV at 0x0");
        Exception wrapper = new RuntimeException("JNI call failed", root);
        assertThat(NativeTaskFailureClassifier.classify(wrapper))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_CRASHED);
    }

    @Test
    void timeoutException_classifiedAsHung() {
        Exception ex = new TimeoutException("nativeGetTaskStatus timed out");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_HUNG);
    }

    @Test
    void timeoutInCauseChain_classifiedAsHung() {
        Exception root = new TimeoutException("RPC deadline exceeded");
        Exception wrapper = new RuntimeException("isFinished failed", root);
        assertThat(NativeTaskFailureClassifier.classify(wrapper))
                .isEqualTo(NativeTaskFailureCategory.NATIVE_HUNG);
    }

    @Test
    void ioException_classifiedAsTransient() {
        Exception ex = new IOException("Connection reset by peer");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.TRANSIENT);
    }

    @Test
    void genericRuntimeException_classifiedAsTransient() {
        Exception ex = new RuntimeException("Unexpected error");
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.TRANSIENT);
    }

    @Test
    void nullMessage_classifiedAsTransient() {
        Exception ex = new RuntimeException((String) null);
        assertThat(NativeTaskFailureClassifier.classify(ex))
                .isEqualTo(NativeTaskFailureCategory.TRANSIENT);
    }
}
