package org.pangrova.worker.jni;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies NativeExecutionException carries the native error code (R7.9).
 */
class NativeExecutionExceptionTest {

    @Test
    void carriesNativeErrorCode() {
        NativeExecutionException ex = new NativeExecutionException(-42);
        assertThat(ex.nativeErrorCode()).isEqualTo(-42);
        assertThat(ex.getMessage()).contains("-42");
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.NATIVE_JNI_EXCEPTION);
    }

    @Test
    void carriesCustomMessage() {
        NativeExecutionException ex = new NativeExecutionException(-1, "OOM in native");
        assertThat(ex.nativeErrorCode()).isEqualTo(-1);
        assertThat(ex.getMessage()).isEqualTo("OOM in native");
    }

    @Test
    void extendsPangrovaException() {
        assertThat(PangrovaException.class).isAssignableFrom(NativeExecutionException.class);
    }

    @Test
    void distinctFromNativeRuntimeException() {
        // NativeExecutionException is for error codes from batch-pull operations,
        // NativeRuntimeException is for JNI-level failures. They are sibling classes.
        assertThat(NativeRuntimeException.class.isAssignableFrom(NativeExecutionException.class)).isFalse();
        assertThat(NativeExecutionException.class.isAssignableFrom(NativeRuntimeException.class)).isFalse();
    }
}
