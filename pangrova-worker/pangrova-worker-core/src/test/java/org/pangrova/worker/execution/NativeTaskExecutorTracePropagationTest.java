package org.pangrova.worker.execution;

import org.pangrova.observability.sdk.tracing.adapter.nativebridge.NativeTraceExporter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies NativeTraceExporter provides valid trace IDs for JNI propagation.
 * Full end-to-end trace propagation requires native library; this test
 * validates the Java-side contract that NativeTaskExecutor relies on.
 */
class NativeTaskExecutorTracePropagationTest {

    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";
    private static final String INVALID_SPAN_ID = "0000000000000000";

    @Test
    void currentTraceIdReturnsValidHexStringWhenNoActiveSpan() {
        String traceId = NativeTraceExporter.currentTraceId();

        assertThat(traceId).isNotNull();
        assertThat(traceId).hasSize(32);
        assertThat(traceId).isEqualTo(INVALID_TRACE_ID);
    }

    @Test
    void currentSpanIdReturnsValidHexStringWhenNoActiveSpan() {
        String spanId = NativeTraceExporter.currentSpanId();

        assertThat(spanId).isNotNull();
        assertThat(spanId).hasSize(16);
        assertThat(spanId).isEqualTo(INVALID_SPAN_ID);
    }

    @Test
    void traceIdFormatIsConsistentAcrossCalls() {
        String first = NativeTraceExporter.currentTraceId();
        String second = NativeTraceExporter.currentTraceId();

        assertThat(first).isEqualTo(second);
        assertThat(first).matches("[0-9a-f]{32}");
    }
}
