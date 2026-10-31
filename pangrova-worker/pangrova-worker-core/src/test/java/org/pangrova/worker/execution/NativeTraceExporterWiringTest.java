package org.pangrova.worker.execution;

import org.pangrova.observability.sdk.tracing.adapter.nativebridge.NativeTraceExporter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates NativeTraceExporter returns well-formed trace/span IDs.
 * No active OTel span → all-zeros (valid sentinel).
 * Real JNI calls are not tested here — native library not loaded in unit tests.
 */
class NativeTraceExporterWiringTest {

    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";
    private static final String INVALID_SPAN_ID = "0000000000000000";

    @Test
    void currentTraceIdReturnsAllZerosWhenNoActiveSpan() {
        String traceId = NativeTraceExporter.currentTraceId();
        assertNotNull(traceId);
        assertEquals(INVALID_TRACE_ID, traceId);
    }

    @Test
    void currentSpanIdReturnsAllZerosWhenNoActiveSpan() {
        String spanId = NativeTraceExporter.currentSpanId();
        assertNotNull(spanId);
        assertEquals(INVALID_SPAN_ID, spanId);
    }

    @Test
    void traceIdFormatIs32HexChars() {
        String traceId = NativeTraceExporter.currentTraceId();
        assertEquals(32, traceId.length());
        assertTrue(traceId.matches("[0-9a-f]{32}"), "traceId must be 32 lowercase hex chars");
    }

    @Test
    void spanIdFormatIs16HexChars() {
        String spanId = NativeTraceExporter.currentSpanId();
        assertEquals(16, spanId.length());
        assertTrue(spanId.matches("[0-9a-f]{16}"), "spanId must be 16 lowercase hex chars");
    }
}
