package org.pangrova.worker.task;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.junit.jupiter.api.Test;

/**
 * Verifies that TaskStatusReport correctly carries NATIVE_FALLBACK_REQUIRED
 * error code and structured metadata for native connector fallback reporting.
 *
 * Validates: Requirements R22.3, R22.4
 */
class NativeFallbackErrorPropagationVerificationTest {

    @Test
    void taskStatusReportCarriesNativeFallbackErrorCode() {
        TaskStatusReport report = new TaskStatusReport(
                "task-1", "query-1", 0, 0, "SCAN", "worker-1", "FAILED",
                null, null, null, 1, System.currentTimeMillis(),
                "NATIVE_FALLBACK_REQUIRED",
                Map.of("feature_name", "avro_format",
                       "connector_type", "iceberg",
                       "split_id", "split-42"));

        assertEquals("NATIVE_FALLBACK_REQUIRED", report.errorCode());
        assertEquals("avro_format", report.errorMetadata().get("feature_name"));
        assertEquals("iceberg", report.errorMetadata().get("connector_type"));
        assertEquals("split-42", report.errorMetadata().get("split_id"));
    }

    @Test
    void taskStatusReportErrorMetadataIsImmutable() {
        Map<String, String> metadata = Map.of("feature_name", "merge_engine:unknown");
        TaskStatusReport report = new TaskStatusReport(
                "task-2", "query-2", 0, 0, "SCAN", "worker-1", "FAILED",
                null, null, null, 1, System.currentTimeMillis(),
                "NATIVE_FALLBACK_REQUIRED", metadata);

        assertThrows(UnsupportedOperationException.class, () ->
                report.errorMetadata().put("extra", "value"));
    }

    @Test
    void taskStatusReportWithoutErrorCodeIsValid() {
        TaskStatusReport report = new TaskStatusReport(
                "task-3", "query-3", 0, 0, "SCAN", "worker-1", "FINISHED",
                null, null, null, 1, System.currentTimeMillis(), null, null);

        assertNull(report.errorCode());
        assertNull(report.errorMetadata());
    }

    @Test
    void taskStatusReportWithNullErrorMetadataIsValid() {
        TaskStatusReport report = new TaskStatusReport(
                "task-4", "query-4", 0, 0, "SCAN", "worker-1", "FAILED",
                null, null, null, 1, System.currentTimeMillis(),
                "NATIVE_FALLBACK_REQUIRED", null);

        assertEquals("NATIVE_FALLBACK_REQUIRED", report.errorCode());
        assertNull(report.errorMetadata());
    }
}
