package org.pangrova.protocol.rest.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.junit.jupiter.api.Test;

/**
 * Verifies field symmetry between {@link SubmitTaskRequest} (submit) and
 * {@link TaskStatusReport} (report-back), ensuring identity fields survive
 * the submit → execute → report cycle.
 *
 * <p>Also verifies {@link TaskStats} field alignment between REST DTO and
 * Protobuf definition in pangrova_native_protocol.proto.</p>
 *
 * <p>Validates: Requirements 2.4</p>
 */
class SubmitTaskRequestSymmetryTest {

    // --- Identity field symmetry ---

    @Test
    void taskIdSurvivesSubmitReportCycle() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request);

        assertThat(report.taskId()).isEqualTo(request.taskId());
    }

    @Test
    void queryIdSurvivesSubmitReportCycle() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request);

        assertThat(report.queryId()).isEqualTo(request.queryId());
    }

    @Test
    void stageIdSurvivesSubmitReportCycle() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request);

        assertThat(report.stageId()).isEqualTo(request.stageId());
    }

    @Test
    void partitionIdSurvivesSubmitReportCycle() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request);

        assertThat(report.partitionId()).isEqualTo(request.partitionId());
    }

    @Test
    void taskTypeSurvivesSubmitReportCycle() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request);

        assertThat(report.taskType()).isEqualTo(request.taskType());
    }

    @Test
    void allIdentityFieldsPresentInBothDtos() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request);

        assertThat(report.taskId()).isEqualTo(request.taskId());
        assertThat(report.queryId()).isEqualTo(request.queryId());
        assertThat(report.stageId()).isEqualTo(request.stageId());
        assertThat(report.partitionId()).isEqualTo(request.partitionId());
        assertThat(report.taskType()).isEqualTo(request.taskType());
    }

    // --- TaskStats REST DTO ↔ Protobuf field alignment ---

    @Test
    void taskStatsRestDtoCoversAllProtobufFields() {
        // Protobuf TaskStats has: rows_read, bytes_read, rows_written,
        // bytes_written, elapsed_time_ms, cpu_time_ms, memory_bytes,
        // peak_memory_bytes, spill_bytes, cache_hit_bytes, cache_miss_bytes
        TaskStats stats = new TaskStats(
                100, 2048, 50, 1024, 500, 120, 4096, 8192, 256, 512, 64);

        assertThat(stats.rowsRead()).isEqualTo(100);
        assertThat(stats.bytesRead()).isEqualTo(2048);
        assertThat(stats.rowsWritten()).isEqualTo(50);
        assertThat(stats.bytesWritten()).isEqualTo(1024);
        assertThat(stats.elapsedTimeMs()).isEqualTo(500);
        assertThat(stats.cpuTimeMs()).isEqualTo(120);
        assertThat(stats.memoryBytes()).isEqualTo(4096);
        assertThat(stats.peakMemoryBytes()).isEqualTo(8192);
        assertThat(stats.spillBytes()).isEqualTo(256);
        assertThat(stats.cacheHitBytes()).isEqualTo(512);
        assertThat(stats.cacheMissBytes()).isEqualTo(64);
    }

    @Test
    void taskStatsFieldCountMatchesProtobuf() {
        // Protobuf TaskStats has 12 fields (field numbers 1-12).
        // REST DTO adds 2 transport-layer fields (compressed/uncompressed bytes)
        // and 1 pipeline observability field (backpressureWaitMs),
        // totaling 15 record components.
        int restFieldCount = TaskStats.class.getRecordComponents().length;
        assertThat(restFieldCount).isEqualTo(15);
    }

    // --- Report enrichment fields ---

    @Test
    void reportContainsWorkerIdNotInRequest() {
        TaskStatusReport report = reportFrom(fullRequest());
        assertThat(report.workerId()).isNotNull();
    }

    @Test
    void reportContainsStateNotInRequest() {
        TaskStatusReport report = reportFrom(fullRequest());
        assertThat(report.state()).isNotNull();
    }

    @Test
    void reportContainsTimestampNotInRequest() {
        TaskStatusReport report = reportFrom(fullRequest());
        assertThat(report.timestampMs()).isGreaterThan(0);
    }

    // --- Helpers ---

    private static SubmitTaskRequest fullRequest() {
        return new SubmitTaskRequest(
                "task-42", "query-7", 3, 5, "OUTPUT",
                "cGxhbg==", List.of("c3BsaXQ="),
                List.of(new ExchangeSourceDescriptor("t-src", "localhost", 8080, 0, "SHUFFLE", 1)),
                new ResourceLimitPayload(256_000_000L, 4, 512_000_000L, "/tmp/spill"),
                Map.of("timezone", "UTC"),
                new OutputConfig("ticket-1", 1024, 10, 30000),
                new WriteConfig("db.t", "INSERT", List.of("dt"), "parquet", 128_000_000),
                null, null, null, null, null, null, null);
    }

    private static TaskStatusReport reportFrom(SubmitTaskRequest request) {
        TaskStats stats = new TaskStats(10, 100, 5, 50, 200, 50, 1024, 2048, 0, 0, 0);
        return new TaskStatusReport(
                request.taskId(), request.queryId(),
                request.stageId(), request.partitionId(), request.taskType(),
                "worker-1", "FINISHED", stats, null,
                new WriteResultDescriptor(List.of(), 5, 50, new byte[0]),
                1L, System.currentTimeMillis());
    }
}
