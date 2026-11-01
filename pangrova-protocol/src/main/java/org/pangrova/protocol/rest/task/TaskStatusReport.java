package org.pangrova.protocol.rest.task;

import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.client.error.ErrorDetail;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Worker-to-Coordinator task status report sent via POST /v1/task/status-report.
 *
 * <p>Identity fields are report projections of the canonical v2 task attempt.
 * {@link SubmitTaskRequest} deliberately does not mirror them in REST; the
 * Coordinator validates every projection against its retained canonical task.</p>
 *
 * <h3>Field validation strategy</h3>
 * <ul>
 *   <li><b>Mandatory</b> ({@code taskId}, {@code queryId}, {@code workerId},
 *       {@code state}): must not be {@code null} or blank. Construction with
 *       any of these missing throws {@link IllegalArgumentException}.
 *       During Jackson deserialization, a missing mandatory field results in
 *       {@code null} being passed to the constructor, which triggers the same
 *       rejection — consumers should catch the exception and log a warning
 *       before discarding the malformed report.</li>
 *   <li><b>Optional</b> ({@code stats}, {@code error}, {@code writeResult}):
 *       default to {@code null} when absent during deserialization.</li>
 *   <li><b>Primitive</b> ({@code stageId}, {@code partitionId},
 *       {@code reportSequence}, {@code timestampMs}): default to {@code 0}
 *       per Java primitive semantics when absent.</li>
 * </ul>
 *
 * @see SubmitTaskRequest
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TaskStatusReport(
    @JsonProperty("task_id") String taskId,
    @JsonProperty("query_id") String queryId,
    @JsonProperty("stage_id") int stageId,
    @JsonProperty("partition_id") int partitionId,
    @JsonProperty("task_type") String taskType,
    @JsonProperty("worker_id") String workerId,
    @JsonProperty("state") String state,
    @JsonProperty("stats") TaskStats stats,
    @JsonProperty("error") ErrorDetail error,
    @JsonProperty("write_result") WriteResultDescriptor writeResult,
    @JsonProperty("report_sequence") long reportSequence,
    @JsonProperty("timestamp_ms") long timestampMs,
    @JsonProperty("error_code") String errorCode,
    @JsonProperty("error_metadata") Map<String, String> errorMetadata,
    @JsonProperty("attempt") TaskAttemptDescriptor attempt,
    @JsonProperty("canonical_task_report") byte[] canonicalTaskReport
) {

    /** Bounded JSON envelope for a 4 MiB canonical report after Base64 encoding. */
    public static final int MAX_HTTP_BODY_BYTES = 8 * 1024 * 1024;

    /** Source-compatible constructor for callers without a Native status. */
    public TaskStatusReport(
            String taskId, String queryId, int stageId, int partitionId,
            String taskType, String workerId, String state, TaskStats stats,
            ErrorDetail error, WriteResultDescriptor writeResult,
            long reportSequence, long timestampMs, String errorCode,
            Map<String, String> errorMetadata, TaskAttemptDescriptor attempt) {
        this(taskId, queryId, stageId, partitionId, taskType, workerId, state,
                stats, error, writeResult, reportSequence, timestampMs,
                errorCode, errorMetadata, attempt, null);
    }

    /**
     * Compact constructor enforcing mandatory field contracts.
     *
     * <p>Rejects reports where any mandatory identity/state field is missing,
     * ensuring the Coordinator never processes a report it cannot correlate
     * back to a known task.</p>
     *
     * <p>errorMetadata is defensively copied to an unmodifiable map when
     * non-null, preserving immutability.</p>
     */
    public TaskStatusReport {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be null or blank");
        }
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be null or blank");
        }
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("state must not be null or blank");
        }
        if (attempt == null) {
            throw new IllegalArgumentException("attempt must not be null");
        }
        errorMetadata = errorMetadata == null ? null : Map.copyOf(errorMetadata);
        canonicalTaskReport = canonicalTaskReport == null
                ? null : canonicalTaskReport.clone();
    }

    @Override
    public byte[] canonicalTaskReport() {
        return canonicalTaskReport == null ? null : canonicalTaskReport.clone();
    }
}
