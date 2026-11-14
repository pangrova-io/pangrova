package org.pangrova.coordinator.query.task.report;

import org.pangrova.protocol.v2.TaskReport;

/** Durable sink invoked before a Native background task is acknowledged as finished. */
public interface GenerationWorkReportSink {

    /** Returns true only when the fenced report is durably committed or is an exact retry. */
    boolean commit(String tenantId, TaskReport status, byte[] originalWireBytes);

    /** Exact durable routing is metadata identity, never authorization inferred from an execution-id prefix. */
    boolean ownsExecution(String tenantId, String executionId);

    /** Fail-closed sink used when the durable metadata store is unavailable. */
    static GenerationWorkReportSink rejecting() {
        return new GenerationWorkReportSink() {
            @Override public boolean commit(String tenantId, TaskReport status, byte[] originalWireBytes) { return false; }
            @Override public boolean ownsExecution(String tenantId, String executionId) { return false; }
        };
    }
}
