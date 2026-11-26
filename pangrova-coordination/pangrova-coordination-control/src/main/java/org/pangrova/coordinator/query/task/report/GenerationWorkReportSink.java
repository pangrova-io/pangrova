package org.pangrova.coordinator.query.task.report;

import org.pangrova.protocol.rest.task.TaskReportEvidence;

/** Durable sink invoked before a Native background task is acknowledged as finished. */
@FunctionalInterface
public interface GenerationWorkReportSink {

    /** Returns true only when the fenced report is durably committed or is an exact retry. */
    boolean commit(String tenantId, TaskReportEvidence evidence);

    /** Fail-closed sink used when the durable metadata store is unavailable. */
    static GenerationWorkReportSink rejecting() {
        return (tenantId, evidence) -> false;
    }
}
