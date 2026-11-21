package org.pangrova.worker.task.report;

import org.pangrova.protocol.rest.task.TaskStatusReport;

/**
 * Reports task execution status to the Coordinator.
 *
 * <p>Implementation: {@link RemoteTaskStatusReporter} — sends a real HTTP POST
 * to the Coordinator's {@code /v1/task/status-report} endpoint via JDK HttpClient.</p>
 */
public interface TaskStatusReporter {

    void report(TaskStatusReport report);
}
