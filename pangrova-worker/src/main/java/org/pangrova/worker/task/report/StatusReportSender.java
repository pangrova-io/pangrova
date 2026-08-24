package org.pangrova.worker.task.report;

import org.pangrova.protocol.rest.task.TaskStatusReport;

/**
 * Functional interface for sending a task status report to the Coordinator.
 *
 * <p>Implementations perform the actual HTTP POST. The retry buffer delegates
 * to this interface so that transport concerns remain decoupled from retry logic.
 *
 * @see TaskStatusRetryBuffer
 */
@FunctionalInterface
public interface StatusReportSender {

    /**
     * Sends the given report to the Coordinator.
     *
     * @param report the status report to send
     * @return {@code true} if the Coordinator accepted the report, {@code false} otherwise
     */
    boolean send(TaskStatusReport report);
}
