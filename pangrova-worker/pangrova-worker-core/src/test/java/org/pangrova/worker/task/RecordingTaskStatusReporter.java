package org.pangrova.worker.task;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.protocol.rest.task.TaskStatusReport;

/**
 * Real TaskStatusReporter implementation for testing.
 * Records all reported statuses for assertion.
 */
public final class RecordingTaskStatusReporter implements TaskStatusReporter {

    private final List<TaskStatusReport> reports = new CopyOnWriteArrayList<>();

    @Override
    public void report(TaskStatusReport report) {
        reports.add(report);
    }

    public List<TaskStatusReport> reports() {
        return List.copyOf(reports);
    }

    public void clear() {
        reports.clear();
    }
}
