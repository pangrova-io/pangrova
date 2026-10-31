package org.pangrova.scheduler.api.placement;

/**
 * Duration snapshot for a single task, used by straggler detection.
 *
 * @param taskId             unique task identifier
 * @param durationMs         elapsed time in milliseconds
 * @param completed          whether the task has finished
 * @param hasSpeculativeCopy whether a speculative copy already exists
 * @param progressPercent    task progress as a percentage (0.0 to 1.0), or -1.0 if unknown
 * @param inputBytes         input data size in bytes assigned to this task, or -1 if unknown
 */
public record TaskDuration(
        String taskId,
        long durationMs,
        boolean completed,
        boolean hasSpeculativeCopy,
        double progressPercent,
        long inputBytes) {

    /**
     * Backward-compatible constructor without progress/input tracking.
     */
    public TaskDuration(String taskId, long durationMs, boolean completed, boolean hasSpeculativeCopy) {
        this(taskId, durationMs, completed, hasSpeculativeCopy, -1.0, -1L);
    }

    /**
     * Returns the progress rate (progress per second), or -1.0 if progress is unknown.
     */
    public double progressRatePerSecond() {
        if (progressPercent < 0.0 || durationMs <= 0) {
            return -1.0;
        }
        return progressPercent / (durationMs / 1000.0);
    }
}
