package org.pangrova.coordinator.speculation;

import org.pangrova.coordinator.task.TaskDuration;

import java.util.List;

/**
 * Detects straggler tasks eligible for speculative re-execution using multiple heuristics:
 * <ul>
 *   <li>Elapsed-time-based: task duration exceeds median * multiplier</li>
 *   <li>Progress-rate-based: task progress rate is significantly below peer average</li>
 *   <li>Data-skew-based: task input size is disproportionately large vs peers</li>
 * </ul>
 *
 * <p>Detection requires 75% stage completion, 10s minimum duration, and caps speculative
 * launches at 10% of total tasks (minimum 1).</p>
 */
public class StragglerDetector {

    private static final double COMPLETION_THRESHOLD = 0.75;
    private static final long MIN_DURATION_MS = 10_000;
    private static final double MAX_SPECULATIVE_RATIO = 0.10;
    private static final double PROGRESS_RATE_SLOW_FACTOR = 0.5;

    private final SpeculationConfig config;

    /**
     * Creates a detector with default configuration (median multiplier 1.5, all heuristics enabled).
     */
    public StragglerDetector() {
        this(new SpeculationConfig(true, SpeculationConfig.DEFAULT_DETECTION_INTERVAL_MS));
    }

    /**
     * Creates a detector with the given configuration.
     */
    public StragglerDetector(SpeculationConfig config) {
        this.config = config;
    }

    /**
     * Returns task IDs that are stragglers eligible for speculative launch.
     * Uses elapsed-time detection with the default median multiplier.
     */
    public List<String> detectStragglers(List<TaskDuration> tasks) {
        return detectStragglers(tasks, null);
    }

    /**
     * Returns task IDs that are stragglers eligible for speculative launch.
     * Uses the median multiplier configured for the given query type.
     *
     * @param tasks     task duration snapshots for the stage
     * @param queryType the query type for threshold lookup (nullable)
     */
    public List<String> detectStragglers(List<TaskDuration> tasks, String queryType) {
        if (tasks.isEmpty()) {
            return List.of();
        }
        if (tasks.size() == 1 && config.singleTaskDurationThresholdMs() > 0) {
            TaskDuration task = tasks.get(0);
            return !task.completed() && !task.hasSpeculativeCopy()
                    && task.durationMs() > Math.max(MIN_DURATION_MS,
                            config.singleTaskDurationThresholdMs())
                    ? List.of(task.taskId()) : List.of();
        }
        if (!hasEnoughCompletion(tasks)) {
            return List.of();
        }
        int maxSpeculative = Math.max(1, (int) (tasks.size() * MAX_SPECULATIVE_RATIO));
        double medianMultiplier = config.medianMultiplierForQueryType(queryType);
        double medianDuration = computeMedianCompleted(tasks);
        double elapsedThreshold = medianDuration * medianMultiplier;

        return tasks.stream()
                .filter(t -> !t.completed())
                .filter(t -> !t.hasSpeculativeCopy())
                .filter(t -> t.durationMs() > MIN_DURATION_MS)
                .filter(t -> isStraggler(t, tasks, elapsedThreshold))
                .map(TaskDuration::taskId)
                .limit(maxSpeculative)
                .toList();
    }

    private boolean isStraggler(TaskDuration task, List<TaskDuration> allTasks, double elapsedThreshold) {
        if (task.durationMs() > elapsedThreshold) {
            return true;
        }
        if (config.progressRateEnabled() && isProgressRateStraggler(task, allTasks)) {
            return true;
        }
        return config.dataSkewEnabled() && isDataSkewStraggler(task, allTasks);
    }

    /**
     * A task is a progress-rate straggler if its progress rate is less than half
     * the average progress rate of completed tasks.
     */
    private boolean isProgressRateStraggler(TaskDuration task, List<TaskDuration> allTasks) {
        double taskRate = task.progressRatePerSecond();
        if (taskRate < 0.0) {
            return false;
        }
        double avgCompletedRate = allTasks.stream()
                .filter(TaskDuration::completed)
                .mapToDouble(TaskDuration::progressRatePerSecond)
                .filter(r -> r >= 0.0)
                .average()
                .orElse(-1.0);
        if (avgCompletedRate <= 0.0) {
            return false;
        }
        return taskRate < avgCompletedRate * PROGRESS_RATE_SLOW_FACTOR;
    }

    /**
     * A task is a data-skew straggler if its input size exceeds the median input size
     * by the configured skew multiplier.
     */
    private boolean isDataSkewStraggler(TaskDuration task, List<TaskDuration> allTasks) {
        long taskInput = task.inputBytes();
        if (taskInput <= 0) {
            return false;
        }
        List<Long> inputSizes = allTasks.stream()
                .mapToLong(TaskDuration::inputBytes)
                .filter(b -> b > 0)
                .sorted()
                .boxed()
                .toList();
        if (inputSizes.isEmpty()) {
            return false;
        }
        double medianInput = computeMedian(inputSizes);
        if (medianInput <= 0.0) {
            return false;
        }
        return taskInput > medianInput * config.dataSkewMultiplier();
    }

    private boolean hasEnoughCompletion(List<TaskDuration> tasks) {
        long completed = tasks.stream().filter(TaskDuration::completed).count();
        return (double) completed / tasks.size() >= COMPLETION_THRESHOLD;
    }

    private double computeMedianCompleted(List<TaskDuration> tasks) {
        List<Long> durations = tasks.stream()
                .filter(TaskDuration::completed)
                .map(TaskDuration::durationMs)
                .sorted()
                .toList();
        if (durations.isEmpty()) {
            return 0.0;
        }
        return computeMedian(durations);
    }

    private static double computeMedian(List<Long> sorted) {
        int mid = sorted.size() / 2;
        return sorted.size() % 2 == 0
                ? (sorted.get(mid - 1) + sorted.get(mid)) / 2.0
                : sorted.get(mid);
    }
}
