package org.pangrova.coordinator.property.speculation;

import net.jqwik.api.*;
import org.pangrova.coordinator.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.speculation.StragglerDetector;
import org.pangrova.coordinator.task.TaskDuration;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property 25: Speculative execution correctness.
 *
 * <p><b>Validates: Requirements 10.10</b></p>
 *
 * <p>For any task execution time distribution, StragglerDetector correctly
 * identifies slow tasks and SpeculativeTaskLauncher manages speculative copies.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 25: Speculative execution correctness")
class SpeculativeExecutionPropertyTest {

    private final StragglerDetector detector = new StragglerDetector();
    private final SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();

    @Property(tries = 10)
    void stragglerDetectionRespectsCompletionThreshold(
            @ForAll("taskCounts") int totalTasks,
            @ForAll("completionRatios") double completionRatio) {

        int completedCount = (int) (totalTasks * completionRatio);
        List<TaskDuration> tasks = buildTasks(
                totalTasks, completedCount, 5_000, 50_000);

        List<String> stragglers = detector.detectStragglers(tasks);

        if (completionRatio < 0.75) {
            assertThat(stragglers)
                    .as("Below 75%% completion, no stragglers should be detected")
                    .isEmpty();
        }
    }

    @Property(tries = 10)
    void stragglerCountCappedAtTenPercent(
            @ForAll("largeCounts") int totalTasks) {

        // 80% completed with fast duration, 20% running with slow duration
        int completedCount = (int) (totalTasks * 0.8);
        List<TaskDuration> tasks = buildTasks(
                totalTasks, completedCount, 1_000, 100_000);

        List<String> stragglers = detector.detectStragglers(tasks);

        int maxExpected = Math.max(1, (int) (totalTasks * 0.10));
        assertThat(stragglers.size())
                .as("Straggler count must be capped at 10%% of total")
                .isLessThanOrEqualTo(maxExpected);
    }

    @Property(tries = 10)
    void speculativeLaunchWithoutDispatchCannotCreatePhantomPair(
            @ForAll("taskIds") String originalTaskId) {
        assertThatThrownBy(() -> launcher.launchSpeculative(
                originalTaskId, "exclude-worker", "query-1"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }

    @Property(tries = 10)
    void completedTasksAreNeverStragglers(
            @ForAll("taskCounts") int totalTasks) {

        int completedCount = (int) (totalTasks * 0.9);
        List<TaskDuration> tasks = buildTasks(
                totalTasks, completedCount, 5_000, 50_000);

        List<String> stragglers = detector.detectStragglers(tasks);

        List<String> completedIds = tasks.stream()
                .filter(TaskDuration::completed)
                .map(TaskDuration::taskId)
                .toList();

        assertThat(stragglers)
                .as("Completed tasks must never be identified as stragglers")
                .doesNotContainAnyElementsOf(completedIds);
    }

    @Property(tries = 10)
    void tasksWithSpeculativeCopiesAreExcluded(
            @ForAll("taskCounts") int totalTasks) {

        int completedCount = (int) (totalTasks * 0.85);
        List<TaskDuration> tasks = new ArrayList<>();

        // Completed tasks
        for (int i = 0; i < completedCount; i++) {
            tasks.add(new TaskDuration(
                    "task-" + i, 5_000, true, false));
        }
        // Running tasks with speculative copies already
        for (int i = completedCount; i < totalTasks; i++) {
            tasks.add(new TaskDuration(
                    "task-" + i, 100_000, false, true));
        }

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers)
                .as("Tasks with existing speculative copies must be excluded")
                .isEmpty();
    }

    @Provide
    Arbitrary<Integer> taskCounts() {
        return Arbitraries.integers().between(10, 100);
    }

    @Provide
    Arbitrary<Integer> largeCounts() {
        return Arbitraries.integers().between(20, 200);
    }

    @Provide
    Arbitrary<Double> completionRatios() {
        return Arbitraries.doubles().between(0.0, 1.0);
    }

    @Provide
    Arbitrary<String> taskIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "task-" + s);
    }

    private List<TaskDuration> buildTasks(
            int total, int completed, long fastMs, long slowMs) {
        List<TaskDuration> tasks = new ArrayList<>();
        for (int i = 0; i < completed; i++) {
            tasks.add(new TaskDuration(
                    "task-" + i, fastMs, true, false));
        }
        for (int i = completed; i < total; i++) {
            tasks.add(new TaskDuration(
                    "task-" + i, slowMs, false, false));
        }
        return tasks;
    }
}
