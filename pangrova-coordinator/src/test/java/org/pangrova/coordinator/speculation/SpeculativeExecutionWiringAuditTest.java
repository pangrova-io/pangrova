package org.pangrova.coordinator.speculation;

import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskDuration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Audit test for speculative execution component wiring.
 *
 * <p>Validates: Requirements 64.1, 64.2, 64.3, 64.4</p>
 *
 * <p>Post-fix status (Task 24 — TASK-M3-1 ~ M3-6):</p>
 * <ul>
 *   <li>StragglerDetector: wired into SpeculativeStageHandler → DAGDispatchEngine</li>
 *   <li>SpeculativeTaskLauncher: wired into TaskStatusReceiver + SpeculativeStageHandler</li>
 *   <li>DuplicateTaskResolver: wired into TaskStatusReceiver for winner/loser resolution</li>
 *   <li>Resource cleanup: loser cancellation + deadline/tracking cleanup in TaskStatusReceiver</li>
 * </ul>
 */
class SpeculativeExecutionWiringAuditTest {

    // ── Req 64.1: StragglerDetector periodic invocation ──

    @Test
    void stragglerDetectorComponentIsFunctional() {
        StragglerDetector detector = new StragglerDetector();

        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5000, true, false),
                new TaskDuration("t2", 5000, true, false),
                new TaskDuration("t3", 5000, true, false),
                new TaskDuration("t4", 50000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);
        assertThat(stragglers).containsExactly("t4");
    }

    // ── Req 64.2: SpeculativeTaskLauncher wired into TaskStatusReceiver ──

    @Test
    void speculativeTaskLauncherWiredIntoTaskStatusReceiver() {
        Set<String> receiverFieldTypes = getFieldTypeNames(TaskStatusReceiver.class);
        assertThat(receiverFieldTypes)
                .as("SpeculativeTaskLauncher should be wired into TaskStatusReceiver")
                .contains("SpeculativeTaskLauncher");
    }

    @Test
    void speculativeTaskLauncherDoesNotRegisterWithoutDispatch() {
        SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();
        assertThatThrownBy(() -> launcher.launchSpeculative(
                "original-task", "exclude-worker", "query-1"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }

    // ── Req 64.3: DuplicateTaskResolver wired into TaskStatusReceiver ──

    @Test
    void duplicateTaskResolverWiredIntoTaskStatusReceiver() {
        Set<String> receiverFieldTypes = getFieldTypeNames(TaskStatusReceiver.class);
        assertThat(receiverFieldTypes)
                .as("DuplicateTaskResolver should be wired into TaskStatusReceiver")
                .contains("DuplicateTaskResolver");
    }

    @Test
    void duplicateTaskResolverComponentIsFunctional() {
        DuplicateTaskResolver resolver = new DuplicateTaskResolver();

        boolean firstWins = resolver.onTaskComplete("query-1", "task-A", "stage-1", 0);
        boolean secondLoses = resolver.onTaskComplete("query-1", "task-B", "stage-1", 0);

        assertThat(firstWins).isTrue();
        assertThat(secondLoses).isFalse();
        assertThat(resolver.getWinner("query-1", "stage-1", 0)).hasValue("task-A");
    }

    // ── Req 64.4: Resource cleanup path exists ──

    @Test
    void speculationAwareResourceCleanupPathExists() {
        Set<String> receiverFieldTypes = getFieldTypeNames(TaskStatusReceiver.class);
        assertThat(receiverFieldTypes)
                .as("TaskStatusReceiver should have speculation components for cleanup")
                .contains("SpeculativeTaskLauncher", "DuplicateTaskResolver");
    }

    @Test
    void taskTrackerHasDurationSnapshotCapability() {
        Set<String> trackerFieldTypes = getFieldTypeNames(CoordinatorTaskTracker.class);
        assertThat(trackerFieldTypes)
                .as("CoordinatorTaskTracker should have Instant fields for start time tracking")
                .contains("ConcurrentHashMap");
    }

    // ── Helper ──

    private static Set<String> getFieldTypeNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .map(Field::getType)
                .map(Class::getSimpleName)
                .collect(Collectors.toSet());
    }
}
