package org.pangrova.server.coordinator.speculation;



import org.pangrova.scheduler.core.speculation.SpeculativeTaskLauncher;
import org.pangrova.scheduler.core.speculation.StragglerDetector;
import org.pangrova.scheduler.core.speculation.DuplicateTaskResolver;

import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.scheduler.api.placement.TaskDuration;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Structural placement checks and local algorithm tests for speculation components.
 * These checks do not prove that a process enables speculation or executes a real
 * duplicate task. Dedicated scheduling/report tests and Worker integration tests own
 * that evidence; absence of a direct field in QueryDispatcher is not an integration gap.
 */
class SpeculativeExecutionIntegrationStatusTest {

    // ── Component existence verification ──

    @Test
    void allSpeculationComponentsExist() {
        // All three components must be loadable
        assertThat(StragglerDetector.class).isNotNull();
        assertThat(SpeculativeTaskLauncher.class).isNotNull();
        assertThat(DuplicateTaskResolver.class).isNotNull();
    }

    @Test
    void componentsAreInCorrectPackage() {
        assertThat(StragglerDetector.class.getPackageName())
                .isEqualTo("org.pangrova.scheduler.core.speculation");
        assertThat(SpeculativeTaskLauncher.class.getPackageName())
                .isEqualTo("org.pangrova.scheduler.core.speculation");
        assertThat(DuplicateTaskResolver.class.getPackageName())
                .isEqualTo("org.pangrova.scheduler.core.speculation");
    }

    // ── Component placement boundaries (not runtime support evidence) ──

    @Test
    void stragglerDetectorNotInQueryDispatcher() {
        Set<String> fieldTypes = getFieldTypeNames(QueryDispatcher.class);
        assertThat(fieldTypes)
                .as("Straggler detection belongs to the dedicated scheduler, not QueryDispatcher")
                .doesNotContain("StragglerDetector");
    }

    @Test
    void stragglerDetectorNotInQueryDispatcherContext() {
        Set<String> componentTypes = getRecordComponentTypeNames(QueryDispatcherContext.class);
        assertThat(componentTypes)
                .as("QueryDispatcherContext does not carry the detector implementation")
                .doesNotContain("StragglerDetector");
    }

    @Test
    void speculativeTaskLauncherNotInQueryDispatcher() {
        Set<String> fieldTypes = getFieldTypeNames(QueryDispatcher.class);
        assertThat(fieldTypes)
                .as("Copy launching belongs to the dedicated scheduler, not QueryDispatcher")
                .doesNotContain("SpeculativeTaskLauncher");
    }

    @Test
    void speculativeTaskLauncherNotInQueryDispatcherContext() {
        Set<String> componentTypes = getRecordComponentTypeNames(QueryDispatcherContext.class);
        assertThat(componentTypes)
                .as("QueryDispatcherContext does not carry the launcher implementation")
                .doesNotContain("SpeculativeTaskLauncher");
    }

    @Test
    void duplicateTaskResolverWiredIntoTaskStatusReceiver() throws ClassNotFoundException {
        assertThat(getFieldTypeNames(TaskStatusReceiver.class))
                .contains("SpeculativeTaskReportCompletion");
        Set<String> fieldTypes = getFieldTypeNames(Class.forName(
                "org.pangrova.coordinator.query.task.SpeculativeTaskReportCompletion"));
        assertThat(fieldTypes)
                .as("DuplicateTaskResolver IS wired into TaskStatusReceiver (Task 24 fix)")
                .contains("DuplicateTaskResolver");
    }

    @Test
    void duplicateTaskResolverNotInCoordinatorTaskTracker() {
        Set<String> fieldTypes = getFieldTypeNames(CoordinatorTaskTracker.class);
        assertThat(fieldTypes)
                .as("Winner selection belongs to report completion, not task identity tracking")
                .doesNotContain("DuplicateTaskResolver");
    }

    @Test
    void dagDispatchEngineHasSpeculationReferences() throws ClassNotFoundException {
        // Speculation is handled by SpeculativeExecutionScheduler (periodic timer).
        // DAGDispatchContext still carries SpeculativeStageHandler for bootstrap wiring.
        Class<?> ctxClass = Class.forName(
                "org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext");
        Set<String> componentTypes = getRecordComponentTypeNames(ctxClass);
        assertThat(componentTypes)
                .as("DAGDispatchContext has SpeculativeStageHandler reference (Task 24 fix)")
                .anyMatch(name -> name.contains("Speculative"));
    }

    @Test
    void featureFlagsIncludeSpeculation() {
        Set<String> flagNames = getRecordComponentNames(
                CoordinatorBootstrapConfig.FeatureFlags.class);
        assertThat(flagNames)
                .as("FeatureFlags includes speculation toggle (SPEC-5 completed)")
                .contains("speculation");
    }

    // ── Standalone component functional verification ──

    @Test
    void stragglerDetectorFunctionalEndToEnd() {
        StragglerDetector detector = new StragglerDetector();

        // Scenario: 80% completed (fast), 20% running (slow)
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 3000, true, false),
                new TaskDuration("t2", 4000, true, false),
                new TaskDuration("t3", 3500, true, false),
                new TaskDuration("t4", 4500, true, false),
                new TaskDuration("t5", 60000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);
        assertThat(stragglers)
                .as("Straggler detection logic is functional")
                .containsExactly("t5");
    }

    @Test
    void speculativeTaskLauncherRejectsMissingDispatchWiring() {
        SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();
        assertThatThrownBy(() -> launcher.launchSpeculative(
                "original-1", "worker-A", "query-1"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }

    @Test
    void duplicateTaskResolverFunctionalEndToEnd() {
        DuplicateTaskResolver resolver = new DuplicateTaskResolver();

        // First task wins
        assertThat(resolver.onTaskComplete("query-1", "task-A", "stage-1", 0)).isTrue();
        // Second task loses
        assertThat(resolver.onTaskComplete("query-1", "task-B", "stage-1", 0)).isFalse();
        // Winner is retrievable
        assertThat(resolver.getWinner("query-1", "stage-1", 0)).hasValue("task-A");
        // Clear works
        resolver.clearQuery("query-1");
        assertThat(resolver.getWinner("query-1", "stage-1", 0)).isEmpty();
    }

    @Test
    void stragglerDetectionCannotAdvertiseUnscheduledCopy() {

        StragglerDetector detector = new StragglerDetector();
        SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();

        // Step 1: Detect stragglers
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 2000, true, false),
                new TaskDuration("t2", 2500, true, false),
                new TaskDuration("t3", 3000, true, false),
                new TaskDuration("t4", 2000, true, false),
                new TaskDuration("slow", 50000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);
        assertThat(stragglers).containsExactly("slow");

        assertThatThrownBy(() -> launcher.launchSpeculative(
                "slow", "worker-A", "query-1"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }

    // ── Helpers ──

    private static Set<String> getFieldTypeNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .map(Field::getType)
                .map(Class::getSimpleName)
                .collect(Collectors.toSet());
    }

    private static Set<String> getRecordComponentTypeNames(Class<?> clazz) {
        RecordComponent[] components = clazz.getRecordComponents();
        if (components == null) {
            return Set.of();
        }
        return Arrays.stream(components)
                .map(RecordComponent::getType)
                .map(Class::getSimpleName)
                .collect(Collectors.toSet());
    }

    private static Set<String> getRecordComponentNames(Class<?> clazz) {
        RecordComponent[] components = clazz.getRecordComponents();
        if (components == null) {
            return Set.of();
        }
        return Arrays.stream(components)
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());
    }
}
