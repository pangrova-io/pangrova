package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.failure.NoAvailableWorkerException;
import org.pangrova.scheduler.core.failure.CircuitBreakerOpenException;
import org.pangrova.scheduler.core.failure.TaskDispatchException;
import org.pangrova.scheduler.core.failure.ErrorCategory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.pangrova.scheduler.core.admission.FairScheduler;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Audits the scheduler package abstraction layers and package structure.
 *
 * <p>Validates: Requirements 5.5</p>
 *
 * <p>Verifies the scheduler hierarchy:
 * <ul>
 *   <li>{@code scheduler/} — HTTP dispatch adapter and worker control client</li>
 *   <li>{@code scheduler/strategy/} — must stay empty; strategies live in pangrova-scheduler</li>
 * </ul>
 */
class SchedulerAbstractionLayerTest {

    private static final Path SCHEDULER_PKG = resolveSchedulerPath();
    private static final Path STRATEGY_PKG = SCHEDULER_PKG.resolve("strategy");

    private static Path resolveSchedulerPath() {
        // Try workspace-relative path first, then fall back to module-relative
        Path workspaceRelative = Path.of("pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/scheduler");
        if (Files.exists(workspaceRelative)) {
            return workspaceRelative;
        }
        return Path.of("src/main/java/org/pangrova/coordinator/scheduler");
    }

    // -- Package structure tests --

    @Test
    void schedulerRootPackage_containsOnlyCoreFiles() throws IOException {
        List<String> rootFiles = listJavaFileNames(SCHEDULER_PKG);

        Set<String> expectedCore = Set.of(
                "TaskDispatcher.java",
                "RemoteTaskDispatcher.java",
                "SubmitTaskRequestConverter.java",
                "SchedulerCancelResult.java",
                "WorkerTaskControlClient.java",
                "package-info.java");

        assertThat(rootFiles)
                .as("scheduler/ root should contain only core dispatch/scheduling files")
                .containsExactlyInAnyOrderElementsOf(expectedCore);
    }

    @Test
    void dispatchFailureContractIsOwnedByScheduler() throws IOException {
        assertThat(ErrorCategory.class.getPackageName()).isEqualTo("org.pangrova.scheduler.core.failure");
        assertThat(listJavaFileNames(SCHEDULER_PKG.resolve("failure"))).isEmpty();
    }

    @Test
    void strategySubPackage_containsOnlyStrategyFiles() throws IOException {
        List<String> strategyFiles = listJavaFileNames(STRATEGY_PKG);

        // FairScheduler moved to pangrova-scheduler (core.admission); the coordinator
        // no longer hosts pluggable scheduling strategies.
        assertThat(strategyFiles)
                .as("coordinator scheduler/strategy/ should be empty after the scheduler extraction")
                .isEmpty();
    }

    @Test
    void schedulerRootPackage_fileCountWithinHealthyThreshold() throws IOException {
        long count = listJavaFileNames(SCHEDULER_PKG).size();
        assertThat(count)
                .as("scheduler/ root should have ≤16 files (healthy threshold)")
                .isLessThanOrEqualTo(16);
    }

    @Test
    void strategySubPackage_fileCountWithinHealthyThreshold() throws IOException {
        long count = listJavaFileNames(STRATEGY_PKG).size();
        assertThat(count)
                .as("scheduler/strategy/ should have ≤8 files (healthy threshold)")
                .isLessThanOrEqualTo(8);
    }

    // -- Abstraction hierarchy tests --

    @Test
    void taskDispatcher_isInterface() {
        assertThat(TaskDispatcher.class.isInterface())
                .as("TaskDispatcher should be an interface (dispatch contract)")
                .isTrue();
    }

    @Test
    void remoteTaskDispatcher_implementsTaskDispatcher() {
        assertThat(TaskDispatcher.class.isAssignableFrom(RemoteTaskDispatcher.class))
                .as("RemoteTaskDispatcher should implement TaskDispatcher")
                .isTrue();
    }

    @Test
    void strategyClasses_areIndependentOfDispatchContract() {
        // Strategy classes should not implement TaskDispatcher — they are a different concern
        assertThat(TaskDispatcher.class.isAssignableFrom(FairScheduler.class)).isFalse();
    }

    @Test
    void strategySubPackage_doesNotImportSchedulerRootInternals() throws IOException {
        // strategy/ should not depend on RemoteTaskDispatcher or exceptions
        List<String> violations = scanForImports(STRATEGY_PKG,
                "import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher",
                "import org.pangrova.scheduler.core.failure.CircuitBreakerOpenException",
                "import org.pangrova.scheduler.core.failure.NoAvailableWorkerException",
                "import org.pangrova.coordinator.scheduler.SubmitTaskRequestConverter");

        assertThat(violations)
                .as("strategy/ sub-package should not depend on scheduler/ root implementation details")
                .isEmpty();
    }

    @Test
    void exceptionClasses_extendCorrectBaseTypes() {
        assertThat(RuntimeException.class.isAssignableFrom(CircuitBreakerOpenException.class))
                .as("CircuitBreakerOpenException should extend RuntimeException")
                .isTrue();
        assertThat(RuntimeException.class.isAssignableFrom(NoAvailableWorkerException.class))
                .as("NoAvailableWorkerException should extend RuntimeException")
                .isTrue();
    }

    // -- Helpers --

    private static List<String> listJavaFileNames(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter(p -> p.toString().endsWith(".java"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        }
    }

    private static List<String> scanForImports(Path sourceDir, String... importPrefixes)
            throws IOException {
        if (!Files.exists(sourceDir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.walk(sourceDir)) {
            return files
                    .filter(p -> p.toString().endsWith(".java"))
                    .flatMap(p -> findMatchingImports(p, importPrefixes).stream())
                    .toList();
        }
    }

    private static List<String> findMatchingImports(Path javaFile, String... prefixes) {
        try {
            return Files.readAllLines(javaFile).stream()
                    .filter(line -> {
                        String trimmed = line.trim();
                        for (String prefix : prefixes) {
                            if (trimmed.startsWith(prefix)) {
                                return true;
                            }
                        }
                        return false;
                    })
                    .map(line -> javaFile.getFileName() + ": " + line.trim())
                    .toList();
        } catch (IOException e) {
            return List.of("Failed to read: " + javaFile);
        }
    }
}
