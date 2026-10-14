package org.pangrova.testing.e2e.speculation;

import org.pangrova.coordinator.speculation.SpeculationConfig;
import org.pangrova.coordinator.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.speculation.StragglerDetector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskDuration;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for speculative execution pipeline.
 *
 * <p>Validates the straggler detection and speculative task launch path:
 * TaskDuration snapshots → StragglerDetector → SpeculativeTaskLauncher →
 * SpeculativeStageHandler orchestration.
 *
 * <p>Uses real component instances (no mocks) with controlled task duration data.
 */
@DisplayName("Speculative Execution E2E — Straggler Detection and Launch")
class SpeculativeExecutionE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested
    @DisplayName("StragglerDetector Detection Logic")
    class StragglerDetectorTest {

        private final StragglerDetector detector = new StragglerDetector();

        @Test
        @DisplayName("detects straggler when 75%+ tasks completed and outlier exceeds median*1.5")
        void detectsStragglerWithCompletionThreshold() {
            List<TaskDuration> tasks = List.of(
                    new TaskDuration("t1", 5_000, true, false),
                    new TaskDuration("t2", 6_000, true, false),
                    new TaskDuration("t3", 5_500, true, false),
                    new TaskDuration("t4", 50_000, false, false));

            List<String> stragglers = detector.detectStragglers(tasks);

            assertThat(stragglers).containsExactly("t4");
        }

        @Test
        @DisplayName("returns empty when completion ratio below 75%")
        void noDetectionBelowCompletionThreshold() {
            List<TaskDuration> tasks = List.of(
                    new TaskDuration("t1", 5_000, true, false),
                    new TaskDuration("t2", 50_000, false, false),
                    new TaskDuration("t3", 40_000, false, false),
                    new TaskDuration("t4", 30_000, false, false));

            List<String> stragglers = detector.detectStragglers(tasks);

            assertThat(stragglers).isEmpty();
        }

        @Test
        @DisplayName("skips tasks that already have speculative copies")
        void skipsTasksWithExistingSpeculativeCopies() {
            List<TaskDuration> tasks = List.of(
                    new TaskDuration("t1", 5_000, true, false),
                    new TaskDuration("t2", 5_500, true, false),
                    new TaskDuration("t3", 6_000, true, false),
                    new TaskDuration("t4", 50_000, false, true));

            List<String> stragglers = detector.detectStragglers(tasks);

            assertThat(stragglers).isEmpty();
        }

        @Test
        @DisplayName("skips tasks below minimum duration threshold (10s)")
        void skipsTasksBelowMinDuration() {
            List<TaskDuration> tasks = List.of(
                    new TaskDuration("t1", 1_000, true, false),
                    new TaskDuration("t2", 1_200, true, false),
                    new TaskDuration("t3", 1_100, true, false),
                    new TaskDuration("t4", 5_000, false, false));

            List<String> stragglers = detector.detectStragglers(tasks);

            assertThat(stragglers).isEmpty();
        }

        @Test
        @DisplayName("returns empty for empty task list")
        void emptyTaskList() {
            assertThat(detector.detectStragglers(List.of())).isEmpty();
        }
    }

    @Nested
    @DisplayName("SpeculativeTaskLauncher Admission")
    class TaskLauncherTest {

        @Test
        @DisplayName("requires configured dispatch dependencies")
        void launchRequiresDispatchDependencies() {
            SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();

            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> launcher.launchSpeculative("task-1", "worker-A", "query-1"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("dispatch dependencies");
            assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
        }

        @Test
        @DisplayName("requires a nonblank query identity")
        void launchRequiresQueryIdentity() {
            SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> launcher.launchSpeculative("task-1", "worker-A", ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("queryId");
        }

        @Test
        @DisplayName("missing original task cannot leave a speculative pair")
        void missingOriginalRequestDoesNotLeavePair() {
            SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();
            CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
            RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                    CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
            launcher.setDispatchDependencies(
                    dispatcher,
                    new WorkerManager(20_000, 60_000,
                            new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()),
                    tracker);
            launcher.setCancelRetrier(new org.pangrova.coordinator.task.TaskCancelRetrier(
                    dispatcher, tracker));

            assertThat(launcher.launchSpeculative("task-1", "worker-A", "query-1"))
                    .isNull();
            assertThat(launcher.isSpeculative("task-1")).isFalse();
            assertThat(launcher.getSpeculativeTaskId("task-1")).isEmpty();
            assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
        }
    }

    @Nested
    @DisplayName("SpeculativeStageHandler Orchestration")
    class StageHandlerTest {

        @Test
        @DisplayName("disabled config makes handler a no-op")
        void disabledConfigIsNoOp() {
            CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
            SpeculativeStageHandler handler = new SpeculativeStageHandler(
                    SpeculationConfig.DISABLED,
                    new StragglerDetector(),
                    new SpeculativeTaskLauncher(),
                    tracker);

            assertThat(handler.isEnabled()).isFalse();
            // evaluateStragglers should not throw even with no tasks
            handler.evaluateStragglers("q-1", "stage-0");
        }

        @Test
        @DisplayName("enabled config activates straggler evaluation")
        void enabledConfigActivatesEvaluation() {
            CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
            SpeculationConfig config = new SpeculationConfig(true, 5_000L);
            SpeculativeStageHandler handler = new SpeculativeStageHandler(
                    config,
                    new StragglerDetector(),
                    new SpeculativeTaskLauncher(),
                    tracker);

            assertThat(handler.isEnabled()).isTrue();
            // evaluateStragglers with empty tracker should not throw
            handler.evaluateStragglers("q-1", "stage-0");
        }

        @Test
        @DisplayName("null detector and launcher are handled gracefully when enabled")
        void nullDetectorLauncherHandledGracefully() {
            CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
            SpeculationConfig config = new SpeculationConfig(true, 10_000L);
            SpeculativeStageHandler handler = new SpeculativeStageHandler(
                    config, null, null, tracker);

            assertThat(handler.isEnabled()).isTrue();
            handler.evaluateStragglers("q-1", "stage-0");
        }
    }

    @Nested
    @DisplayName("SpeculationConfig Validation")
    class ConfigValidationTest {

        @Test
        @DisplayName("DISABLED singleton has expected defaults")
        void disabledSingletonDefaults() {
            assertThat(SpeculationConfig.DISABLED.enabled()).isFalse();
            assertThat(SpeculationConfig.DISABLED.detectionIntervalMs()).isEqualTo(5_000L);
        }

        @Test
        @DisplayName("rejects non-positive detection interval")
        void rejectsNonPositiveInterval() {
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> new SpeculationConfig(true, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("detectionIntervalMs must be positive");
        }

        @Test
        @DisplayName("accepts valid enabled config")
        void acceptsValidConfig() {
            SpeculationConfig config = new SpeculationConfig(true, 15_000L);

            assertThat(config.enabled()).isTrue();
            assertThat(config.detectionIntervalMs()).isEqualTo(15_000L);
        }
    }

    @Nested
    @DisplayName("End-to-End SQL with Speculation Disabled")
    class SqlWithSpeculationTest {

        private String specTable;

        @BeforeEach
        void setupData() {
            String suffix = Long.toHexString(System.nanoTime());
            specTable = "spec_sql_" + suffix;
            dataSetup.createTestTable(CATALOG, SCHEMA, specTable,
                    "id INT, val VARCHAR(50)");
            String fqn = CATALOG + "." + SCHEMA + "." + specTable;
            sqlRunner.execute("INSERT INTO " + fqn + " VALUES (1, 'row1')");
            sqlRunner.execute("INSERT INTO " + fqn + " VALUES (2, 'row2')");
        }

        @Test
        @DisplayName("query executes normally when speculation is disabled")
        void queryExecutesWithSpeculationDisabled() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA
                            + "." + specTable);
            assertThat(rows).hasSize(1);
            assertThat(SpeculationConfig.DISABLED.enabled()).isFalse();
        }
    }
}
