package org.pangrova.coordinator.task;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies per-task timeout enforcement via CoordinatorTaskTracker deadlines.
 * Tasks past their deadline are detected by getTimedOutTasks() and can trigger
 * query failure.
 */
class CoordinatorTaskTimeoutTest {

    private CoordinatorTaskTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
    }

    @Test
    void taskPastDeadlineDetectedAsTimedOut() {
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");
        tracker.setDeadline("t1", Instant.now().minusSeconds(10));

        Map<String, String> timedOut = tracker.getTimedOutTasks(Instant.now());

        assertEquals(1, timedOut.size());
        assertEquals("q1", timedOut.get("t1"));
    }

    @Test
    void taskBeforeDeadlineNotTimedOut() {
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");
        tracker.setDeadline("t1", Instant.now().plusSeconds(300));

        Map<String, String> timedOut = tracker.getTimedOutTasks(Instant.now());

        assertTrue(timedOut.isEmpty());
    }

    @Test
    void finishedTaskNotReportedAsTimedOut() {
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");
        tracker.updateTaskState("q1", "t1", "RUNNING");
        tracker.updateTaskState("q1", "t1", "FINISHED");
        tracker.setDeadline("t1", Instant.now().minusSeconds(10));

        Map<String, String> timedOut = tracker.getTimedOutTasks(Instant.now());

        assertTrue(timedOut.isEmpty(),
                "Terminal tasks should not appear in timed-out set");
    }

    @Test
    void removeDeadlineClearsTracking() {
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");
        tracker.setDeadline("t1", Instant.now().minusSeconds(10));

        tracker.removeDeadline("t1");

        Map<String, String> timedOut = tracker.getTimedOutTasks(Instant.now());
        assertTrue(timedOut.isEmpty());
    }

    @Test
    void multipleTasksWithMixedDeadlines() {
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");
        tracker.setDeadline("t1", Instant.now().minusSeconds(10));

        tracker.registerTask("q2", "t2");
        tracker.markScheduled("q2", "t2");
        tracker.setDeadline("t2", Instant.now().plusSeconds(300));

        tracker.registerTask("q3", "t3");
        tracker.markScheduled("q3", "t3");
        tracker.setDeadline("t3", Instant.now().minusSeconds(5));

        Map<String, String> timedOut = tracker.getTimedOutTasks(Instant.now());

        assertEquals(2, timedOut.size());
        assertEquals("q1", timedOut.get("t1"));
        assertEquals("q3", timedOut.get("t3"));
    }

    @Test
    void configDefaultTaskTimeoutDerivedFromQueryTimeout() {
        var config = org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig
                .withDefaults("coord-1", 8080);

        assertEquals(0, config.taskTimeoutSeconds(),
                "Default task timeout should be 0 (derived at runtime)");
        assertEquals(config.queryTimeoutSeconds() / 2,
                config.effectiveTaskTimeoutSeconds(),
                "Effective timeout should be queryTimeout / 2");
    }

    @Test
    void configExplicitTaskTimeoutOverridesDefault() {
        var config = new org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig(
                new org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.CoreConfig("coord-1", 8080, 30_000, org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED, "0.0.0.0"),
                org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                new org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.QueryConfig(300, 10, 60),
                org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.TransportConfig.defaults(),
                org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        assertEquals(60, config.effectiveTaskTimeoutSeconds(),
                "Explicit taskTimeoutSeconds should override derived value");
    }
}
