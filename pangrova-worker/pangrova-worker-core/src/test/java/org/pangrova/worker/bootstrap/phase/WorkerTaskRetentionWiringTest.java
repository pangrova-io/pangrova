package org.pangrova.worker.bootstrap.phase;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerTaskOwnership;
import org.pangrova.worker.task.TaskRetentionRegistryFixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Uses the actual bootstrap timer and task registry; no Native execution is simulated. */
class WorkerTaskRetentionWiringTest {
    @TempDir Path directory;

    @Test
    void configuredRetentionReachesRegistryAndTimerClosesWithOwner() throws Exception {
        var config = config(1);
        var manager = TaskRetentionRegistryFixture.manager(directory);
        var ownership = new WorkerTaskOwnership(config.resourceConfig().shuffleChannelLimits());
        ownership.publish(manager);
        Set<Long> previousTimers = cleanupThreads();
        try {
            WorkerTaskBootstrap.startTaskCleanup(config, manager, ownership);
            TaskRetentionRegistryFixture.addFinished(manager, "recent-terminal");
            // A long-running task must be retained from terminal time, not its old start time.
            await().during(Duration.ofMillis(250)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(manager.getTaskState("recent-terminal")).isPresent());
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(manager.getTaskState("recent-terminal")).isEmpty());
            assertThat(cleanupThreads()).anyMatch(id -> !previousTimers.contains(id));
        } finally {
            ownership.close();
        }
        assertThat(ownership.isDrained()).isTrue();
        assertThat(cleanupThreads()).isSubsetOf(previousTimers);
        ownership.close();
    }

    @Test
    void longerConfiguredRetentionDoesNotUseShortTimerAge() throws Exception {
        var config = config(600);
        var manager = TaskRetentionRegistryFixture.manager(directory);
        var ownership = new WorkerTaskOwnership(config.resourceConfig().shuffleChannelLimits());
        ownership.publish(manager);
        try {
            WorkerTaskBootstrap.startTaskCleanup(config, manager, ownership);
            TaskRetentionRegistryFixture.addFinished(manager, "retained");
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                    .untilAsserted(() -> assertThat(manager.getTaskState("retained")).isPresent());
        } finally {
            ownership.close();
        }
    }

    private WorkerBootstrapConfig config(int seconds) {
        return new WorkerBootstrapConfig("retention-worker", 0, 0, null,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, seconds, false, null, null, null,
                null, null, null, null, null, null, directory.toString());
    }

    private static Set<Long> cleanupThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.isAlive() && thread.getName().startsWith("worker-task-cleanup"))
                .map(Thread::getId).collect(Collectors.toSet());
    }
}
