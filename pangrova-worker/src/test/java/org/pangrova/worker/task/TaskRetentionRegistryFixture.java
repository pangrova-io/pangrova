package org.pangrova.worker.task;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;
import org.pangrova.worker.enums.TaskState;

/** State-only task registry for bootstrap timer tests, not Native release evidence. */
public final class TaskRetentionRegistryFixture {
    private TaskRetentionRegistryFixture() {}

    public static TaskManager manager(Path directory) throws IOException {
        return new TaskManager(ProcessIncarnationStore.open(directory));
    }

    public static void addFinished(TaskManager manager, String id) {
        var info = new TaskInfo(id, "retention-query", null, null, Instant.now().minusSeconds(3600));
        if (!info.compareAndSetState(TaskState.RECEIVED, TaskState.RUNNING)
                || !info.compareAndSetState(TaskState.RUNNING, TaskState.FINISHED)) {
            throw new AssertionError("Failed to construct terminal fixture");
        }
        manager.tasks.register(info);
    }
}
