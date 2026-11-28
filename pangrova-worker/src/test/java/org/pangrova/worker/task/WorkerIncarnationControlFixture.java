package org.pangrova.worker.task;

import java.nio.file.Path;
import java.util.OptionalLong;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Real admission-process ownership for HTTP control tests without a Native execution pipeline. */
public final class WorkerIncarnationControlFixture implements AutoCloseable {
    private final TaskManager manager;

    public WorkerIncarnationControlFixture(Path directory) {
        manager = new TaskManager(ProcessIncarnationStore.open(directory));
    }

    public TaskManager manager() {
        return manager;
    }

    /** Observes the real Native handle for the exact admitted owner, including during result drain. */
    public static OptionalLong nativeHandle(TaskManager manager, String localTaskId, TaskAttemptId expected) {
        var task = manager.tasks.get(localTaskId);
        if (task == null) return OptionalLong.empty();
        if (!task.request().taskRequest().getAttempt().equals(expected)) {
            throw new IllegalStateException("Worker owns a different attempt at " + localTaskId);
        }
        long handle = task.executorHandle();
        return handle == 0 ? OptionalLong.empty() : OptionalLong.of(handle);
    }

    @Override
    public void close() {
        manager.close();
    }
}
