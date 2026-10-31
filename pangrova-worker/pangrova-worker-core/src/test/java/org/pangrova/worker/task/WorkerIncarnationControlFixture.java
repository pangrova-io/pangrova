package org.pangrova.worker.task;

import java.nio.file.Path;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;

/** Real admission-process ownership for HTTP control tests without a Native execution pipeline. */
public final class WorkerIncarnationControlFixture implements AutoCloseable {
    private final TaskManager manager;

    public WorkerIncarnationControlFixture(Path directory) {
        manager = new TaskManager(ProcessIncarnationStore.open(directory));
    }

    public TaskManager manager() {
        return manager;
    }

    @Override
    public void close() {
        manager.close();
    }
}
