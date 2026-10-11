package org.pangrova.worker.task;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.execution.NativeTaskCreationFailure;

/** Publishes the actual owner or the exact failure fact from one synchronous creation call. */
final class TaskExecutorCreation {
    private TaskExecutorCreation() {}

    static long create(TaskInfo info, TaskExecutor executor, Runnable preparation) {
        boolean nativeInvoked = false;
        try {
            preparation.run();
            byte[] request = info.request().canonicalBytes();
            nativeInvoked = true;
            long handle = executor.createTask(request);
            if (handle <= 0) {
                throw new IllegalStateException(
                        "Task creation returned no positive actual owner: " + info.taskId());
            }
            info.setExecutorHandle(handle);
            return handle;
        } catch (RuntimeException | Error failure) {
            if (!nativeInvoked) {
                info.nativeCreationRolledBackBeforeEntry();
            } else if (failure instanceof NativeTaskCreationFailure creation
                    && creation.actualHandle() > 0) {
                info.setExecutorHandle(creation.actualHandle());
            } else if (failure instanceof NativeTaskCreationFailure creation
                    && creation.withoutNativeOwner()) {
                info.nativeCreationRolledBackBeforeEntry();
            } else {
                info.failNativeCreation(failure);
            }
            throw failure;
        }
    }
}
