package org.pangrova.worker.task;

import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.exception.NativeTaskCreationFailure;

/** Publishes the actual owner or the exact failure fact from one synchronous creation call. */
final class TaskExecutorCreation {
    private TaskExecutorCreation() {}

    static long create(TaskInfo info, TaskExecutor executor, ValidatedTaskSubmission admission,
            Runnable preparation) {
        boolean nativeInvoked = false;
        try {
            preparation.run();
            if (admission.submission().copySourceRead() != null
                    && org.pangrova.worker.execution.NativeTaskExecutor.ownerOf(executor).isEmpty()) {
                throw new IllegalStateException("COPY requires the Native execution owner");
            }
            nativeInvoked = true;
            long handle = executor.createAdmittedTask(admission);
            if (handle <= 0) {
                throw new IllegalStateException(
                        "Task creation returned no positive actual owner: " + info.taskId());
            }
            info.setExecutorHandle(handle);
            return handle;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof NativeTaskCreationFailure creation) {
                info.setPreNativeReadBytes(creation.workerObjectReadBytes());
            }
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
