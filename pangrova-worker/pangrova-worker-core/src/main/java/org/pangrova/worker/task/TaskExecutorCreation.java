package org.pangrova.worker.task;

import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.execution.NativeTaskCreationFailure;

/** Publishes the actual owner or the exact failure fact from one synchronous creation call. */
final class TaskExecutorCreation {
    private TaskExecutorCreation() {}

    static long create(TaskInfo info, TaskExecutor executor, Runnable preparation) {
        boolean nativeInvoked = false;
        try {
            preparation.run();
            byte[] request = info.request().canonicalBytes();
            long handle;
            if (info.request().copySourceRead() != null) {
                if (!(executor instanceof org.pangrova.worker.execution.NativeTaskExecutor nativeExecutor)) {
                    throw new IllegalStateException("COPY requires the Native execution owner");
                }
                nativeInvoked = true;
                handle = nativeExecutor.createCopyTask(info.request());
            } else {
                nativeInvoked = true;
                handle = executor.createTask(request);
            }
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
