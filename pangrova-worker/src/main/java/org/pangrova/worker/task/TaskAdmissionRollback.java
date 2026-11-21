package org.pangrova.worker.task;

/** Returns all pre-Native admission resources before publishing the original cleanup failure. */
final class TaskAdmissionRollback {
    private TaskAdmissionRollback() {}

    static void release(Runnable... actions) {
        Throwable failure = null;
        for (var action : actions) {
            try {
                action.run();
            } catch (RuntimeException | Error error) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure instanceof RuntimeException error) {
            throw error;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }
}
