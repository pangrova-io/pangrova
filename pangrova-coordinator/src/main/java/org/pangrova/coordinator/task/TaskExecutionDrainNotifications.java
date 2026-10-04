package org.pangrova.coordinator.task;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.pangrova.protocol.v2.TaskReport;

/** Fans out exact accepted drain facts without replacing another consumer or holding issuance locks. */
final class TaskExecutionDrainNotifications {
    private final CopyOnWriteArrayList<Consumer<TaskReport>> listeners = new CopyOnWriteArrayList<>();
    void add(Consumer<TaskReport> listener) { listeners.add(java.util.Objects.requireNonNull(listener)); }
    void publish(TaskReport report) {
        Throwable failure = null;
        for (var listener : listeners) {
            try { listener.accept(report); }
            catch (RuntimeException | Error rejected) {
                if (failure == null) failure = rejected;
                else if (failure != rejected) failure.addSuppressed(rejected);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure != null) throw (RuntimeException) failure;
    }
}
