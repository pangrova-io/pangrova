package org.pangrova.worker.task;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.task.TerminalReportDeliveryPolicy;
import org.pangrova.protocol.rest.task.TaskStatusReport;

/** Retains each real terminal envelope until ACK, advancing to release evidence only in order. */
@ThreadSafe
final class TaskTerminalPublication {
    private final AtomicReference<Phase> phase = new AtomicReference<>();

    void publish(
            boolean released, Supplier<TaskStatusReport> prepare, Consumer<TaskStatusReport> send) {
        var candidate =
                new Phase(
                        released,
                        prepare,
                        System.nanoTime()
                                + TerminalReportDeliveryPolicy.DELIVERY_HORIZON.toNanos());
        phase.compareAndSet(null, candidate);
        Phase current = phase.get();
        while (true) {
            current.publish(send);
            if (current.released || !released) return;
            // Even if Native resources have exited, the prior exact envelope must receive its real
            // ACK first.
            var next = new Phase(true, prepare, current.expiresAtNanos);
            phase.compareAndSet(current, next);
            current = phase.get();
        }
    }

    private static final class Phase {
        private final boolean released;
        private final Supplier<TaskStatusReport> prepare;
        private final long expiresAtNanos;
        private final AtomicReference<CompletableFuture<TaskStatusReport>> envelope =
                new AtomicReference<>();
        private final AtomicReference<CompletableFuture<Void>> delivery = new AtomicReference<>();

        private Phase(boolean released, Supplier<TaskStatusReport> prepare, long expiresAtNanos) {
            this.released = released;
            this.prepare = prepare;
            this.expiresAtNanos = expiresAtNanos;
        }

        private void publish(Consumer<TaskStatusReport> send) {
            TaskStatusReport retained = prepare();
            var candidate = new CompletableFuture<Void>();
            var claimed = claim(delivery, candidate);
            if (claimed != candidate) {
                claimed.join();
                return;
            }
            try {
                if (System.nanoTime() - expiresAtNanos >= 0) {
                    throw new IllegalStateException(
                            "Terminal receipt metadata delivery horizon exhausted: "
                                    + retained.taskId());
                }
                send.accept(retained);
                candidate.complete(null);
            } catch (RuntimeException | Error failure) {
                candidate.completeExceptionally(failure);
                throw failure;
            }
        }

        private TaskStatusReport prepare() {
            var candidate = new CompletableFuture<TaskStatusReport>();
            var claimed = claim(envelope, candidate);
            if (claimed != candidate) return claimed.join();
            try {
                TaskStatusReport retained =
                        java.util.Objects.requireNonNull(prepare.get(), "terminal report");
                candidate.complete(retained);
                return retained;
            } catch (RuntimeException | Error failure) {
                candidate.completeExceptionally(failure);
                throw failure;
            }
        }
    }

    private static <T> CompletableFuture<T> claim(
            AtomicReference<CompletableFuture<T>> owner, CompletableFuture<T> candidate) {
        while (true) {
            var current = owner.get();
            if (current != null && !current.isCompletedExceptionally()) return current;
            if (owner.compareAndSet(current, candidate)) return candidate;
        }
    }
}
