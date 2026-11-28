package org.pangrova.worker.task;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.pangrova.common.concurrency.GuardedBy;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.constants.TerminalReportDeliveryPolicy;
import org.pangrova.protocol.rest.task.TaskStatusReport;

/** Retains each real terminal envelope until ACK, advancing to release evidence only in order. */
@ThreadSafe
final class TaskTerminalPublication {
    private final AtomicReference<Phase> phase = new AtomicReference<>();

    void publish(
            boolean released, Supplier<TaskStatusReport> prepare, Consumer<TaskStatusReport> send) {
        try {
            publishAsync(released, prepare, report -> {
                send.accept(report);
                return CompletableFuture.completedFuture(null);
            }).toCompletableFuture().join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }

    CompletionStage<Void> publishAsync(
            boolean released,
            Supplier<TaskStatusReport> prepare,
            Function<TaskStatusReport, CompletionStage<Void>> send) {
        return publishAsync(released, prepare, send, null);
    }

    CompletionStage<Void> publishAsync(
            boolean released,
            Supplier<TaskStatusReport> prepare,
            Function<TaskStatusReport, CompletionStage<Void>> send,
            Runnable onInitialAcknowledgement) {
        Phase current = phase.get();
        if (current == null) {
            var candidate = new Phase(released, prepare, System.nanoTime()
                    + TerminalReportDeliveryPolicy.DELIVERY_HORIZON.toNanos());
            phase.compareAndSet(null, candidate);
            current = phase.get();
        }
        if (onInitialAcknowledgement != null) {
            current.observeAcknowledgement().thenRun(onInitialAcknowledgement);
        }
        CompletionStage<Void> acknowledged = current.deliver(send);
        if (current.released || !released) return acknowledged;
        // Physical cleanup can leave while the original exact envelope awaits its ACK.
        Phase preceding = current;
        return acknowledged.thenCompose(ignored -> {
            var next = new Phase(true, prepare, preceding.expiresAtNanos);
            phase.compareAndSet(preceding, next);
            return phase.get().deliver(send);
        });
    }

    void retryFailedInitialDelivery(Function<TaskStatusReport, CompletionStage<Void>> send) {
        Phase current = phase.get();
        if (current != null && !current.released && current.deliveryFailed()) {
            current.deliver(send);
        }
    }

    private static final class Phase {
        private final boolean released;
        private final Supplier<TaskStatusReport> prepare;
        private final long expiresAtNanos;
        private final AtomicReference<CompletableFuture<TaskStatusReport>> envelope =
                new AtomicReference<>();
        private final AtomicReference<CompletableFuture<Void>> delivery = new AtomicReference<>();
        @GuardedBy("this")
        private CompletableFuture<Void> acknowledgement;

        private Phase(boolean released, Supplier<TaskStatusReport> prepare, long expiresAtNanos) {
            this.released = released;
            this.prepare = prepare;
            this.expiresAtNanos = expiresAtNanos;
        }

        private synchronized CompletionStage<Void> observeAcknowledgement() {
            var attempt = delivery.get();
            if (attempt != null && attempt.isDone() && !attempt.isCompletedExceptionally()) return attempt;
            if (acknowledgement == null) acknowledgement = new CompletableFuture<>();
            return acknowledgement;
        }

        private void signalAcknowledgement() {
            CompletableFuture<Void> observer;
            synchronized (this) {
                observer = acknowledgement;
            }
            if (observer != null) observer.complete(null);
        }

        private boolean deliveryFailed() {
            var attempt = delivery.get();
            return attempt != null && attempt.isCompletedExceptionally();
        }

        private CompletionStage<Void> deliver(Function<TaskStatusReport, CompletionStage<Void>> send) {
            var existing = delivery.get();
            if (existing != null && !existing.isCompletedExceptionally()) return existing;
            var candidate = new CompletableFuture<Void>();
            var claimed = claim(delivery, candidate);
            if (claimed != candidate) return claimed;
            TaskStatusReport retained;
            try {
                retained = prepare();
            } catch (RuntimeException | Error failure) {
                candidate.completeExceptionally(failure);
                throw failure;
            }
            try {
                if (System.nanoTime() - expiresAtNanos >= 0) {
                    throw new IllegalStateException(
                            "Terminal receipt metadata delivery horizon exhausted: "
                                    + retained.taskId());
                }
                send.apply(retained).whenComplete((ignored, failure) -> {
                    if (failure != null) {
                        candidate.completeExceptionally(failure);
                    } else {
                        candidate.complete(null);
                        signalAcknowledgement();
                    }
                });
            } catch (RuntimeException failure) {
                candidate.completeExceptionally(failure);
            } catch (Error failure) {
                candidate.completeExceptionally(failure);
                throw failure;
            }
            return candidate;
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
