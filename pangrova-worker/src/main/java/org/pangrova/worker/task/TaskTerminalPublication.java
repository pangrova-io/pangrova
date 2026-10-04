package org.pangrova.worker.task;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.task.TerminalReportDeliveryPolicy;
import org.pangrova.protocol.rest.task.TaskStatusReport;

/** Retains one real post-release envelope until its exact metadata ACK is received. */
@ThreadSafe
final class TaskTerminalPublication {
    private final AtomicReference<CompletableFuture<Envelope>> envelope = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> delivery = new AtomicReference<>();

    void publish(Supplier<TaskStatusReport> prepare, Consumer<TaskStatusReport> send) {
        Envelope retained = prepare(prepare);
        var candidate = new CompletableFuture<Void>();
        var claimed = claim(delivery, candidate);
        if (claimed != candidate) { claimed.join(); return; }
        try {
            if (System.nanoTime() - retained.expiresAtNanos() >= 0) {
                throw new IllegalStateException("Terminal receipt metadata delivery horizon exhausted: "
                        + retained.report().taskId());
            }
            send.accept(retained.report());
            candidate.complete(null);
        } catch (RuntimeException | Error failure) {
            candidate.completeExceptionally(failure);
            throw failure;
        }
    }

    private Envelope prepare(Supplier<TaskStatusReport> prepare) {
        var candidate = new CompletableFuture<Envelope>();
        var claimed = claim(envelope, candidate);
        if (claimed != candidate) return claimed.join();
        try {
            Envelope retained = new Envelope(prepare.get(), System.nanoTime()
                    + TerminalReportDeliveryPolicy.DELIVERY_HORIZON.toNanos());
            candidate.complete(retained);
            return retained;
        } catch (RuntimeException | Error failure) {
            candidate.completeExceptionally(failure);
            throw failure;
        }
    }

    private static <T> CompletableFuture<T> claim(AtomicReference<CompletableFuture<T>> owner,
                                                 CompletableFuture<T> candidate) {
        while (true) {
            var current = owner.get();
            if (current != null && !current.isCompletedExceptionally()) return current;
            if (owner.compareAndSet(current, candidate)) return candidate;
        }
    }

    private record Envelope(TaskStatusReport report, long expiresAtNanos) { }
}
