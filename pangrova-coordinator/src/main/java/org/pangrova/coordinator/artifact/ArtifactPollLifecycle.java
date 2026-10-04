package org.pangrova.coordinator.artifact;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

/** Single reconciliation owner and shutdown barrier, without a monitor spanning external I/O. */
final class ArtifactPollLifecycle {
    private final AtomicReference<CompletableFuture<Void>> owner = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<Void>> closeOwner = new AtomicReference<>();

    int poll(IntSupplier reconciliation) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        if (closed.get() || !owner.compareAndSet(null, completion)) {
            return 0;
        }
        try {
            return closed.get() ? 0 : reconciliation.getAsInt();
        } finally {
            owner.compareAndSet(completion, null);
            completion.complete(null);
        }
    }

    void close(Runnable cleanup) {
        closed.set(true);
        var completion = new CompletableFuture<Void>();
        while (!closeOwner.compareAndSet(null, completion)) {
            var existing = closeOwner.get();
            if (existing != null) {
                existing.join();
                return;
            }
        }
        try {
            CompletableFuture<Void> inFlight = owner.get();
            if (inFlight != null) {
                inFlight.join();
            }
            cleanup.run();
            completion.complete(null);
        } catch (RuntimeException | Error failure) {
            completion.completeExceptionally(failure);
            closeOwner.compareAndSet(completion, null);
            throw failure;
        }
    }
}
