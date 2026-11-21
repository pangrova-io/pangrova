package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Seals actual Native creation admission; an exception after entry never proves absence of an owner. */
@ThreadSafe
final class TaskNativeCreation {
    private final AtomicReference<Creation> creation = new AtomicReference<>();

    void begin() {
        if (!creation.compareAndSet(null, new Creation(new CompletableFuture<>(), false))) {
            throw new IllegalStateException("Native creation is already entered or admission is sealed");
        }
    }

    void publish(long handle, java.util.function.LongConsumer publishHandle) {
        if (handle <= 0) throw new IllegalArgumentException("Actual Native owner must have a positive handle");
        creation.compareAndSet(null, new Creation(new CompletableFuture<>(), false));
        var current = creation.get();
        if (current.withoutOwner()) throw new IllegalStateException("Native owner appeared after admission closed");
        publishHandle.accept(handle);
        if (!current.exit().complete(handle) && current.exit().join() != handle) {
            throw new IllegalStateException("Native creation published a different owner");
        }
    }

    void failed(Throwable failure) {
        var current = creation.get();
        if (current == null || current.withoutOwner()) {
            throw new IllegalStateException("Native creation failure has no entered creation", failure);
        }
        current.exit().completeExceptionally(failure);
    }

    void failedBeforeNativeEntry() {
        var current = creation.get();
        if (current == null || current.exit().isDone()) {
            throw new IllegalStateException("No entered creation can publish verified pre-Native rollback");
        }
        if (!creation.compareAndSet(current, new Creation(CompletableFuture.completedFuture(0L), true))) {
            throw new IllegalStateException("Native creation admission changed during rollback");
        }
        current.exit().complete(0L);
    }

    boolean sealWithoutOwnerOrAwaitExit() {
        var sealed = new Creation(CompletableFuture.completedFuture(0L), true);
        creation.compareAndSet(null, sealed);
        var current = creation.get();
        current.exit().join(); // actual creation exit, not cancellation/isDone of a submitted Future
        return creation.get().withoutOwner();
    }

    void resolveEnteredFailure(long handle, java.util.function.LongConsumer publishHandle) {
        if (handle < 0) throw new IllegalArgumentException("Recovered Native handle exceeds its domain");
        var current = creation.get();
        if (current == null || !current.exit().isCompletedExceptionally()) {
            throw new IllegalStateException("Creation recovery does not match an unresolved entered call");
        }
        var resolved = new Creation(CompletableFuture.completedFuture(handle), handle == 0);
        if (!creation.compareAndSet(current, resolved)) {
            throw new IllegalStateException("Creation owner changed during exact recovery");
        }
        publishHandle.accept(handle);
    }

    private record Creation(CompletableFuture<Long> exit, boolean withoutOwner) {}
}
