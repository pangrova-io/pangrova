package org.pangrova.worker.bootstrap.ownership;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory;
import org.pangrova.worker.task.TaskManager;

/** Retains the partially initialized task subsystem until its actual owners have drained. */
@ThreadSafe
public final class WorkerTaskOwnership implements AutoCloseable {
    private final NativeShuffleWorkerFactory.Session shuffle;

    @GuardedBy("this")
    private final List<AutoCloseable> partialOwners = new ArrayList<>();

    @GuardedBy("this")
    private TaskManager manager;

    @GuardedBy("this")
    private int transferredPartialOwners;

    @GuardedBy("this")
    private boolean sealed;

    @GuardedBy("this")
    private boolean drained;

    @GuardedBy("this")
    private long generation;

    /** Direct Native shuffle has no JVM provider session. */
    public WorkerTaskOwnership() {
        this(null);
    }

    public WorkerTaskOwnership(NativeShuffleWorkerFactory.Session shuffle) {
        this.shuffle = shuffle;
    }

    public NativeShuffleWorkerFactory.Session shuffle() {
        return java.util.Objects.requireNonNull(shuffle, "Worker has no external shuffle provider");
    }

    /** Retains a partially created owner until successful transfer or actual cleanup. */
    public synchronized void retain(AutoCloseable owner) {
        partialOwners.add(java.util.Objects.requireNonNull(owner, "owner"));
        generation++;
        drained = false;
        if (sealed) throw new IllegalStateException("Task bootstrap ownership is sealed");
    }

    /**
     * Keeps the acquired resource if construction fails. Once the new owner exists only it closes
     * the resource. Construction is serialized with shutdown and must not wait for shutdown.
     */
    public synchronized <R extends AutoCloseable, T extends AutoCloseable> T retain(
            R resource, java.util.function.Function<R, T> constructor) {
        retain(resource);
        int slot = partialOwners.size() - 1;
        T owner = java.util.Objects.requireNonNull(constructor.apply(resource), "owner");
        partialOwners.set(slot, owner);
        return owner;
    }

    /** Transfers the initialized manager and the already retained partial owners. */
    public synchronized void publish(TaskManager actualManager) {
        this.manager = java.util.Objects.requireNonNull(actualManager, "manager");
        transferredPartialOwners = partialOwners.size();
        generation++;
        drained = false;
        if (sealed) throw new IllegalStateException("Task bootstrap ownership is sealed");
    }

    public synchronized boolean isDrained() {
        return drained;
    }

    @Override
    public void close() {
        final TaskManager actualManager;
        final List<AutoCloseable> actualPartialOwners;
        final long observedGeneration;
        synchronized (this) {
            sealed = true;
            if (drained) return;
            actualManager = manager;
            actualPartialOwners =
                    List.copyOf(
                            partialOwners.subList(
                                    actualManager == null ? 0 : transferredPartialOwners,
                                    partialOwners.size()));
            observedGeneration = generation;
        }
        if (shuffle != null) shuffle.seal();
        RuntimeException failure = null;
        for (int index = actualPartialOwners.size() - 1; index >= 0; index--) {
            failure = closeOwner(actualPartialOwners.get(index), failure);
        }
        if (actualManager != null) failure = closeOwner(actualManager, failure);
        try {
            if (shuffle != null) shuffle.requireDrained();
        } catch (RuntimeException observed) {
            if (failure == null) failure = observed;
            else failure.addSuppressed(observed);
        }
        if (failure != null) throw failure;
        synchronized (this) {
            if (observedGeneration != generation) {
                throw new IllegalStateException("Task bootstrap admission changed while draining");
            }
            drained = true;
            partialOwners.clear();
            transferredPartialOwners = 0;
        }
    }

    private static RuntimeException closeOwner(AutoCloseable owner, RuntimeException aggregate) {
        try {
            owner.close();
        } catch (Exception | Error failure) {
            var observed =
                    new IllegalStateException("Task bootstrap owner has not drained", failure);
            if (aggregate == null) return observed;
            aggregate.addSuppressed(observed);
        }
        return aggregate;
    }
}
