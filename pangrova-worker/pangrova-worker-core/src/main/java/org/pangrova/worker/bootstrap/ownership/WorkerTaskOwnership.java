package org.pangrova.worker.bootstrap.ownership;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission;
import org.pangrova.worker.task.TaskManager;

/** Retains the partially initialized task subsystem until its actual owners have drained. */
@ThreadSafe
public final class WorkerTaskOwnership implements AutoCloseable {
    private final RemoteShuffleChannelAdmission channelAdmission;

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

    public WorkerTaskOwnership(RemoteShuffleChannelAdmission.Limits limits) {
        channelAdmission = new RemoteShuffleChannelAdmission(limits);
    }

    public RemoteShuffleChannelAdmission channelAdmission() {
        return channelAdmission;
    }

    /** Retains a partially created owner until successful transfer or actual cleanup. */
    public synchronized void retain(AutoCloseable owner) {
        partialOwners.add(java.util.Objects.requireNonNull(owner, "owner"));
        generation++;
        drained = false;
        if (sealed) throw new IllegalStateException("Task bootstrap ownership is sealed");
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
        channelAdmission.seal();
        RuntimeException failure = null;
        for (int index = actualPartialOwners.size() - 1; index >= 0; index--) {
            failure = closeOwner(actualPartialOwners.get(index), failure);
        }
        if (actualManager != null) failure = closeOwner(actualManager, failure);
        try {
            channelAdmission.requireDrained();
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
