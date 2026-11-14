package org.pangrova.worker.execution;

import org.pangrova.worker.exception.NativeTaskCreationFailure;

import org.pangrova.shuffle.api.worker.ShuffleProducerSession;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.common.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;

/** Preallocated creation transaction owners, retained across failed Native/Java publication. */
@ThreadSafe
final class NativeCreationOwners {
    private final ConcurrentHashMap<TaskAttemptId, Owner> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Owner> handles = new ConcurrentHashMap<>();

    Owner prepare(TaskRequest request) {
        var owner = new Owner(request);
        if (pending.putIfAbsent(request.getAttempt(), owner) != null) {
            throw new IllegalStateException(
                    "An unresolved creation already owns this exact task attempt");
        }
        return owner;
    }

    Owner forHandle(long handle) {
        var indexed = handles.get(handle);
        if (indexed != null) return indexed;
        // Only unresolved publication takes this cold recovery lookup. Normal handles use the
        // ordinary index.
        return pending.values().stream()
                .filter(owner -> owner.handle == handle && handle > 0)
                .findFirst()
                .orElse(null);
    }

    void publish(Owner owner) {
        var previous = handles.putIfAbsent(owner.handle, owner);
        if (previous != null && previous != owner) {
            throw new IllegalStateException("Native handle already has a different creation owner");
        }
    }

    boolean contains(Owner owner) {
        return pending.get(owner.request.getAttempt()) == owner;
    }

    boolean ownsAttempt(TaskAttemptId attempt) {
        return pending.containsKey(attempt);
    }

    Owner forAttempt(TaskAttemptId attempt) {
        return pending.get(attempt);
    }

    void remove(Owner owner) {
        if (owner.handle > 0) handles.remove(owner.handle, owner);
        pending.remove(owner.request.getAttempt(), owner);
    }

    boolean isEmpty() {
        return pending.isEmpty();
    }

    java.util.List<Owner> snapshot() {
        return java.util.List.copyOf(pending.values());
    }

    NativeTaskCreationFailure rollbackWithoutNativeOwner(
            Owner owner,
            ShuffleProducerSession<TaskRequest> egress,
            Throwable original) {
        try {
            cleanupCallbacks(owner, egress);
            remove(owner);
        } catch (Throwable cleanup) {
            original.addSuppressed(cleanup);
            return new NativeTaskCreationFailure(0, original);
        }
        return new NativeTaskCreationFailure(0, true, original);
    }

    static void cleanupCallbacks(
            Owner owner, ShuffleProducerSession<TaskRequest> egress) {
        Throwable failure = null;
        try {
            if (owner.externalRegistered) {
                egress.releaseTask(owner.request);
                owner.externalRegistered = false;
            }
        } catch (Throwable cleanup) {
            failure = cleanup;
        }
        try {
            if (owner.aiRegistered) {
                AiTaskLifecycleRegistry.current().releaseTask(
                        owner.request.getSecurity().getTenantId(), owner.request.getAttempt());
                owner.aiRegistered = false;
            }
        } catch (Throwable cleanup) {
            if (failure == null) failure = cleanup;
            else failure.addSuppressed(cleanup);
        }
        try {
            if (owner.copyInput != null) {
                owner.copyInput.close();
                owner.copyInput = null;
            }
        } catch (Throwable cleanup) {
            if (failure == null) failure = cleanup;
            else failure.addSuppressed(cleanup);
        }
        if (failure != null) throw new NativeTaskCreationFailure(owner.handle, failure);
    }

    @ThreadSafe
    static final class Owner {
        final TaskRequest request;
        final NativeOperationLifetime operations = new NativeOperationLifetime();
        final Thread constructionThread = Thread.currentThread();
        volatile long handle;
        volatile boolean enteredNative;
        volatile boolean nativeClosedWithoutOwner;
        volatile boolean aiRegistered;
        volatile boolean externalRegistered;
        volatile org.pangrova.worker.copy.VerifiedLocalCopySource copyInput;
        volatile org.pangrova.worker.copy.CopyTaskReadBudget copyReadBudget;
        volatile byte[] releaseReceipt;
        final java.util.concurrent.CompletableFuture<Void> constructionExit =
                new java.util.concurrent.CompletableFuture<>();

        Owner(TaskRequest request) {
            this.request = request;
        }

        void awaitConstructionExit() {
            if (!constructionExit.isDone() && constructionThread == Thread.currentThread()) {
                throw new IllegalStateException(
                        "Cannot join a creation transaction from its own entered thread");
            }
            constructionExit.join();
        }
    }
}
