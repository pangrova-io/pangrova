package org.pangrova.shuffle.pangrova.client.coordinator;

import org.pangrova.common.api.annotation.Internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Serializes exact execution operations without holding a monitor across RPC or database I/O. */
@Internal
final class ShuffleLifecycleOperations {
    private final Map<Object, CompletableFuture<Void>> tails = new HashMap<>();
    private boolean closed;

    /** Only queue admission is synchronized; the actual operation executes outside the monitor. */
    public <T> T execute(String executionId, Supplier<T> operation) {
        return executeQueued(executionId, operation);
    }

    /** Independent task grants can run in parallel. Each gets its own slot so close drains every RPC. */
    public <T> T executeGrant(Supplier<T> operation) {
        return executeQueued(new Object(), operation);
    }

    private <T> T executeQueued(Object queueKey, Supplier<T> operation) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        CompletableFuture<Void> predecessor;
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Shuffle lifecycle operation admission is closed");
            }
            predecessor = tails.put(queueKey, completion);
        }
        try {
            if (predecessor != null) {
                predecessor.join();
            }
            return operation.get();
        } finally {
            synchronized (this) {
                tails.remove(queueKey, completion);
            }
            completion.complete(null);
        }
    }

    /** Closes admission and drains actual admitted operations before transport shutdown. */
    public void awaitIdle() {
        List<CompletableFuture<Void>> pending;
        synchronized (this) {
            closed = true;
            pending = List.copyOf(tails.values());
        }
        pending.forEach(CompletableFuture::join);
    }
}
