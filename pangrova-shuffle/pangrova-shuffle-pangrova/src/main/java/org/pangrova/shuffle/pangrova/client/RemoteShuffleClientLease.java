package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.pangrova.common.concurrency.ThreadSafe;

/** Keeps an actual channel alive for the full producer/consumer owner, including renewal. */
@ThreadSafe
public final class RemoteShuffleClientLease implements AutoCloseable {
    private final RemoteShuffleClientPool pool;
    private final PangrovaShuffleClient client;
    private final AtomicBoolean returned = new AtomicBoolean();
    private volatile boolean closed;
    private final AtomicReference<CompletableFuture<Void>> closeAttempt = new AtomicReference<>();

    RemoteShuffleClientLease(RemoteShuffleClientPool pool, PangrovaShuffleClient client) {
        this.pool = pool;
        this.client = client;
    }

    public PangrovaShuffleClient client() {
        if (returned.get()) throw new IllegalStateException("Remote shuffle client lease was released");
        return client;
    }

    @Override
    public void close() {
        if (closed) return;
        var attempt = new CompletableFuture<Void>();
        if (!closeAttempt.compareAndSet(null, attempt)) {
            awaitConcurrentClose();
            return;
        }
        try {
            pool.returnLease(client, returned.compareAndSet(false, true));
            closed = true;
            attempt.complete(null);
        } catch (RuntimeException | Error failure) {
            attempt.completeExceptionally(failure);
            throw failure;
        } finally {
            closeAttempt.compareAndSet(attempt, null);
        }
    }

    private void awaitConcurrentClose() {
        var active = closeAttempt.get();
        if (active == null) {
            if (!closed) close();
            return;
        }
        try {
            active.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RemoteShuffleClientException("Concurrent client lease return interrupted", failure);
        } catch (ExecutionException failure) {
            throw new RemoteShuffleClientException("Concurrent client lease return failed", failure.getCause());
        } catch (TimeoutException failure) {
            throw new RemoteShuffleClientException("Concurrent client lease return has not exited", failure);
        }
    }
}
