package org.pangrova.client.jdbc;

import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;

import java.net.http.HttpClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

/** Owns the driver's sole system-trust HTTP client and actual executor termination barrier. */
@ThreadSafe
final class JdbcSharedHttpOwner implements AutoCloseable {
    @GuardedBy("this") private HttpClient client;
    @GuardedBy("this") private ExecutorService executor;
    @GuardedBy("this") private int threads;
    @GuardedBy("this") private boolean creating;
    @GuardedBy("this") private boolean closing;
    @GuardedBy("this") private boolean sealed;
    @GuardedBy("this") private CompletableFuture<HttpClient> initialization;

    HttpClient get(int requestedThreads, boolean explicit) {
        if (requestedThreads < 1) throw new IllegalArgumentException("poolSize must be a positive integer");
        CompletableFuture<HttpClient> pending;
        boolean initiate;
        synchronized (this) {
            if (sealed) throw new IllegalStateException("Shared HTTP owner admission is sealed; retry cleanup first");
            if ((client != null || creating) && explicit && requestedThreads != threads) {
                throw new IllegalArgumentException("Explicit poolSize conflicts with the existing shared HTTP executor size");
            }
            if (client != null) return client;
            initiate = !creating;
            if (initiate) {
                initialization = new CompletableFuture<>();
                threads = requestedThreads;
                creating = true;
            }
            pending = initialization;
        }
        return initiate ? initialize(requestedThreads, pending) : awaitInitialization(pending);
    }

    private static HttpClient awaitInitialization(CompletableFuture<HttpClient> pending) {
        try { return pending.join(); }
        catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failure.getCause() instanceof Error error) throw error;
            throw failure;
        }
    }

    private HttpClient initialize(int requestedThreads, CompletableFuture<HttpClient> pending) {
        try {
            var allocated = ManagedExecutorFactory.newFixedThreadPool("pangrova-jdbc-http", requestedThreads);
            synchronized (this) { executor = allocated; }
            var initialized = HttpClient.newBuilder().executor(allocated).build();
            synchronized (this) {
                client = initialized;
                if (sealed) throw new IllegalStateException("Shared HTTP owner closed during initialization");
                creating = false;
                pending.complete(initialized);
                return client;
            }
        } catch (RuntimeException | Error failure) {
            synchronized (this) { creating = false; sealed = true; }
            try { close(); }
            catch (RuntimeException | Error cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            pending.completeExceptionally(failure);
            throw failure;
        }
    }

    @Override public void close() {
        ExecutorService selected;
        synchronized (this) {
            sealed = true;
            if (creating || closing) throw new IllegalStateException("Shared HTTP initialization or cleanup is still owned");
            selected = executor;
            if (selected == null) { sealed = false; return; }
            closing = true;
        }
        boolean terminated = false;
        try {
            ExecutorDrain.shutdownAndAwait(selected, 5_000);
            terminated = true;
        } finally {
            synchronized (this) {
                if (terminated && executor == selected) {
                    client = null;
                    executor = null;
                    threads = 0;
                    initialization = null;
                    sealed = false;
                }
                closing = false;
            }
        }
    }
}
