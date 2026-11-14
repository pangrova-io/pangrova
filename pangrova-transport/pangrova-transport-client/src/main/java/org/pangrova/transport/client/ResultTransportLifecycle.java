package org.pangrova.transport.client;

import org.pangrova.transport.client.exception.TransportConnectionException;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.pangrova.common.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Owns admitted result operations, real reader jobs and shared client leases; monitors never perform I/O. */
@ThreadSafe
final class ResultTransportLifecycle {
    private final Map<String, CompletableFuture<ClientOwner>> cached = new HashMap<>();
    private final Map<ResultTransportClientOwner, ClientOwner> clients = new IdentityHashMap<>();
    private final Map<FlightStream, StreamOwner> streams = new IdentityHashMap<>();
    private int operations;
    private int readers;
    private boolean sealed;
    private boolean closing;
    private boolean closed;

    synchronized Operation enter() {
        if (sealed) throw failure("Result transport admission is closed", null);
        operations = Math.incrementExact(operations);
        return new Operation();
    }

    synchronized void registerClient(ResultTransportClientOwner client) {
        clients.put(client, new ClientOwner(client));
    }

    ClientLease client(String key, Supplier<ResultTransportClientOwner> factory) {
        for (;;) {
            var proposal = new CompletableFuture<ClientOwner>();
            CompletableFuture<ClientOwner> selected;
            synchronized (this) {
                selected = cached.putIfAbsent(key, proposal);
                if (selected == null) selected = proposal;
            }
            if (selected == proposal) build(key, proposal, factory);
            ClientOwner owner = join(selected);
            synchronized (this) {
                if (owner.retired) continue;
                owner.leases = Math.incrementExact(owner.leases);
                return new ClientLease(key, owner);
            }
        }
    }

    private void build(String key, CompletableFuture<ClientOwner> result,
            Supplier<ResultTransportClientOwner> factory) {
        try {
            ResultTransportClientOwner client = factory.get();
            synchronized (this) {
                result.complete(java.util.Objects.requireNonNull(clients.get(client)));
            }
        } catch (RuntimeException | Error error) {
            synchronized (this) {
                cached.remove(key, result);
            }
            result.completeExceptionally(error);
            throw error;
        }
    }

    private static ClientOwner join(CompletableFuture<ClientOwner> selected) {
        try {
            return selected.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failure.getCause() instanceof Error error) throw error;
            throw failure;
        }
    }

    FutureTask<Boolean> read(ExecutorService executor, Callable<Boolean> action) {
        return read(executor, action, false, null);
    }

    FutureTask<Boolean> read(ExecutorService executor, FlightStream stream) {
        StreamOwner owner;
        synchronized (this) {
            owner = java.util.Objects.requireNonNull(streams.get(stream), "Unowned stream");
        }
        return read(executor, stream::next, false, owner);
    }

    private FutureTask<Boolean> read(ExecutorService executor, Callable<Boolean> action,
            boolean cleanup, StreamOwner owner) {
        var task = claimRead(action, cleanup, owner);
        submit(executor, task);
        return task;
    }

    private ReadTask claimRead(Callable<Boolean> action, boolean cleanup, StreamOwner owner) {
        synchronized (this) {
            if (operations == 0 && !(cleanup && closing)) {
                throw failure("Result read has no admitted stream owner", null);
            }
            var task = new ReadTask(action, new AtomicBoolean(), owner);
            int nextReaders = Math.incrementExact(readers);
            int nextOwnerReaders = owner == null ? 0 : Math.incrementExact(owner.readers);
            readers = nextReaders;
            if (owner != null) owner.readers = nextOwnerReaders;
            return task;
        }
    }

    private static void submit(ExecutorService executor, ReadTask task) {
        try {
            executor.execute(task);
        } catch (RuntimeException | Error failure) {
            task.cancel(false);
            throw failure;
        }
    }

    synchronized StreamOwner stream(FlightStream stream, ExecutorService executor) {
        var owner = new StreamOwner(stream, executor);
        streams.put(stream, owner);
        return owner;
    }

    void closeStreams() {
        ArrayList<StreamOwner> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(streams.values());
        }
        TransportConnectionException failure = null;
        for (StreamOwner owner : snapshot) {
            try {
                owner.close();
            } catch (TransportConnectionException cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) throw failure;
    }

    final class StreamOwner implements AutoCloseable {
        private final FlightStream stream;
        private final ExecutorService executor;
        private ReadTask cleanup;
        private int readers;

        StreamOwner(FlightStream stream, ExecutorService executor) {
            this.stream = stream;
            this.executor = executor;
        }

        FlightStream stream() { return stream; }

        @Override public void close() {
            ReadTask selected;
            boolean launch = false;
            synchronized (ResultTransportLifecycle.this) {
                if (!streams.containsKey(stream)) return;
                if (cleanup == null && readers == 0) {
                    cleanup = claimRead(() -> {
                        stream.close();
                        synchronized (ResultTransportLifecycle.this) {
                            streams.remove(stream);
                        }
                        return true;
                    }, true, null);
                    launch = true;
                }
                selected = cleanup;
            }
            if (launch) {
                try {
                    submit(executor, selected);
                } catch (RuntimeException | Error error) {
                    resetFailedCleanup(selected);
                    throw error;
                }
            }
            if (selected == null) {
                stream.cancel("Result reader still owns the stream", null);
                throw failure("Result stream reader is still owned; retry close after it exits", null);
            }
            awaitCleanup(selected);
        }

        private void awaitCleanup(ReadTask selected) {
            try {
                selected.get(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw failure("Result stream close was interrupted; ownership is retained", interrupted);
            } catch (TimeoutException timeout) {
                throw failure("Result stream cleanup is still owned; retry close after it exits", timeout);
            } catch (ExecutionException error) {
                resetFailedCleanup(selected);
                throw failure("Result stream close failed; ownership is retained", error.getCause());
            } catch (java.util.concurrent.CancellationException cancelled) {
                resetFailedCleanup(selected);
                throw failure("Result stream cleanup was not admitted; ownership is retained", cancelled);
            }
        }

        private void resetFailedCleanup(ReadTask selected) {
            synchronized (ResultTransportLifecycle.this) {
                if (cleanup == selected) cleanup = null;
            }
        }
    }

    synchronized boolean beginClose() {
        sealed = true;
        if (closed) return false;
        if (closing || operations != 0 || readers != 0) {
            throw failure("Result operations or readers are still owned; retry close after they exit", null);
        }
        closing = true;
        return true;
    }

    void closeClients() {
        ArrayList<ClientOwner> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(clients.values());
        }
        TransportConnectionException failure = null;
        for (ClientOwner owner : snapshot) {
            try {
                closeClient(owner);
            } catch (TransportConnectionException cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) throw failure;
    }

    synchronized void endClose(boolean succeeded) {
        closed = succeeded;
        closing = false;
        if (succeeded) cached.clear();
    }

    private void closeClient(ClientOwner owner) {
        synchronized (this) {
            if (!streams.isEmpty()) throw failure("Result streams are still owned; client ownership is retained", null);
        }
        try {
            owner.client.close();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw failure("Result client close was interrupted; ownership is retained", interrupted);
        } catch (RuntimeException error) {
            throw failure("Result client close failed; ownership is retained", error);
        }
        synchronized (this) {
            clients.remove(owner.client);
        }
    }

    private static TransportConnectionException failure(String message, Throwable cause) {
        return new TransportConnectionException("TRANSPORT_LIFECYCLE_FAILED", message, cause);
    }

    final class Operation implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();

        @Override public void close() {
            if (released.compareAndSet(false, true)) {
                synchronized (ResultTransportLifecycle.this) {
                    operations--;
                }
            }
        }
    }

    final class ClientLease implements AutoCloseable {
        private final String key;
        private final ClientOwner owner;
        private final AtomicBoolean released = new AtomicBoolean();

        ClientLease(String key, ClientOwner owner) {
            this.key = key;
            this.owner = owner;
        }

        FlightClient client() { return owner.client.client(); }

        void retire() {
            synchronized (ResultTransportLifecycle.this) {
                owner.retired = true;
                cached.computeIfPresent(key, (ignored, future) -> future.getNow(null) == owner ? null : future);
            }
        }
        @Override public void close() {
            if (!released.compareAndSet(false, true)) return;
            boolean retireNow;
            synchronized (ResultTransportLifecycle.this) {
                owner.leases--;
                retireNow = owner.retired && owner.leases == 0 && streams.isEmpty();
            }
            if (retireNow) closeClient(owner);
        }
    }

    private static final class ClientOwner {
        private final ResultTransportClientOwner client;
        private int leases;
        private boolean retired;

        ClientOwner(ResultTransportClientOwner client) { this.client = client; }
    }

    private final class ReadTask extends FutureTask<Boolean> {
        private final AtomicInteger ownership = new AtomicInteger();
        private final AtomicBoolean returned;
        private final StreamOwner owner;

        ReadTask(Callable<Boolean> action, AtomicBoolean returned, StreamOwner owner) {
            super(() -> {
                try {
                    return action.call();
                } finally {
                    readExited(returned, owner);
                }
            });
            this.returned = returned;
            this.owner = owner;
        }
        @Override public void run() {
            if (!ownership.compareAndSet(0, 1)) return;
            try {
                super.run();
            } finally {
                ownership.set(2);
                readExited(returned, owner);
            }
        }
        @Override protected void done() {
            if (isCancelled() && ownership.compareAndSet(0, 2)) {
                readExited(returned, owner);
            }
        }
    }

    private void readExited(AtomicBoolean returned, StreamOwner owner) {
        if (returned.compareAndSet(false, true)) {
            synchronized (this) {
                readers--;
                if (owner != null) owner.readers--;
            }
        }
    }
}
