package org.pangrova.testing.e2e.speculation;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Opaque duplex TCP forwarding; TLS, tickets and result bytes are never decoded or rewritten. */
@ThreadSafe
final class ProcessResultWireRelay implements AutoCloseable {
    // One thread is permanently reserved for accept; each admitted connection owns one forward thread.
    private static final int MAX_CONNECTIONS = 31;
    private static final long CLOSE_TIMEOUT_SECONDS = 40;
    private final ServerSocket listener = new ServerSocket();
    private final ExecutorService threads = ManagedExecutorFactory.newBoundedThreadPool(
            "process-result-wire", MAX_CONNECTIONS + 1, MAX_CONNECTIONS);
    private final ExecutorService reverseThreads = ManagedExecutorFactory.newBoundedThreadPool(
            "process-result-wire-reverse", MAX_CONNECTIONS, MAX_CONNECTIONS);
    private final Semaphore connectionSlots = new Semaphore(MAX_CONNECTIONS);
    private final CopyOnWriteArrayList<Socket> connections = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final Supplier<InetSocketAddress> target;
    private final CompletableFuture<Void> acceptExited = new CompletableFuture<>();

    ProcessResultWireRelay(Supplier<InetSocketAddress> target) throws IOException {
        this.target = Objects.requireNonNull(target);
        listener.bind(new InetSocketAddress("0.0.0.0", 0));
        threads.execute(this::accept);
    }

    int port() { return listener.getLocalPort(); }
    List<Throwable> forwardingFailures() { return List.copyOf(failures); }

    private void accept() {
        try {
            while (!closing.get()) {
                Socket source = listener.accept();
                if (closing.get()) {
                    source.close();
                    break;
                }
                if (!connectionSlots.tryAcquire()) {
                    source.close();
                    failures.add(new RejectedExecutionException("Result relay connection limit reached"));
                    continue;
                }
                connections.add(source);
                try { threads.execute(() -> connect(source)); }
                catch (RuntimeException rejected) {
                    try { source.close(); } catch (IOException cleanupFailure) { rejected.addSuppressed(cleanupFailure); }
                    connections.remove(source);
                    connectionSlots.release();
                    throw rejected;
                }
            }
            acceptExited.complete(null);
        } catch (IOException failure) {
            if (closing.get() && failure instanceof SocketException) acceptExited.complete(null);
            else {
                failures.add(failure);
                acceptExited.completeExceptionally(failure);
            }
        } catch (RuntimeException failure) {
            failures.add(failure);
            acceptExited.completeExceptionally(failure);
        }
    }

    private void connect(Socket source) {
        Socket destination = new Socket();
        CompletableFuture<Void> returning = null;
        connections.add(destination);
        try (source; destination) {
            destination.connect(Objects.requireNonNull(target.get()), 5_000);
            returning = CompletableFuture.runAsync(() -> copy(destination, source), reverseThreads);
            copy(source, destination);
            returning.join();
        } catch (IOException | RuntimeException failure) {
            if (!closing.get()) failures.add(failure);
        } finally {
            // A failed forward copy closes both sockets above; wait for the reverse owner before releasing capacity.
            if (returning != null && !returning.isDone()) {
                try { returning.join(); }
                catch (RuntimeException failure) { if (!closing.get()) failures.add(failure); }
            }
            connections.remove(source);
            connections.remove(destination);
            connectionSlots.release();
        }
    }

    private void copy(Socket source, Socket destination) {
        try {
            source.getInputStream().transferTo(destination.getOutputStream());
            destination.shutdownOutput();
        } catch (IOException failure) {
            if (!closing.get()) {
                failures.add(failure);
                throw new java.io.UncheckedIOException(failure);
            }
        }
    }

    @Override public void close() throws Exception {
        boolean[] interrupted = {Thread.interrupted()};
        try {
            closing.set(true);
            Exception failure = null;
            try { listener.close(); } catch (IOException closeFailure) { failure = closeFailure; }
            try { awaitAccept(interrupted); }
            catch (Exception cleanupFailure) { failure = addFailure(failure, cleanupFailure); }
            for (Socket connection : connections) {
                try { connection.close(); }
                catch (IOException closeFailure) { failure = addFailure(failure, closeFailure); }
            }
            threads.shutdown();
            reverseThreads.shutdown();
            if (!awaitTermination(threads, interrupted)) {
                failure = addFailure(failure, new IllegalStateException("Result relay forward owner is still live"));
            }
            if (!awaitTermination(reverseThreads, interrupted)) {
                failure = addFailure(failure, new IllegalStateException("Result relay reverse owner is still live"));
            }
            if (!connections.isEmpty() || connectionSlots.availablePermits() != MAX_CONNECTIONS) {
                failure = addFailure(failure, new IllegalStateException("Result relay sockets or slots remain owned"));
            }
            if (failure != null) throw failure;
        } finally {
            if (interrupted[0]) Thread.currentThread().interrupt();
        }
    }

    private void awaitAccept(boolean[] interrupted) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLOSE_TIMEOUT_SECONDS);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Result relay accept owner is still live");
            try {
                acceptExited.get(remaining, TimeUnit.NANOSECONDS);
                return;
            } catch (InterruptedException failure) {
                interrupted[0] = true;
            }
        }
    }

    private static boolean awaitTermination(ExecutorService owner, boolean[] interrupted) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLOSE_TIMEOUT_SECONDS);
        while (!owner.isTerminated()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return false;
            try { owner.awaitTermination(remaining, TimeUnit.NANOSECONDS); }
            catch (InterruptedException failure) { interrupted[0] = true; }
        }
        return true;
    }

    private static Exception addFailure(Exception accumulated, Exception failure) {
        if (accumulated == null) return failure;
        accumulated.addSuppressed(failure);
        return accumulated;
    }
}
