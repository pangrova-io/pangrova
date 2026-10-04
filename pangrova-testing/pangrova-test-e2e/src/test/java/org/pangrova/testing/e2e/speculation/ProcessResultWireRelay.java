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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Opaque duplex TCP forwarding; TLS, tickets and result bytes are never decoded or rewritten. */
@ThreadSafe
final class ProcessResultWireRelay implements AutoCloseable {
    private final ServerSocket listener = new ServerSocket();
    private final ExecutorService threads = ManagedExecutorFactory.newBoundedThreadPool("process-result-wire", 32, 64);
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
                connections.add(source);
                try { threads.execute(() -> connect(source)); }
                catch (RuntimeException rejected) {
                    try { source.close(); } catch (IOException cleanupFailure) { rejected.addSuppressed(cleanupFailure); }
                    connections.remove(source);
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
        try (source; Socket destination = new Socket()) {
            connections.add(destination);
            destination.connect(Objects.requireNonNull(target.get()), 5_000);
            CompletableFuture<Void> returning = CompletableFuture.runAsync(() -> copy(destination, source), threads);
            copy(source, destination);
            returning.join();
        } catch (IOException | RuntimeException failure) {
            if (!closing.get()) failures.add(failure);
        } finally {
            connections.removeIf(Socket::isClosed);
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
        closing.set(true);
        Exception failure = null;
        try { listener.close(); } catch (IOException closeFailure) { failure = closeFailure; }
        for (Socket connection : connections) {
            try { connection.close(); }
            catch (IOException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
        }
        threads.shutdown();
        try {
            acceptExited.get(40, TimeUnit.SECONDS);
        } catch (Exception cleanupFailure) {
            if (cleanupFailure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (failure == null) failure = cleanupFailure;
            else failure.addSuppressed(cleanupFailure);
        }
        try {
            if (!threads.awaitTermination(40, TimeUnit.SECONDS)) throw new IllegalStateException("Actual result wire forwarding is still live");
        } catch (Exception cleanupFailure) {
            if (cleanupFailure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (failure == null) failure = cleanupFailure;
            else failure.addSuppressed(cleanupFailure);
        }
        if (failure != null) throw failure;
    }
}
