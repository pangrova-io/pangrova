package org.pangrova.testing.testkit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Transparent local PostgreSQL wire fault. All authentication, SQL and command results come from PostgreSQL. */
public final class PostgresCommitWireFault implements AutoCloseable {
    private final String host;
    private final int port;
    private final ServerSocket listener;
    private final Thread acceptor;
    private final List<Thread> pumps = new CopyOnWriteArrayList<>();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean armed = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> pumpFailure = new AtomicReference<>();
    private final CountDownLatch actualCommit = new CountDownLatch(1);
    private final CountDownLatch recovery = new CountDownLatch(1);

    public PostgresCommitWireFault(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        listener = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
        acceptor = new Thread(this::accept, "journal-real-pg-wire-accept");
        try {
            acceptor.start();
        } catch (RuntimeException | Error failure) {
            try {
                listener.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    public int port() { return listener.getLocalPort(); }

    public void armCommitResponseLoss() { armed.set(true); }

    public boolean awaitActualCommit() throws InterruptedException {
        return actualCommit.await(5, TimeUnit.SECONDS);
    }

    public void allowReconciliation() { recovery.countDown(); }

    private void accept() {
        try {
            while (!closed.get()) {
                Socket client = listener.accept();
                boolean accepted;
                synchronized (this) {
                    accepted = !closed.get();
                    if (accepted) {
                        sockets.add(client);
                        startPump(() -> connect(client), "journal-real-pg-connect");
                    }
                }
                if (!accepted) client.close();
            }
        } catch (IOException failure) {
            if (!closed.get()) pumpFailure.compareAndSet(null, failure);
        } catch (RuntimeException | Error failure) {
            pumpFailure.compareAndSet(null, failure);
        }
    }

    private void connect(Socket client) {
        Socket server = null;
        try {
            if (actualCommit.getCount() == 0 && !recovery.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual reconciliation connection was not released");
            }
            server = new Socket();
            synchronized (this) {
                if (closed.get()) return;
                sockets.add(server);
            }
            server.connect(new java.net.InetSocketAddress(host, port), 5000);
            Socket backend = server;
            startPump(() -> clientToServer(client, backend), "journal-real-pg-request");
            serverToClient(client, server);
        } catch (IOException failure) {
            if (!closed.get() && actualCommit.getCount() != 0) {
                throw new IllegalStateException("Actual PostgreSQL relay failed before the selected fault", failure);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted awaiting the real reconciliation connection", interrupted);
        } finally {
            closeSocket(client);
            if (server != null) closeSocket(server);
        }
    }

    private void clientToServer(Socket client, Socket server) {
        try {
            client.getInputStream().transferTo(server.getOutputStream());
        } catch (IOException failure) {
            if (!closed.get() && actualCommit.getCount() != 0) {
                throw new IllegalStateException("Actual PostgreSQL request relay failed", failure);
            }
        }
    }

    private void serverToClient(Socket client, Socket server) throws IOException {
        var input = new DataInputStream(server.getInputStream());
        var output = new DataOutputStream(client.getOutputStream());
        while (true) {
            int kind = input.read();
            if (kind < 0) return;
            int size = input.readInt();
            if (size < 4 || size > 16 * 1024 * 1024) throw new IOException("Invalid test PostgreSQL frame size");
            byte[] body = input.readNBytes(size - 4);
            if (body.length != size - 4) throw new IOException("Truncated actual PostgreSQL message");
            if (kind == 'C' && "COMMIT\0".equals(new String(body, StandardCharsets.UTF_8))
                    && armed.compareAndSet(true, false)) {
                // PostgreSQL's actual CommandComplete has arrived; none of its bytes are sent to the JDBC client.
                actualCommit.countDown();
                return;
            }
            output.writeByte(kind);
            output.writeInt(size);
            output.write(body);
            output.flush();
        }
    }

    private synchronized void startPump(Runnable operation, String name) {
        if (closed.get()) return;
        Thread thread = new Thread(() -> {
            try {
                operation.run();
            } catch (RuntimeException | Error failure) {
                pumpFailure.compareAndSet(null, failure);
            }
        }, name);
        pumps.add(thread);
        thread.start();
    }

    private void closeSocket(Socket socket) {
        try {
            socket.close();
            sockets.remove(socket);
        } catch (IOException failure) {
            throw new IllegalStateException("Actual relay socket close failed", failure);
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        synchronized (this) {
            closed.set(true);
        }
        recovery.countDown();
        listener.close();
        acceptor.join(5000);
        if (acceptor.isAlive()) throw new IllegalStateException("Actual relay acceptor did not exit");
        Throwable failure = null;
        for (Socket socket : List.copyOf(sockets)) {
            try {
                closeSocket(socket);
            } catch (RuntimeException error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        for (Thread pump : pumps) {
            pump.join(5000);
            if (pump.isAlive()) {
                var error = new IllegalStateException("Actual PostgreSQL relay pump has not exited");
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        if (pumpFailure.get() != null) {
            if (failure == null) {
                failure = pumpFailure.get();
            } else {
                failure.addSuppressed(pumpFailure.get());
            }
        }
        if (failure != null) throw new IllegalStateException("PostgreSQL relay still owns resources", failure);
    }
}
