package org.pangrova.connector.lance;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Scoped, authenticated UNIX actor which runs the real fixed SDK preparation command. */
final class LanceSdkPreparationActor implements AutoCloseable {
    private static final int MAX_FRAME = 32 * 1024 * 1024;
    private static final int MAX_NONCE_BYTES = 128;
    private static final int PROBE = 1;
    private static final int PREPARE = 2;
    private final Path directory;
    private final Path socket;
    private final String nonce = UUID.randomUUID().toString() + UUID.randomUUID();
    private final ServerSocketChannel server;
    private final ExecutorService executor;
    private volatile SocketChannel active;
    private volatile byte[] probedRequest;
    private volatile Preparation preparation;

    LanceSdkPreparationActor() throws IOException {
        // Darwin UNIX paths are limited to 104 bytes; JUnit's temp directory can exceed that bound.
        directory = Files.createTempDirectory(Path.of("/tmp"), "lance-sdk-");
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        socket = directory.resolve("actor.sock");
        server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        try {
            server.bind(UnixDomainSocketAddress.of(socket));
            Files.setPosixFilePermissions(socket, PosixFilePermissions.fromString("rw-------"));
        } catch (IOException failure) {
            server.close();
            Files.deleteIfExists(socket);
            Files.delete(directory);
            throw failure;
        }
        executor = ManagedExecutorFactory.newSingleThreadExecutor("lance-sdk-preparation-actor");
        executor.execute(this::serve);
    }

    Path socket() { return socket; }
    String nonce() { return nonce; }

    static void requireListeningActor(Path socket, String nonce, PrepareDistributedWriteRequest bare)
            throws IOException {
        var actual = exchange(socket, nonce, PROBE, bare.toByteArray());
        var expected = ProtocolChecksums.sha256(bare.toByteArray()).getDigest().toByteArray();
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new IOException("Listening SDK actor did not confirm the exact preparation request");
        }
    }

    PrepareDistributedWriteResult prepare(PrepareDistributedWriteRequest issued, Preparation operation)
            throws IOException {
        if (issued.getInvocationId().isEmpty() || probedRequest == null
                || !MessageDigest.isEqual(probedRequest,
                        issued.toBuilder().clearInvocationId().build().toByteArray())) {
            throw new IOException("SDK actor requires the exact request probed before durable issuance");
        }
        preparation = java.util.Objects.requireNonNull(operation, "Actual SDK preparation");
        return PrepareDistributedWriteResult.parseFrom(exchange(socket, nonce, PREPARE, issued.toByteArray()));
    }

    private void serve() {
        while (server.isOpen()) {
            try (var connection = server.accept()) {
                active = connection;
                serve(connection);
            } catch (IOException failure) {
                if (server.isOpen()) {
                    System.err.println("Actual SDK fixture actor failed: " + failure);
                }
            } finally {
                active = null;
            }
        }
    }

    private void serve(SocketChannel connection) throws IOException {
        var input = new DataInputStream(Channels.newInputStream(connection));
        var output = new DataOutputStream(Channels.newOutputStream(connection));
        try {
            int operation = input.readInt();
            var suppliedNonce = readFrame(input, MAX_NONCE_BYTES);
            if (!MessageDigest.isEqual(nonce.getBytes(StandardCharsets.UTF_8), suppliedNonce)) {
                throw new IOException("Unknown SDK fixture actor client");
            }
            var bytes = readFrame(input, MAX_FRAME);
            var request = PrepareDistributedWriteRequest.parseFrom(bytes);
            byte[] result;
            if (operation == PROBE && request.getInvocationId().isEmpty()) {
                probedRequest = bytes;
                result = ProtocolChecksums.sha256(bytes).getDigest().toByteArray();
            } else if (operation == PREPARE && preparation != null && !request.getInvocationId().isEmpty()
                    && MessageDigest.isEqual(probedRequest,
                            request.toBuilder().clearInvocationId().build().toByteArray())) {
                result = preparation.execute(request).toByteArray();
            } else {
                throw new IOException("SDK actor request differs from its probed durable issuance");
            }
            output.writeInt(1);
            writeFrame(output, result);
        } catch (Exception failure) {
            output.writeInt(0);
            writeFrame(output, failure.toString().getBytes(StandardCharsets.UTF_8));
        }
        output.flush();
    }

    private static byte[] exchange(Path socket, String nonce, int operation, byte[] bytes) throws IOException {
        try (var connection = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            connection.connect(UnixDomainSocketAddress.of(socket));
            var input = new DataInputStream(Channels.newInputStream(connection));
            var output = new DataOutputStream(Channels.newOutputStream(connection));
            output.writeInt(operation);
            writeFrame(output, nonce.getBytes(StandardCharsets.UTF_8));
            writeFrame(output, bytes);
            output.flush();
            int success = input.readInt();
            var result = readFrame(input, MAX_FRAME);
            if (success != 1) {
                throw new IOException("Actual SDK actor rejected preparation: "
                        + new String(result, StandardCharsets.UTF_8));
            }
            return result;
        }
    }

    private static byte[] readFrame(DataInputStream input, int maximum) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum) {
            throw new IOException("SDK actor frame exceeds its bound");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Truncated SDK actor frame");
        }
        return bytes;
    }

    private static void writeFrame(DataOutputStream output, byte[] bytes) throws IOException {
        if (bytes.length == 0 || bytes.length > MAX_FRAME) {
            throw new IOException("SDK actor frame exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    @Override
    public void close() throws IOException {
        server.close();
        var connection = active;
        if (connection != null) {
            connection.close();
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IOException("Actual SDK fixture actor has not joined; its endpoint remains held");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted before actual SDK actor join", interrupted);
        }
        Files.deleteIfExists(socket);
        Files.delete(directory);
    }

    @FunctionalInterface
    interface Preparation {
        PrepareDistributedWriteResult execute(PrepareDistributedWriteRequest request) throws Exception;
    }
}
