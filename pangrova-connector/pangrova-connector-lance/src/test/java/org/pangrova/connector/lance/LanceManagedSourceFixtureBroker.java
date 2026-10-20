package org.pangrova.connector.lance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/** One real PG authority JVM shared by independently launched Native GoogleTest processes. */
public final class LanceManagedSourceFixtureBroker {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_FRAME = 32 * 1024 * 1024;
    private final LanceManagedSourceFixture fixture;
    private final String nonce;

    private LanceManagedSourceFixtureBroker(String nonce) {
        if (nonce == null || nonce.length() < 32) throw new IllegalArgumentException("Fixture nonce is required");
        this.nonce = nonce;
        fixture = new LanceManagedSourceFixture();
    }

    /** Starts after the connector-lance test classes and transitive metadata classes are compiled. */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Expected socket path and readiness file");
        var postgres = LanceCatalogTestDatabase.instance();
        Runtime.getRuntime().addShutdownHook(new Thread(postgres::stop, "lance-sdk-fixture-postgres-cleanup"));
        run(args);
    }

    private static void run(String[] args) throws Exception {
        var broker = new LanceManagedSourceFixtureBroker(System.getenv("PANGROVA_LANCE_FIXTURE_NONCE"));
        var socket = Path.of(args[0]);
        var ready = Path.of(args[1]);
        if (Files.exists(socket) || Files.exists(ready)) throw new IllegalStateException("Fixture endpoint already exists");
        var clients = ManagedExecutorFactory.newBoundedThreadPool("lance-sdk-authority", 4, 32);
        var renewal = ManagedExecutorFactory.newSingleThreadScheduledExecutor("lance-sdk-real-pg-renewal");
        try (var server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            server.bind(UnixDomainSocketAddress.of(socket));
            Files.setPosixFilePermissions(socket, PosixFilePermissions.fromString("rw-------"));
            renewal.scheduleWithFixedDelay(broker.fixture::renewOwned, 5, 5, TimeUnit.SECONDS);
            Files.writeString(ready, JSON.writeValueAsString(JSON.createObjectNode().put("socket", socket.toString())
                    .put("catalog", encode(broker.fixture.catalog().toByteArray()))));
            Files.setPosixFilePermissions(ready, PosixFilePermissions.fromString("rw-------"));
            while (!Thread.currentThread().isInterrupted()) {
                var connection = server.accept();
                try { clients.execute(() -> broker.serve(connection)); }
                catch (java.util.concurrent.RejectedExecutionException rejected) { connection.close(); }
            }
        } finally {
            renewal.shutdownNow();
            clients.shutdownNow();
            Files.deleteIfExists(ready);
            Files.deleteIfExists(socket);
        }
    }

    private void serve(SocketChannel connection) {
        try (connection;
             var input = new DataInputStream(Channels.newInputStream(connection));
             var output = new DataOutputStream(Channels.newOutputStream(connection))) {
            int length = input.readInt();
            if (length <= 0 || length > MAX_FRAME) throw new IOException("Fixture request frame exceeds its bound");
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) throw new IOException("Truncated fixture request");
            ObjectNode response;
            try {
                response = dispatch(JSON.readTree(bytes));
                response.put("success", true);
            } catch (Exception rejected) {
                response = JSON.createObjectNode().put("success", false).put("error", rejected.toString());
            }
            byte[] result = JSON.writeValueAsBytes(response);
            if (result.length > MAX_FRAME) throw new IOException("Fixture response exceeds its bound");
            output.writeInt(result.length);
            output.write(result);
            output.flush();
        } catch (IOException failure) {
            System.err.println("Lance PG fixture connection failed: " + failure);
        }
    }

    private ObjectNode dispatch(JsonNode request) throws Exception {
        if (!nonce.equals(request.required("nonce").textValue())) throw new SecurityException("Unknown fixture client");
        return switch (request.required("operation").textValue()) {
            case "catalog" -> JSON.createObjectNode().put("catalog", encode(fixture.catalog().toByteArray()));
            case "warehouse" -> JSON.createObjectNode().put("warehouse", fixture.warehouse().toString());
            case "allocate" -> allocate(request);
            case "allocation" -> JSON.createObjectNode().put("binding", encode(
                    fixture.allocatedBinding(request.required("execution").textValue()).toByteArray()));
            case "reserve" -> reserve(request);
            case "handoff" -> {
                var anchor = fixture.handoff(request.required("execution").textValue());
                yield JSON.createObjectNode().put("owner", encode(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(anchor.ownerId()).setOwnerIncarnation(anchor.ownerIncarnation())
                        .setFencingToken(anchor.fencingToken()).build().toByteArray()));
            }
            case "issue" -> JSON.createObjectNode().put("request", encode(fixture.issuePreparation(
                    org.pangrova.protocol.v2.PrepareDistributedWriteRequest.parseFrom(decode(request.required("request"))),
                    Path.of(request.required("actor_socket").textValue()), nonce).toByteArray()));
            case "cleanup" -> {
                var fragments = new ArrayList<byte[]>();
                for (var fragment : request.required("fragments")) fragments.add(decode(fragment));
                fixture.requireCleanupWinners(request.required("execution").textValue(), fragments);
                var selected = fixture.cleanupRequest(request.required("execution").textValue());
                var result = JSON.createObjectNode();
                selected.ifPresent(value -> result.put("request", encode(value.toByteArray())));
                yield result;
            }
            case "deleted" -> {
                fixture.completeCleanup(request.required("execution").textValue(),
                        org.pangrova.protocol.v2.CleanupSourceObjectsResult.parseFrom(decode(request.required("result"))));
                yield JSON.createObjectNode();
            }
            case "register" -> register(request);
            case "reference" -> JSON.createObjectNode().put("reference", encode(fixture.reference(
                    request.required("uri").textValue(), request.required("version").longValue()).toByteArray()));
            case "abort" -> {
                var receipt = fixture.abort(request.required("execution").textValue());
                yield JSON.createObjectNode().put("execution", receipt.executionId()).put("fingerprint", receipt.commitFingerprint());
            }
            default -> throw new IllegalArgumentException("Unknown fixture operation");
        };
    }

    private ObjectNode allocate(JsonNode request) {
        var allocation = fixture.allocate(request.required("uri").textValue(), request.required("writeId").textValue(),
                LanceWriteOperation.forNumber(request.required("writeOperation").intValue()),
                request.required("version").longValue(), decode(request.required("schemaSha256")),
                decode(request.required("schemaIpc")), request.required("partition").intValue());
        var result = JSON.createObjectNode().put("execution", allocation.executionId())
                .put("attempt", encode(allocation.attempt().toByteArray())).put("prefix", allocation.attemptPrefix())
                .put("binding", encode(allocation.binding().toByteArray()));
        allocation.output().acceptedWriteBaseSnapshot().ifPresent(reference -> result.put("base", encode(
                org.pangrova.protocol.SourceSnapshotBindings.encode(reference, fixture.catalog()).toByteArray())));
        return result;
    }

    private ObjectNode reserve(JsonNode request) throws Exception {
        var fragments = new ArrayList<byte[]>();
        var encoded = request.required("fragments");
        if (!encoded.isArray() || encoded.isEmpty() || encoded.size() > 4096) {
            throw new IllegalArgumentException("Fixture winners exceed their bound");
        }
        for (var fragment : encoded) fragments.add(decode(fragment));
        String execution = request.required("execution").textValue();
        fixture.recordIntent(execution, fragments);
        var accepted = fixture.accepted(execution);
        if (accepted.isPresent()) return JSON.createObjectNode().put("accepted", encode(accepted.orElseThrow().toByteArray()));
        return JSON.createObjectNode().put("request", encode(fixture.reserve(execution, fragments).toByteArray()));
    }

    private ObjectNode register(JsonNode request) throws Exception {
        var reference = fixture.register(request.required("execution").textValue(),
                PrepareDistributedWriteResult.parseFrom(decode(request.required("result"))));
        var result = JSON.createObjectNode();
        if (reference != null) result.put("accepted", encode(reference.toByteArray()));
        return result;
    }

    private static byte[] decode(JsonNode node) { return Base64.getDecoder().decode(node.textValue()); }
    private static String encode(byte[] bytes) { return Base64.getEncoder().encodeToString(bytes); }
}
