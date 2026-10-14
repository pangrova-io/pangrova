package org.pangrova.testing.e2e.shuffle;

import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import org.pangrova.protocol.v2.shuffle.ReadBatchResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.protocol.v2.shuffle.WriteBatchResponse;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-only mTLS wire forwarding/observation. Normal frames are unchanged and every
 * business response originates at the production ShuffleService. Faults act only on
 * an actual frame or flow-control boundary; they never authorize or commit anything.
 */
final class ShuffleWireRelay implements AutoCloseable {
    final List<Frame> requests = new CopyOnWriteArrayList<>();
    final List<Frame> responses = new CopyOnWriteArrayList<>();
    final List<String> opened = new CopyOnWriteArrayList<>();
    final List<String> cancelled = new CopyOnWriteArrayList<>();
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    volatile boolean pauseWriteInput;
    volatile boolean pauseReadOutput;
    volatile boolean pauseWriteResponse;
    volatile boolean truncateBeforeReadTerminal;
    volatile boolean corruptWriteCoverage;
    private final Server server;

    ShuffleWireRelay(ActualShuffleBackend backend, ManagedChannel upstream) throws Exception {
        var definition = ServerServiceDefinition.builder(ShuffleServiceGrpc.getServiceDescriptor());
        for (var method : ShuffleServiceGrpc.getServiceDescriptor().getMethods()) {
            addForwardedMethod(definition, method, upstream);
        }
        var ssl = GrpcSslContexts.forServer(backend.certificate().toFile(), backend.privateKey().toFile())
                .trustManager(backend.trustedPeers().toFile()).clientAuth(ClientAuth.REQUIRE).build();
        server = NettyServerBuilder.forPort(0).sslContext(ssl)
                .maxInboundMessageSize(2 * 1024 * 1024).addService(definition.build()).build().start();
    }

    int port() { return server.getPort(); }

    private <Request, Response> void addForwardedMethod(ServerServiceDefinition.Builder builder,
            MethodDescriptor<Request, Response> method, ManagedChannel upstream) {
        builder.addMethod(method, (downstream, headers) -> {
            opened.add(method.getBareMethodName());
            var options = CallOptions.DEFAULT;
            if (Context.current().getDeadline() != null) options = options.withDeadline(Context.current().getDeadline());
            var client = upstream.newCall(method, options);
            var done = new AtomicBoolean();
            client.start(responseListener(method, downstream, client, done), headers);
            client.request(1);
            downstream.request(1);
            return requestListener(method, downstream, client, done);
        });
    }

    private <Request, Response> ClientCall.Listener<Response> responseListener(
            MethodDescriptor<Request, Response> method, ServerCall<Request, Response> downstream,
            ClientCall<Request, Response> client, AtomicBoolean done) {
        return new ClientCall.Listener<>() {
            @Override public void onHeaders(Metadata headers) { downstream.sendHeaders(headers); }
            @Override public void onMessage(Response message) {
                observe(responses, method, message);
                if (truncateBeforeReadTerminal && message instanceof ReadBatchResponse frame && frame.hasTerminal()) {
                    if (done.compareAndSet(false, true)) downstream.close(Status.OK, new Metadata());
                    client.cancel("wire fault drops the actual terminal frame", null);
                    return;
                }
                if (((pauseReadOutput && message instanceof ReadBatchResponse)
                        || (pauseWriteResponse && message instanceof WriteBatchResponse))
                        && !awaitResume(done, client)) return;
                downstream.sendMessage(tamperActualResponse(method, message));
                client.request(1);
            }
            @Override public void onClose(Status status, Metadata trailers) {
                if (done.compareAndSet(false, true)) downstream.close(status, trailers);
            }
        };
    }

    private <Request, Response> ServerCall.Listener<Request> requestListener(
            MethodDescriptor<Request, Response> method, ServerCall<Request, Response> downstream,
            ClientCall<Request, Response> client, AtomicBoolean done) {
        return new ServerCall.Listener<>() {
            @Override public void onMessage(Request message) {
                observe(requests, method, message);
                if (pauseWriteInput && method.getBareMethodName().equals("WriteBatch")
                        && !awaitResume(done, client)) return;
                client.sendMessage(message);
                downstream.request(1);
            }
            @Override public void onHalfClose() { client.halfClose(); }
            @Override public void onCancel() {
                cancelled.add(method.getBareMethodName());
                done.set(true);
                resume.countDown();
                client.cancel("actual downstream RPC cancellation", null);
            }
        };
    }

    private boolean awaitResume(AtomicBoolean done, ClientCall<?, ?> client) {
        paused.countDown();
        try {
            if (!resume.await(10, TimeUnit.SECONDS)) {
                client.cancel("bounded wire pause expired", null);
                return false;
            }
            return !done.get();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            client.cancel("wire pause interrupted", failure);
            return false;
        }
    }

    private <Request, Response> Response tamperActualResponse(
            MethodDescriptor<Request, Response> method, Response message) {
        if (corruptWriteCoverage && message instanceof WriteBatchResponse response) {
            // Fault injection starts with the real persisted ACK, not a synthesized success.
            byte[] corrupted = response.toBuilder().clearPartitions().build().toByteArray();
            return method.getResponseMarshaller().parse(new ByteArrayInputStream(corrupted));
        }
        return message;
    }

    private static void observe(List<Frame> target, MethodDescriptor<?, ?> method, Object message) {
        if (!(message instanceof Message actual)) throw new IllegalStateException("Non-protobuf actual wire frame");
        if (target.size() >= 1024) throw new IllegalStateException("Acceptance wire observation exceeded its bound");
        target.add(new Frame(method.getBareMethodName(), actual));
    }

    @Override public void close() throws Exception {
        resume.countDown();
        server.shutdownNow();
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Actual wire relay RPCs did not terminate");
        }
    }

    record Frame(String method, Message message) {}
}
