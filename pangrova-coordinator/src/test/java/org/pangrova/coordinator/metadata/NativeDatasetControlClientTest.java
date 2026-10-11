package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.metadata.nativecontrol.NativeDatasetControlRpcException;

import org.junit.jupiter.api.Test;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;

import java.net.HttpURLConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeDatasetControlClientTest {
    @Test
    void completeResponsePreservesBytesAcrossChunks() {
        var body = new NativeDatasetControlClient.BoundedResponse();
        body.onSubscribe(new Subscription());
        body.onNext(List.of(ByteBuffer.wrap(new byte[] {1, 2}), ByteBuffer.wrap(new byte[] {3})));
        body.onComplete();
        assertThat(body.getBody().toCompletableFuture().join()).containsExactly(1, 2, 3);
    }

    @Test
    void fullCanonicalEnvelopePreservesItsExactBytesAndCancelsTheNextByte() {
        var body = new NativeDatasetControlClient.BoundedResponse();
        var subscription = new Subscription(); body.onSubscribe(subscription);
        byte[] first = new byte[8 * 1024 * 1024]; first[0] = 17;
        byte[] second = new byte[NativeDatasetControlClient.MAX_RESPONSE_BYTES - first.length]; second[second.length - 1] = 23;
        body.onNext(List.of(ByteBuffer.wrap(first), ByteBuffer.wrap(second)));
        assertThat(subscription.cancelled).isFalse(); body.onComplete();
        byte[] actual = body.getBody().toCompletableFuture().join();
        assertThat(actual).hasSize(NativeDatasetControlClient.MAX_RESPONSE_BYTES);
        assertThat(actual[0]).isEqualTo((byte) 17); assertThat(actual[actual.length - 1]).isEqualTo((byte) 23);
        var over = new NativeDatasetControlClient.BoundedResponse();
        var overSubscription = new Subscription(); over.onSubscribe(overSubscription);
        over.onNext(List.of(ByteBuffer.wrap(first), ByteBuffer.wrap(second), ByteBuffer.wrap(new byte[] {1})));
        assertThat(overSubscription.cancelled).isTrue();
        assertThatThrownBy(() -> over.getBody().toCompletableFuture().join()).hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void oversizedResponseCancelsBeforeBuffering() {
        var body = new NativeDatasetControlClient.BoundedResponse();
        var subscription = new Subscription();
        body.onSubscribe(subscription);
        body.onNext(List.of(ByteBuffer.allocate(NativeDatasetControlClient.MAX_RESPONSE_BYTES + 1)));
        assertThat(subscription.cancelled).isTrue();
        assertThatThrownBy(() -> body.getBody().toCompletableFuture().join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void transportFailureIsPreserved() {
        var body = new NativeDatasetControlClient.BoundedResponse();
        var error = new java.io.IOException("connection lost");
        body.onError(error);
        assertThatThrownBy(() -> body.getBody().toCompletableFuture().join()).hasCause(error);
    }

    @Test
    void authenticationDenialNeverBecomesPublicationUncertainty() {
        for (int status : List.of(HttpURLConnection.HTTP_UNAUTHORIZED, HttpURLConnection.HTTP_FORBIDDEN)) {
            assertThatThrownBy(() -> NativeDatasetControlClient.decodeResponse(status, new byte[] {1}))
                    .isInstanceOf(SecurityException.class);
        }
        assertThatThrownBy(() -> NativeDatasetControlClient.decodeResponse(HttpURLConnection.HTTP_OK,
                error(ErrorClass.ERROR_CLASS_POLICY_DENIED))).isInstanceOf(SecurityException.class);
    }

    @Test
    void onlyAuthoritativeRejectionIsDefinitive() {
        for (ErrorClass errorClass : List.of(ErrorClass.ERROR_CLASS_INVALID_REQUEST,
                ErrorClass.ERROR_CLASS_PROTOCOL_MISMATCH, ErrorClass.ERROR_CLASS_CAPABILITY_MISMATCH,
                ErrorClass.ERROR_CLASS_ADMISSION)) {
            assertThatThrownBy(() -> NativeDatasetControlClient.decodeResponse(HttpURLConnection.HTTP_BAD_REQUEST,
                    error(errorClass))).isInstanceOf(IllegalStateException.class);
        }
        for (ErrorClass errorClass : List.of(ErrorClass.ERROR_CLASS_INTERNAL, ErrorClass.ERROR_CLASS_DEADLINE,
                ErrorClass.ERROR_CLASS_PROVIDER_OUTCOME_UNCERTAIN)) {
            assertThatThrownBy(() -> NativeDatasetControlClient.decodeResponse(HttpURLConnection.HTTP_INTERNAL_ERROR,
                    error(errorClass))).isInstanceOf(NativeDatasetControlRpcException.class);
        }
        assertThatThrownBy(() -> NativeDatasetControlClient.decodeResponse(HttpURLConnection.HTTP_OK, new byte[] {1}))
                .isInstanceOf(NativeDatasetControlRpcException.class);
    }

    @Test
    void loopbackUsesCanonicalBinaryCommandAndClosesSharedClient() throws Exception {
        var received = new AtomicReference<RuntimeCommand>();
        var credential = new AtomicReference<String>();
        var server = new Server(new InetSocketAddress("127.0.0.1", 0));
        byte[] response = RuntimeCommandResponse.newBuilder().setSuccess(true).build().toByteArray();
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse result)
                    throws IOException {
                received.set(RuntimeCommand.parseFrom(request.getInputStream().readAllBytes()));
                credential.set(request.getHeader("X-Internal-Auth"));
                result.setStatus(HttpURLConnection.HTTP_OK);
                result.getOutputStream().write(response);
                base.setHandled(true);
            }
        });
        server.start();
        try (var client = new NativeDatasetControlClient("credential")) {
            var endpoint = URI.create("http://127.0.0.1:" + server.getURI().getPort() + "/control");
            var command = RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                    .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT).build();
            assertThat(client.call(endpoint, command, System.currentTimeMillis() + 10_000).getSuccess()).isTrue();
            assertThat(received.get()).isEqualTo(command);
            assertThat(credential.get()).isEqualTo("credential");
            client.close();
            client.close();
            assertThatThrownBy(() -> client.call(endpoint, command, System.currentTimeMillis() + 10_000))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        } finally {
            server.stop();
        }
    }

    private static byte[] error(ErrorClass errorClass) {
        return RuntimeCommandResponse.newBuilder().setError(ExecutionError.newBuilder()
                .setErrorClass(errorClass).setMessage("denied")).build().toByteArray();
    }

    @Test
    void loopbackAuthenticationRejectionAndNativeFailureHaveDistinctOutcomes() throws Exception {
        var status = new AtomicInteger(HttpURLConnection.HTTP_UNAUTHORIZED);
        var server = new Server(new InetSocketAddress("127.0.0.1", 0));
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse result)
                    throws IOException {
                request.getInputStream().readAllBytes();
                result.setStatus(status.get());
                result.getOutputStream().write(error(ErrorClass.ERROR_CLASS_INTERNAL));
                base.setHandled(true);
            }
        });
        server.start();
        try (var client = new NativeDatasetControlClient("invalid-credential")) {
            var endpoint = URI.create("http://127.0.0.1:" + server.getURI().getPort() + "/control");
            var command = RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion()).build();
            assertThatThrownBy(() -> client.call(endpoint, command, System.currentTimeMillis() + 10_000))
                    .isInstanceOf(SecurityException.class);
            status.set(HttpURLConnection.HTTP_FORBIDDEN);
            assertThatThrownBy(() -> client.call(endpoint, command, System.currentTimeMillis() + 10_000))
                    .isInstanceOf(SecurityException.class);
            status.set(HttpURLConnection.HTTP_OK);
            assertThatThrownBy(() -> client.call(endpoint, command, System.currentTimeMillis() + 10_000))
                    .isInstanceOf(NativeDatasetControlRpcException.class);
        } finally {
            server.stop();
        }
    }

    @Test
    void closeCancelsAnAlreadyDispatchedRequestWithoutInventingPublicationEvidence() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var server = new Server(new InetSocketAddress("127.0.0.1", 0));
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse result)
                    throws IOException {
                request.getInputStream().readAllBytes();
                entered.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                }
                result.setStatus(HttpURLConnection.HTTP_OK);
                base.setHandled(true);
            }
        });
        var caller = ManagedExecutorFactory.newBoundedThreadPool("native-control-close-test", 1, 1);
        server.start();
        try (var client = new NativeDatasetControlClient("credential")) {
            var endpoint = URI.create("http://127.0.0.1:" + server.getURI().getPort() + "/control");
            var pending = caller.submit(() -> client.call(endpoint, RuntimeCommand.getDefaultInstance(),
                    System.currentTimeMillis() + 30_000));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            client.close();
            assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(NativeDatasetControlRpcException.class);
            assertThatThrownBy(() -> client.call(endpoint, RuntimeCommand.getDefaultInstance(),
                    System.currentTimeMillis() + 30_000)).hasMessageContaining("closed");
        } finally {
            release.countDown();
            caller.shutdownNow();
            server.stop();
        }
    }

    private static final class Subscription implements Flow.Subscription {

        boolean cancelled;

        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
