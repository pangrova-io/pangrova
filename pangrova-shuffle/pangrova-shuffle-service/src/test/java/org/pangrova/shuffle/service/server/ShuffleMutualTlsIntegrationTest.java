package org.pangrova.shuffle.service.server;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.service.server.CoordinatorPeerInterceptor;
import org.pangrova.shuffle.service.config.ShuffleServiceConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShuffleMutualTlsIntegrationTest {

    private static final String AUTHORIZED_IDENTITY = "localhost";

    @Test
    void allowsPrivilegedRpcFromAuthorizedCertificateSan() throws Exception {
        try (Harness harness = start(List.of(AUTHORIZED_IDENTITY), true)) {
            RegisterShuffleResponse response = harness.stub().registerShuffle(
                    RegisterShuffleRequest.getDefaultInstance());

            assertThat(response.getShuffleId()).isEqualTo("authorized");
        }
    }

    @Test
    void rejectsPrivilegedRpcFromUnlistedCertificateSan() throws Exception {
        try (Harness harness = start(List.of("spiffe://pangrova/coordinator"), true)) {
            assertThatThrownBy(() -> harness.stub().registerShuffle(
                    RegisterShuffleRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                            assertThat(failure.getStatus().getCode())
                                    .isEqualTo(Status.Code.PERMISSION_DENIED));
        }
    }

    @Test
    void rejectsClientWithoutCertificateAtHandshake() throws Exception {
        try (Harness harness = start(List.of(AUTHORIZED_IDENTITY), false)) {
            assertThatThrownBy(() -> harness.stub().registerShuffle(
                    RegisterShuffleRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                            assertThat(failure.getStatus().getCode())
                                    .isIn(Status.Code.UNAVAILABLE, Status.Code.UNAUTHENTICATED));
        }
    }

    private static Harness start(List<String> identities, boolean presentClientCertificate)
            throws Exception {
        Path certificate = resource("tls/localhost-cert.pem");
        Path key = resource("tls/localhost-key.pem");
        var security = new ShuffleServiceConfig.Security(
                certificate, key, certificate, key, identities, Duration.ofMinutes(1));
        Server server = NettyServerBuilder.forPort(0)
                .sslContext(ShuffleServerRuntime.serverSslContext(security))
                .directExecutor().addService(new RecordingShuffleEndpoint())
                .intercept(new CoordinatorPeerInterceptor(identities)).build().start();
        var clientTls = GrpcSslContexts.forClient().trustManager(certificate.toFile());
        if (presentClientCertificate) {
            clientTls.keyManager(certificate.toFile(), key.toFile());
        }
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .overrideAuthority("localhost").sslContext(clientTls.build()).build();
        return new Harness(server, channel, ShuffleServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, TimeUnit.SECONDS));
    }

    private static Path resource(String name) throws URISyntaxException {
        return Path.of(java.util.Objects.requireNonNull(
                ShuffleMutualTlsIntegrationTest.class.getClassLoader().getResource(name)).toURI());
    }

    private static final class RecordingShuffleEndpoint
            extends ShuffleServiceGrpc.ShuffleServiceImplBase {
        @Override
        public void registerShuffle(
                RegisterShuffleRequest request,
                StreamObserver<RegisterShuffleResponse> response) {
            response.onNext(RegisterShuffleResponse.newBuilder()
                    .setShuffleId("authorized").build());
            response.onCompleted();
        }
    }

    private record Harness(
            Server server,
            ManagedChannel channel,
            ShuffleServiceGrpc.ShuffleServiceBlockingStub stub) implements AutoCloseable {
        @Override
        public void close() throws InterruptedException {
            channel.shutdownNow();
            server.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
