package org.pangrova.transport.client;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.client.protocol.ResultProtocol;
import org.pangrova.transport.client.security.ResultTransportSecurity;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual official result-server TLS handshakes and EOF; not JNI or SQL execution evidence. */
class FlightResultFetcherMutualTlsTest {
    @TempDir private static Path workspace;
    private static TestTlsAuthority authority;
    private static TestTlsAuthority unrelatedAuthority;
    private static TestTlsAuthority.Identity serverIdentity;
    private static TestTlsAuthority.Identity clientIdentity;
    private static TestTlsAuthority.Identity expiredIdentity;

    @BeforeAll
    static void createActualCertificateAuthority() throws Exception {
        try {
            authority = new TestTlsAuthority(workspace);
            serverIdentity = authority.issue("server", "localhost", false);
            clientIdentity = authority.issue("client", "localhost", false);
            expiredIdentity = authority.issue("expired-server", "localhost", true);
            unrelatedAuthority = new TestTlsAuthority(workspace);
        } catch (Exception | Error failure) {
            try { closeActualCertificateAuthority(); } catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    @AfterAll
    static void closeActualCertificateAuthority() throws Exception {
        Exception failure = null;
        if (unrelatedAuthority != null) {
            try { unrelatedAuthority.close(); } catch (Exception cleanupFailure) { failure = cleanupFailure; }
        }
        if (authority != null) {
            try { authority.close(); }
            catch (Exception cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure != null) throw failure;
    }
    @Test
    void mutualTlsDeliversActualRowsAndEofWithExactAdvertisedHostname() throws Exception {
        try (var server = new ServerOwner(true);
             var fetcher = new FlightResultFetcher(mutualSecurity())) {
            var decoded = ArrowBatchDecoder.decode(fetcher.fetch(server.endpoint("localhost"), deadline()));
            assertThat(decoded.rows()).singleElement().satisfies(row -> assertThat(row).containsExactly(42));
            assertThat(server.delivered()).isEqualTo(1);
        }
    }

    @Test
    void trustedTlsDescriptorIsNotDowngradedWhenCallerDidNotRequireTls() throws Exception {
        try (var server = new ServerOwner(false);
             var fetcher = new FlightResultFetcher(new ResultTransportSecurity(false, authority.root(), null, null))) {
            var decoded = ArrowBatchDecoder.decode(fetcher.fetch(server.endpoint("localhost"), deadline()));
            assertThat(decoded.rows()).singleElement().satisfies(row -> assertThat(row).containsExactly(42));
            assertThat(server.delivered()).isEqualTo(1);
        }
    }

    @Test
    void mutualTlsRejectsClientWithoutCertificateBeforeServingRows() throws Exception {
        try (var server = new ServerOwner(true);
             var fetcher = new FlightResultFetcher(new ResultTransportSecurity(true, authority.root(), null, null))) {
            assertThatThrownBy(() -> fetcher.fetch(server.endpoint("localhost"), deadline()))
                    .isInstanceOf(TransportConnectionException.class);
            assertThat(server.delivered()).isZero();
        }
    }

    @Test
    void unrelatedTrustRootCannotAuthenticateActualServer() throws Exception {
        try (var server = new ServerOwner(true);
             var fetcher = new FlightResultFetcher(new ResultTransportSecurity(true, unrelatedAuthority.root(),
                     clientIdentity.certificateChain(), clientIdentity.privateKey()))) {
            assertThatThrownBy(() -> fetcher.fetch(server.endpoint("localhost"), deadline()))
                    .isInstanceOf(TransportConnectionException.class);
            assertThat(server.delivered()).isZero();
        }
    }

    @Test
    void trustedCertificateCannotBypassActualEndpointHostnameVerification() throws Exception {
        try (var server = new ServerOwner(true);
             var fetcher = new FlightResultFetcher(mutualSecurity())) {
            assertThatThrownBy(() -> fetcher.fetch(server.endpoint("127.0.0.1"), deadline()))
                    .isInstanceOf(TransportConnectionException.class);
            assertThat(server.delivered()).isZero();
        }
    }

    @Test
    void validTrustAnchorRejectsActuallyExpiredCaSignedServerLeafBeforeServingRows() throws Exception {
        try (var server = new ServerOwner(false, expiredIdentity);
             var fetcher = new FlightResultFetcher(mutualSecurity())) {
            assertThatThrownBy(() -> fetcher.fetch(server.endpoint("localhost"), deadline()))
                    .isInstanceOf(TransportConnectionException.class);
            assertThat(server.delivered()).isZero();
        }
    }

    @Test
    void actualManagedChannelTerminatesBeforeItsSdkAllocatorIsReleased() throws Exception {
        try (var server = new ServerOwner(true);
             var allocator = new RootAllocator(32L * 1024 * 1024)) {
            var retained = new java.util.concurrent.atomic.AtomicReference<ResultTransportClientOwner>();
            var owner = ResultTransportClientProvider.open(allocator, Location.forGrpcTls("localhost", server.server.getPort()),
                    mutualSecurity(), retained::set);
            try (owner) {
                assertThat(retained.get()).isSameAs(owner);
                try (var stream = owner.client().getStream(new Ticket(new byte[0]))) {
                    assertThat(stream.next()).isTrue();
                    assertThat(stream.getRoot().getVector(0).getObject(0)).isEqualTo(42);
                    assertThat(stream.next()).isFalse();
                }
                assertThat(owner.channelTerminated()).isFalse();
                assertThat(allocator.getChildAllocators()).hasSize(1);
                owner.close();
                assertThat(owner.channelTerminated()).isTrue();
                assertThat(allocator.getChildAllocators()).isEmpty();
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    @Test
    void closeRetainsActualStreamOwnerAndSealsNewAdmissionUntilEofThenRetry() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = ManagedExecutorFactory.newSingleThreadExecutor("result-tls-owned-consumer");
        try (var server = new ServerOwner(true);
             var fetcher = new FlightResultFetcher(mutualSecurity())) {
            var result = executor.submit(() -> fetcher.fetchStream(server.endpoint("localhost"), bytes -> {
                assertThat(ArrowBatchDecoder.decode(bytes).rows()).singleElement()
                        .satisfies(row -> assertThat(row).containsExactly(42));
                entered.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(fetcher::close).isInstanceOf(TransportConnectionException.class)
                        .hasMessageContaining("still owned");
                assertThatThrownBy(() -> fetcher.fetch(server.endpoint("localhost"), deadline()))
                        .isInstanceOf(TransportConnectionException.class).hasMessageContaining("admission is closed");
            } finally { release.countDown(); }
            result.get(5, TimeUnit.SECONDS);
            fetcher.close();
            assertThat(server.delivered()).isEqualTo(1);
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void interruptedCloseIsVisibleAndCanBeRetriedWithoutReopeningAdmission() {
        var fetcher = new FlightResultFetcher(ResultTransportSecurity.systemTrust(false));
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(fetcher::close).isInstanceOf(TransportConnectionException.class)
                    .hasMessageContaining("interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            fetcher.close();
        }
    }

    private static ResultTransportSecurity mutualSecurity() {
        return new ResultTransportSecurity(true, authority.root(),
                clientIdentity.certificateChain(), clientIdentity.privateKey());
    }

    private static long deadline() { return System.currentTimeMillis() + 5_000; }

    private static final class ServerOwner implements AutoCloseable {
        private final RootAllocator allocator = new RootAllocator(32L * 1024 * 1024);
        private final AtomicInteger delivered = new AtomicInteger();
        private final FlightServer server;

        ServerOwner(boolean mutual) throws Exception { this(mutual, serverIdentity); }

        ServerOwner(boolean mutual, TestTlsAuthority.Identity identity) throws Exception {
            FlightServer candidate = null;
            try {
                var builder = FlightServer.builder(allocator, Location.forGrpcTls("0.0.0.0", 0),
                        new NoOpFlightProducer() {
                            @Override public void getStream(FlightProducer.CallContext context, Ticket ticket,
                                    FlightProducer.ServerStreamListener listener) {
                                produce(listener);
                            }
                        }).useTls(identity.certificateChain().toFile(), identity.privateKey().toFile());
                if (mutual) builder.useMTlsClientVerification(authority.root().toFile());
                candidate = builder.build();
                server = candidate.start();
            } catch (Exception | Error failure) {
                if (candidate != null) {
                    try { candidate.close(); }
                    catch (Exception cleanupFailure) {
                        if (cleanupFailure instanceof InterruptedException) Thread.currentThread().interrupt();
                        failure.addSuppressed(cleanupFailure);
                    }
                }
                try { allocator.close(); } catch (RuntimeException cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                throw failure;
            }
        }

        private void produce(FlightProducer.ServerStreamListener listener) {
            delivered.incrementAndGet();
            var value = new IntVector("value", allocator);
            try (var root = VectorSchemaRoot.of(value)) {
                value.allocateNew(1);
                value.setSafe(0, 42);
                root.setRowCount(1);
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
        }

        ResultEndpoint endpoint(String hostname) {
            return new ResultEndpoint(hostname, server.getPort(), "actual-tls-component", ResultProtocol.GRPC_TLS.value());
        }

        int delivered() { return delivered.get(); }

        @Override public void close() throws Exception {
            Exception failure = null;
            try { server.close(); }
            catch (Exception closeFailure) {
                if (closeFailure instanceof InterruptedException) Thread.currentThread().interrupt();
                failure = closeFailure;
            }
            try { allocator.close(); }
            catch (RuntimeException cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
            if (failure != null) throw failure;
        }
    }
}
