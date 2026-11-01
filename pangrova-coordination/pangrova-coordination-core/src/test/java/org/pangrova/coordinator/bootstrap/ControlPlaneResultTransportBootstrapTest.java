package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.transport.ShuffleTlsConfig;
import org.pangrova.client.protocol.ResultProtocol;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.transport.FlightResultFetcher;
import org.pangrova.transport.TransportConnectionException;
import org.pangrova.transport.security.ResultTransportSecurity;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production acknowledgement wiring over actual authenticated TCP, not relational execution evidence. */
class ControlPlaneResultTransportBootstrapTest {
    @TempDir private static Path workspace;
    private static TestTlsAuthority authority;
    private static TestTlsAuthority.Identity serverIdentity;
    private static TestTlsAuthority.Identity clientIdentity;

    @BeforeAll
    static void createIdentities() throws Exception {
        authority = new TestTlsAuthority(workspace);
        serverIdentity = authority.issue("control-server", "localhost", false);
        clientIdentity = authority.issue("control-client", "localhost", false);
    }

    @AfterAll
    static void closeAuthority() throws Exception {
        if (authority != null) authority.close();
    }

    @Test
    void configuredMutualTlsIsAdvertisedAndDeliversEmptyAcknowledgementWithEof() throws Exception {
        withServer(true, endpoint -> {
            assertThat(endpoint.protocol()).isEqualTo(ResultProtocol.GRPC_TLS.value());
            try (var fetcher = new FlightResultFetcher(new ResultTransportSecurity(true, authority.root(),
                    clientIdentity.certificateChain(), clientIdentity.privateKey()))) {
                assertThat(fetcher.fetch(endpoint, System.currentTimeMillis() + 5_000)).isNotEmpty();
            }
        });
    }

    @Test
    void configuredMutualTlsRejectsClientWithoutCertificate() throws Exception {
        withServer(true, endpoint -> {
            try (var fetcher = new FlightResultFetcher(
                    new ResultTransportSecurity(true, authority.root(), null, null))) {
                assertThatThrownBy(() -> fetcher.fetch(endpoint, System.currentTimeMillis() + 5_000))
                        .isInstanceOf(TransportConnectionException.class);
            }
        });
    }

    @Test
    void explicitlyUnconfiguredTransportRemainsPlaintext() throws Exception {
        withServer(false, endpoint -> {
            assertThat(endpoint.protocol()).isEqualTo(ResultProtocol.GRPC.value());
            try (var fetcher = new FlightResultFetcher()) {
                assertThat(fetcher.fetch(endpoint, System.currentTimeMillis() + 5_000)).isNotEmpty();
            }
        });
    }

    private static void withServer(boolean secure, EndpointAssertion assertion) throws Exception {
        var defaults = CoordinatorBootstrapConfig.withDefaults("control-transport-test", 0);
        var tls = secure ? new ShuffleTlsConfig(serverIdentity.certificateChain(), serverIdentity.privateKey(),
                authority.root(), "localhost") : null;
        var config = new CoordinatorBootstrapConfig(defaults.core(), defaults.featureFlags(), defaults.query(),
                new CoordinatorBootstrapConfig.TransportConfig(0, 16 * 1024 * 1024, "localhost", tls),
                defaults.workerHealth(), null, null, null);
        var scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("control-transport-test");
        ControlPlaneResultTransportBootstrap.Started started = null;
        try {
            started = ControlPlaneResultTransportBootstrap.start(config, null, scheduler);
            var result = started.service().dispatchControlPlaneRows("control-ack", List.of(), List.of(), () -> {});
            assertion.verify(result.resultEndpoint());
        } finally {
            try {
                if (started != null) {
                    started.service().stopExpirationScheduling();
                    scheduler.shutdown();
                    assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                    try { started.server().stop(); }
                    finally {
                        started.service().close();
                        started.allocator().close();
                    }
                }
            } finally {
                scheduler.shutdownNow();
                assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @FunctionalInterface
    private interface EndpointAssertion {
        void verify(org.pangrova.client.protocol.ResultEndpoint endpoint) throws Exception;
    }
}
