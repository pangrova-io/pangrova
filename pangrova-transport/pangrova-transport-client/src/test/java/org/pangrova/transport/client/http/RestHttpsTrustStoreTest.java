package org.pangrova.transport.client.http;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.testing.testkit.security.TestHttpsEndpoint;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.client.session.ConnectionPoolConfig;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual JKS HTTPS health and request-owner barriers; not database execution or authentication proof. */
class RestHttpsTrustStoreTest {
    @TempDir static Path directory;
    private static TestTlsAuthority authority;
    private static TestTlsAuthority.Identity identity;
    private static Path store;

    @BeforeAll static void setup() throws Exception {
        authority = new TestTlsAuthority(directory);
        try {
            identity = authority.issue("rest-server", "localhost", false);
            store = directory.resolve("rest.jks");
            TestHttpsEndpoint.writeTrustStore(store, authority.root(), "rest-password".toCharArray());
        } catch (Exception | Error failure) {
            try { authority.close(); } catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    @AfterAll static void cleanup() throws Exception { if (authority != null) authority.close(); }

    @Test void configuredJksAuthenticatesActualHttpsServer() throws Exception {
        try (var server = new TestHttpsEndpoint(authority, identity);
             var client = configured(server).build()) {
            client.ping(2);
            assertThat(server.requests()).isEqualTo(1);
        }
    }

    @Test void sharedHttpClientCannotSilentlyReplaceExplicitTrustStore() {
        assertThatThrownBy(() -> PangrovaRestClient.builder().useTls(true)
                .trustStorePath(store.toString()).trustStorePassword("rest-password")
                .httpClient(HttpClient.newHttpClient()).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot override");
    }

    @Test void explicitTrustStoreCannotBeIgnoredByPlainHttp() {
        assertThatThrownBy(() -> PangrovaRestClient.builder().trustStorePath(store.toString()).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires HTTPS");
    }

    @Test void blankTrustStorePathIsRejected() {
        assertThatThrownBy(() -> PangrovaRestClient.builder().useTls(true).trustStorePath(" ").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-blank");
    }

    @Test void trustStorePasswordWithoutStoreIsRejected() {
        assertThatThrownBy(() -> PangrovaRestClient.builder().useTls(true).trustStorePassword("unused-secret").build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires its trust store");
    }

    @Test void failedConstructionDoesNotExposeTrustStorePassword() {
        assertThatThrownBy(() -> PangrovaRestClient.builder().useTls(true)
                .trustStorePath(directory.resolve("missing.jks").toString()).trustStorePassword("private-secret").build())
                .isInstanceOf(IllegalStateException.class).hasMessageNotContaining("private-secret");
    }

    @Test void closeRetainsActualHeldHttpsRequestAndSealsNewAdmission() throws Exception {
        var requester = ManagedExecutorFactory.newSingleThreadExecutor("held-https-client");
        try (var server = new TestHttpsEndpoint(authority, identity);
             var client = configured(server).connectionPoolConfig(ConnectionPoolConfig.singleEndpoint("localhost", server.port())).build();
             var block = server.blockNextRequest()) {
            var actual = requester.submit(() -> client.ping(5));
            try {
                assertThat(block.awaitEntered()).isTrue();
                assertThatThrownBy(client::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("still owned");
                assertThatThrownBy(() -> client.ping(2)).isInstanceOf(IllegalStateException.class).hasMessageContaining("admission is closed");
            } finally { block.close(); }
            actual.get(5, TimeUnit.SECONDS);
            client.close();
            assertThat(server.requests()).isEqualTo(1);
        } finally {
            requester.shutdown();
            assertThat(requester.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test void successfulCloseForbidsActualFurtherRequests() throws Exception {
        try (var server = new TestHttpsEndpoint(authority, identity); var client = configured(server).build()) {
            client.ping(2);
            client.close();
            assertThatThrownBy(() -> client.ping(2)).isInstanceOf(IllegalStateException.class).hasMessageContaining("admission is closed");
            assertThat(server.requests()).isEqualTo(1);
        }
    }

    private static PangrovaRestClient.Builder configured(TestHttpsEndpoint server) {
        return PangrovaRestClient.builder().coordinatorHost("localhost").coordinatorPort(server.port()).useTls(true)
                .trustStorePath(store.toString()).trustStorePassword("rest-password");
    }
}
