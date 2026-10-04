package org.pangrova.client.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.testing.testkit.security.TestHttpsEndpoint;
import org.pangrova.testing.testkit.security.TestTlsAuthority;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual JDBC-configured HTTPS handshakes and isolated trust policies; not SQL/JNI E2E. */
class JdbcHttpsTrustStoreTest {
    @TempDir static Path directory;
    private static TestTlsAuthority first;
    private static TestTlsAuthority second;
    private static TestTlsAuthority.Identity firstServer;
    private static TestTlsAuthority.Identity secondServer;
    private static TestTlsAuthority.Identity expiredServer;
    private static Path firstStore;
    private static Path secondStore;

    @BeforeAll static void createAuthorities() throws Exception {
        try {
            first = new TestTlsAuthority(directory);
            second = new TestTlsAuthority(directory);
            firstServer = first.issue("first-server", "localhost", false);
            secondServer = second.issue("second-server", "localhost", false);
            expiredServer = first.issue("expired-server", "localhost", true);
            firstStore = directory.resolve("first.jks");
            secondStore = directory.resolve("second.jks");
            TestHttpsEndpoint.writeTrustStore(firstStore, first.root(), "store-password".toCharArray());
            TestHttpsEndpoint.writeTrustStore(secondStore, second.root(), "store-password".toCharArray());
        } catch (Exception | Error failure) {
            try { closeAuthorities(); } catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    @AfterAll static void closeAuthorities() throws Exception {
        Exception failure = null;
        try { if (first != null) first.close(); } catch (Exception cleanupFailure) { failure = cleanupFailure; }
        try { if (second != null) second.close(); }
        catch (Exception cleanupFailure) {
            if (failure == null) failure = cleanupFailure;
            else failure.addSuppressed(cleanupFailure);
        }
        if (failure != null) throw failure;
    }

    @AfterEach void closeSharedOwner() { PangrovaDriver.shutdownSharedClient(); }

    @Test void explicitJksIsConsumedByTheActualHttpsHandshake() throws Exception {
        try (var server = new TestHttpsEndpoint(first, firstServer);
             var connection = connect(server, "localhost", firstStore)) {
            assertThat(connection.isValid(2)).isTrue();
            assertThat(server.requests()).isEqualTo(1);
            assertThat(connection.restClient().config().trustStorePath()).isEqualTo(firstStore.toString());
        }
    }

    @Test void configuredJksConsumesPoolSizeIndependentlyOfFailoverEndpoints() throws Exception {
        var props = properties(firstStore);
        props.setProperty("poolSize", "2");
        props.setProperty("failover", "backup.invalid:8080");
        try (var server = new TestHttpsEndpoint(first, firstServer);
             var connection = (PangrovaConnection) new PangrovaDriver().connect(
                     "jdbc:pangrova://localhost:" + server.port(), props)) {
            assertThat(connection.restClient().config().httpExecutorThreads()).isEqualTo(2);
            assertThat(connection.restClient().config().connectionPoolConfig().maxConnectionsPerEndpoint()).isEqualTo(10);
            assertThat(connection.restClient().config().connectionPoolConfig().endpoints()).hasSize(2);
            assertThat(connection.isValid(2)).isTrue();
            assertThat(server.requests()).isEqualTo(1);
        }
    }

    @Test void configuredJksDefaultExecutorSizeIsExplicitAndTraceable() throws Exception {
        try (var server = new TestHttpsEndpoint(first, firstServer);
             var connection = connect(server, "localhost", firstStore)) {
            assertThat(connection.restClient().config().httpExecutorThreads()).isEqualTo(4);
            assertThat(connection.isValid(2)).isTrue();
            assertThat(server.requests()).isEqualTo(1);
        }
    }

    @Test void malformedPoolSizeIsRejectedBeforeTrustStoreOrSharedClientAllocation() {
        for (String size : java.util.List.of("", "0", "-1", "two", "2147483648")) {
            var props = properties(directory.resolve("unopened.jks"));
            props.setProperty("poolSize", size);
            assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", props))
                    .isInstanceOf(SQLException.class).hasMessageContaining("poolSize must be a positive integer");
            props.remove("sslTrustStore");
            props.remove("sslTrustStorePassword");
            assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", props))
                    .isInstanceOf(SQLException.class).hasMessageContaining("poolSize must be a positive integer");
        }
    }

    @Test void systemTrustSharedClientDoesNotTrustAnUninstalledTestCa() throws Exception {
        try (var server = new TestHttpsEndpoint(first, firstServer);
             var connection = connect(server, "localhost", null)) {
            assertThatThrownBy(() -> connection.restClient().ping(2))
                    .hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);
            assertThat(server.requests()).isZero();
        }
    }

    @Test void unrelatedJksRejectsTheActualServerCertificate() throws Exception {
        assertRejected(firstServer, "localhost", secondStore);
    }

    @Test void trustedCaDoesNotAuthorizeAnExpiredLeaf() throws Exception {
        assertRejected(expiredServer, "localhost", firstStore);
    }

    @Test void trustedCaDoesNotDisableHostnameVerification() throws Exception {
        assertRejected(firstServer, "127.0.0.1", firstStore);
    }

    @Test void missingJksFailsConstructionInsteadOfUsingSharedSystemTrust() {
        var props = properties(directory.resolve("missing.jks"));
        assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", props))
                .isInstanceOf(SQLException.class).hasMessageContaining("trust store could not be loaded");
    }

    @Test void wrongJksPasswordFailsConstruction() {
        var props = properties(firstStore);
        props.setProperty("sslTrustStorePassword", "wrong-store-password");
        assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", props))
                .isInstanceOf(SQLException.class).hasMessageContaining("trust store could not be loaded");
    }

    @Test void twoActualCertificateAuthoritiesKeepDistinctConnectionContexts() throws Exception {
        try (var firstEndpoint = new TestHttpsEndpoint(first, firstServer);
             var secondEndpoint = new TestHttpsEndpoint(second, secondServer);
             var firstConnection = connect(firstEndpoint, "localhost", firstStore);
             var secondConnection = connect(secondEndpoint, "localhost", secondStore);
             var wrongConnection = connect(secondEndpoint, "localhost", firstStore)) {
            assertThat(firstConnection.isValid(2)).isTrue();
            assertThat(secondConnection.isValid(2)).isTrue();
            assertThatThrownBy(() -> wrongConnection.restClient().ping(2))
                    .hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);
            assertThat(firstConnection.isValid(2)).isTrue();
            assertThat(firstEndpoint.requests()).isEqualTo(2);
            assertThat(secondEndpoint.requests()).isEqualTo(1);
        }
    }

    @Test void closingOneConfiguredConnectionDoesNotCloseAnotherContext() throws Exception {
        try (var server = new TestHttpsEndpoint(first, firstServer);
             var connection = connect(server, "localhost", firstStore);
             var other = connect(server, "localhost", firstStore)) {
            assertThat(connection.isValid(2)).isTrue();
            connection.close();
            assertThat(connection.isClosed()).isTrue();
            assertThatThrownBy(connection::createStatement).isInstanceOf(SQLException.class).hasMessageContaining("closed");
            assertThat(other.isValid(2)).isTrue();
            assertThat(server.requests()).isEqualTo(2);
        }
    }

    private static void assertRejected(TestTlsAuthority.Identity identity, String hostname, Path store) throws Exception {
        try (var server = new TestHttpsEndpoint(first, identity);
             var connection = connect(server, hostname, store)) {
            assertThatThrownBy(() -> connection.restClient().ping(2))
                    .hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);
            assertThat(server.requests()).isZero();
        }
    }

    private static PangrovaConnection connect(TestHttpsEndpoint server, String host, Path store) throws SQLException {
        return (PangrovaConnection) new PangrovaDriver().connect(
                "jdbc:pangrova://" + host + ":" + server.port(), properties(store));
    }

    private static Properties properties(Path store) {
        var props = new Properties();
        props.setProperty("ssl", "true");
        props.setProperty("connectTimeoutMs", "2000");
        props.setProperty("requestTimeoutMs", "2000");
        if (store != null) {
            props.setProperty("sslTrustStore", store.toString());
            props.setProperty("sslTrustStorePassword", "store-password");
        }
        return props;
    }
}
