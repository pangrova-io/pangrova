package org.pangrova.client.jdbc;

import org.pangrova.transport.client.http.PangrovaRestClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.testing.testkit.security.TestHttpsEndpoint;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link PangrovaDriver#connect} maps connection properties
 * (authentication, TLS, failover, retries) from both {@link Properties} and
 * the URL query string onto the underlying REST client configuration, and
 * that unknown security-relevant properties fail explicitly instead of being
 * silently swallowed (architecture-remediation #185).
 */
class PangrovaDriverConnectionPropertiesTest {
    @TempDir java.nio.file.Path directory;

    private static final String BASE_URL = "jdbc:pangrova://localhost:8080";

    @AfterEach
    void tearDown() {
        PangrovaDriver.shutdownSharedClient();
    }

    private static PangrovaRestClientConfig connectAndGetConfig(String url, Properties info)
            throws SQLException {
        PangrovaDriver driver = new PangrovaDriver();
        try (PangrovaConnection conn = (PangrovaConnection) driver.connect(url, info)) {
            assertThat(conn).isNotNull();
            return conn.restClient().config();
        }
    }

    @Test
    void connect_tokenProperty_appliedToClientConfig() throws SQLException {
        Properties info = new Properties();
        info.setProperty("token", "secret-token");
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL, info);
        assertThat(cfg.authentication().authorizationHeader()).isEqualTo("Bearer secret-token");
    }

    @Test
    void connect_tokenFromUrlQuery_appliedToClientConfig() throws SQLException {
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL + "?token=url-token", null);
        assertThat(cfg.authentication().authorizationHeader()).isEqualTo("Bearer url-token");
    }

    @Test
    void connect_sslProperties_appliedToClientConfig() throws Exception {
        var store = directory.resolve("actual-truststore.jks");
        try (var authority = new TestTlsAuthority(directory)) {
            TestHttpsEndpoint.writeTrustStore(store, authority.root(), "changeit".toCharArray());
        }
        Properties info = new Properties();
        info.setProperty("ssl", "true");
        info.setProperty("sslTrustStore", store.toString());
        info.setProperty("sslTrustStorePassword", "changeit");
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL, info);
        assertThat(cfg.useTls()).isTrue();
        assertThat(cfg.scheme()).isEqualTo("https");
        assertThat(cfg.trustStorePath()).isEqualTo(store.toString());
        assertThat(cfg.trustStorePassword()).isEqualTo("changeit");
    }

    @Test
    void connect_failoverProperty_appliedToClientConfig() throws SQLException {
        Properties info = new Properties();
        info.setProperty("failover", "backup1:8080,backup2:8081");
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL, info);
        assertThat(cfg.connectionPoolConfig()).isNotNull();
        assertThat(cfg.connectionPoolConfig().failoverEnabled()).isTrue();
        assertThat(cfg.connectionPoolConfig().endpoints())
                .containsExactly("localhost:8080", "backup1:8080", "backup2:8081");
    }

    @Test
    void connect_retriesProperty_appliedToClientConfig() throws SQLException {
        Properties info = new Properties();
        info.setProperty("retries", "7");
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL, info);
        assertThat(cfg.retryPolicy().maxRetries()).isEqualTo(7);
    }

    @Test
    void connect_timeoutProperties_appliedToClientConfig() throws SQLException {
        Properties info = new Properties();
        info.setProperty("connectTimeoutMs", "1500");
        info.setProperty("requestTimeoutMs", "12000");
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL, info);
        assertThat(cfg.connectTimeout().toMillis()).isEqualTo(1500);
        assertThat(cfg.requestTimeout().toMillis()).isEqualTo(12000);
    }

    @Test
    void connect_propertiesOverrideUrlQuery() throws SQLException {
        Properties info = new Properties();
        info.setProperty("token", "props-wins");
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL + "?token=url-loses", info);
        assertThat(cfg.authentication().authorizationHeader()).isEqualTo("Bearer props-wins");
    }

    @Test
    void connect_sharedPoolRejectsExplicitConflictButAllowsAnUnspecifiedSize() throws SQLException {
        var props = new Properties();
        props.setProperty("poolSize", "2");
        try (var first = new PangrovaDriver().connect(BASE_URL, props);
             var same = new PangrovaDriver().connect(BASE_URL, props);
             var unspecified = new PangrovaDriver().connect(BASE_URL, null)) {
            assertThat(first).isNotNull();
            assertThat(same).isNotNull();
            assertThat(unspecified).isNotNull();
            props.setProperty("poolSize", "3");
            assertThatThrownBy(() -> new PangrovaDriver().connect(BASE_URL, props))
                    .isInstanceOf(SQLException.class).hasMessageContaining("existing shared HTTP executor size");
        }
    }

    @Test
    void connect_unknownSecurityProperty_throws() {
        PangrovaDriver driver = new PangrovaDriver();
        Properties info = new Properties();
        info.setProperty("sslKeyStorePassword", "oops");
        assertThatThrownBy(() -> driver.connect(BASE_URL, info))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("sslKeyStorePassword");
    }

    @Test
    void connect_unknownSecurityPropertyFromUrl_throws() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThatThrownBy(() -> driver.connect(BASE_URL + "?authToken=oops", null))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("authToken");
    }

    @Test
    void connect_unknownNonSecurityProperty_ignored() throws SQLException {
        Properties info = new Properties();
        info.setProperty("applicationName", "my-bi-tool");
        // Non-security unknown props are tolerated, not fatal.
        PangrovaRestClientConfig cfg = connectAndGetConfig(BASE_URL, info);
        assertThat(cfg).isNotNull();
    }

    @Test
    void connect_invalidBooleanForSsl_throws() {
        PangrovaDriver driver = new PangrovaDriver();
        Properties info = new Properties();
        info.setProperty("ssl", "yesplease");
        assertThatThrownBy(() -> driver.connect(BASE_URL, info))
                .isInstanceOf(SQLException.class);
    }

    @Test
    void getPropertyInfo_advertisesKnownProperties() {
        PangrovaDriver driver = new PangrovaDriver();
        DriverPropertyInfo[] infos = driver.getPropertyInfo(BASE_URL, null);
        assertThat(infos).isNotEmpty();
        assertThat(infos).extracting(i -> i.name)
                .contains("token", "ssl", "sslTrustStore", "sslTrustStorePassword",
                        "failover", "retries", "connectTimeoutMs", "requestTimeoutMs");
    }

    @Test
    void getPropertyInfo_prefillsCurrentValues() {
        PangrovaDriver driver = new PangrovaDriver();
        Properties info = new Properties();
        info.setProperty("token", "abc");
        DriverPropertyInfo[] infos = driver.getPropertyInfo(BASE_URL, info);
        DriverPropertyInfo tokenInfo = java.util.Arrays.stream(infos)
                .filter(i -> i.name.equals("token"))
                .findFirst().orElseThrow();
        assertThat(tokenInfo.value).isEqualTo("abc");
    }
}
