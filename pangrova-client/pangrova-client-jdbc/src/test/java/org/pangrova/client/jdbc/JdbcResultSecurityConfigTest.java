package org.pangrova.client.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual driver and client configuration; does not claim an authenticated SQL execution. */
class JdbcResultSecurityConfigTest {
    @AfterEach void closeSharedClient() { PangrovaDriver.shutdownSharedClient(); }

    @Test
    void jdbcResultPemOptionsReachTheSoleClientPolicy() throws Exception {
        var properties = new Properties();
        properties.setProperty("resultTlsRootCa", "root.pem");
        properties.setProperty("resultTlsCertificateChain", "client.pem");
        properties.setProperty("resultTlsPrivateKey", "key.pem");
        try (var connection = (PangrovaConnection) new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", properties)) {
            var config = connection.restClient().config();
            assertThat(config.useTls()).isFalse();
            assertThat(config.trustStorePath()).isNull();
            assertThat(config.resultTransportSecurity().requireTls()).isTrue();
            assertThat(config.resultTransportSecurity().rootCaPath()).isEqualTo(Path.of("root.pem"));
            assertThat(config.resultTransportSecurity().certificateChainPath()).isEqualTo(Path.of("client.pem"));
            assertThat(config.resultTransportSecurity().privateKeyPath()).isEqualTo(Path.of("key.pem"));
        }
    }

    @Test
    void partialAndUnknownResultSecurityPropertiesFailBeforeReturningConnection() {
        var incomplete = new Properties();
        incomplete.setProperty("resultTlsPrivateKey", "key.pem");
        assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", incomplete))
                .isInstanceOf(SQLException.class);
        var unknown = new Properties();
        unknown.setProperty("resultTlsClientKey", "key.pem");
        assertThatThrownBy(() -> new PangrovaDriver().connect("jdbc:pangrova://localhost:8080", unknown))
                .isInstanceOf(SQLException.class).hasMessageContaining("Unknown security property");
    }
}
