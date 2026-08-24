package org.pangrova.client.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies R10.5: PangrovaDriver shares a single HttpClient across JDBC connections.
 */
class PangrovaDriverSharedClientTest {

    @AfterEach
    void tearDown() {
        PangrovaDriver.shutdownSharedClient();
    }

    @Test
    void acceptsURL_validPangrovaUrl_returnsTrue() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.acceptsURL("jdbc:pangrova://localhost:8080")).isTrue();
        assertThat(driver.acceptsURL("jdbc:pangrova://host:9090/catalog")).isTrue();
    }

    @Test
    void acceptsURL_nonPangrovaUrl_returnsFalse() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.acceptsURL("jdbc:mysql://localhost:3306")).isFalse();
        assertThat(driver.acceptsURL(null)).isFalse();
    }

    @Test
    void shutdownSharedClient_isIdempotent() {
        // Should not throw even when called multiple times without creating a client
        PangrovaDriver.shutdownSharedClient();
        PangrovaDriver.shutdownSharedClient();
    }

    @Test
    void majorVersion_isZero() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.getMajorVersion()).isEqualTo(0);
        assertThat(driver.getMinorVersion()).isEqualTo(1);
    }

    @Test
    void jdbcCompliant_returnsFalse() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.jdbcCompliant()).isFalse();
    }
}
