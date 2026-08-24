package org.pangrova.testing.e2e.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for ConnectorTestConfig record and its factory methods.
 */
class ConnectorTestConfigTest {

    @Test
    void constructorRejectsNullCatalogName() {
        assertThatThrownBy(() -> new ConnectorTestConfig(null, "db", "iceberg", Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRejectsNullSchemaName() {
        assertThatThrownBy(() -> new ConnectorTestConfig("cat", null, "iceberg", Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRejectsNullConnectorType() {
        assertThatThrownBy(() -> new ConnectorTestConfig("cat", "db", null, Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void propertiesAreDefensivelyCopied() {
        var mutable = new java.util.HashMap<String, String>();
        mutable.put("key", "value");
        var config = new ConnectorTestConfig("cat", "db", "iceberg", mutable);

        mutable.put("extra", "should-not-appear");
        assertThat(config.properties()).doesNotContainKey("extra");
    }

    @Test
    void icebergFactoryProducesCorrectConfig() {
        var config = ConnectorTestConfig.iceberg("ice_cat", "db", "http://localhost:9000");

        assertThat(config.catalogName()).isEqualTo("ice_cat");
        assertThat(config.schemaName()).isEqualTo("db");
        assertThat(config.connectorType()).isEqualTo("iceberg");
        assertThat(config.properties())
                .containsEntry("warehouse", "s3a://warehouse/")
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsKey("s3.access-key")
                .containsKey("s3.secret-key");
    }

    @Test
    void paimonFactoryProducesCorrectConfig() {
        var config = ConnectorTestConfig.paimon("pai_cat", "db", "/tmp/warehouse");

        assertThat(config.connectorType()).isEqualTo("paimon");
        assertThat(config.properties()).containsEntry("warehouse", "/tmp/warehouse");
    }

}
