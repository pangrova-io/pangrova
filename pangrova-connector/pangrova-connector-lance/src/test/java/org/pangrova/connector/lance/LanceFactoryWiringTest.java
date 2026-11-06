package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.plugin.ConnectorFactory;
import org.pangrova.types.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceFactoryWiringTest {
    private static final Map<String, String> CONFIG = Map.of("warehouse", "s3://warehouse");

    @Test
    void serviceLoaderRegistersFactoryAndWorkerCodecDoesNotOpenCatalogOrPageSink() {
        ConnectorFactory factory = ServiceLoader.load(ConnectorFactory.class).stream()
                .map(ServiceLoader.Provider::get).filter(provider -> provider.getName().equals("lance"))
                .findFirst().orElseThrow();
        var connector = factory.create("lance_catalog", CONFIG, Optional::empty);
        assertThat(connector.getOutputHandleCodec().typeUrl()).contains("connector/lance");
        assertThat(connector.getSplitManager()).isInstanceOf(LanceSplitManager.class);
        assertThatThrownBy(connector::getPageSinkProvider).hasMessageContaining("Native output allocation");
    }

    @Test
    void createWithoutRealNativeSchemaControlFailsBeforeOpeningCatalog() {
        var connector = new LanceConnectorFactory().create("lance_catalog", CONFIG, Optional::empty);
        var metadata = new ConnectorTableMetadata(new SchemaTableName("db", "new_table"), List.of("id"),
                List.of(new ConnectorTableMetadata.Column("id", PangrovaType.INTEGER)), Map.of());
        assertThatThrownBy(() -> connector.getMetadata(LanceSplitManagerTest.session("tenant-a"))
                .createTable(LanceSplitManagerTest.session("tenant-a"), metadata))
                .hasMessageContaining("Native dataset control port");
    }

    @Test
    void rejectsUnsupportedWarehouseAndMissingPersistentCatalog() {
        var factory = new LanceConnectorFactory();
        var workerConnector = factory.create("lance_catalog", Map.of("warehouse", "s3://warehouse"), Optional::empty);
        assertThatThrownBy(() -> workerConnector.getMetadata(LanceSplitManagerTest.session("tenant-a"))
                .listSchemaNames(LanceSplitManagerTest.session("tenant-a")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("owner process");
        assertThatThrownBy(() -> factory.create("lance_catalog", Map.of("jdbc.url", "jdbc:postgresql://127.0.0.1:1/never_opened",
                "warehouse", "s3://warehouse"), Optional::empty)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shared metadata authority");
        assertThatThrownBy(() -> factory.create("lance_catalog", Map.of("warehouse", "s3://warehouse/path/../other"), Optional::empty))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void localWarehouseRequiresAbsolutePathOrCanonicalFileUriWithoutRewriting() {
        var factory = new LanceConnectorFactory();
        for (String warehouse : List.of("/warehouse/lance", "file:///warehouse/lance")) {
            assertThat(factory.create("lance_catalog", Map.of("warehouse", warehouse), Optional::empty)).isInstanceOf(LanceConnector.class);
        }
        assertThatThrownBy(() -> factory.create("lance_catalog", Map.of("warehouse", "file:/warehouse/lance"), Optional::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical file:///");
        for (String warehouse : List.of("relative/lance", "file://remote/warehouse/lance",
                "/warehouse/%2e%2e/lance", "/warehouse/lance?credential=secret")) {
            assertThatThrownBy(() -> factory.create("lance_catalog", Map.of("warehouse", warehouse), Optional::empty)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
