package org.pangrova.metadata.service;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.core.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.exception.MetadataException;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.core.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that DefaultMetadataService.resolveFromCatalog throws MetadataException
 * with proper context when getTableHandle returns null.
 */
class DefaultMetadataServiceResolveTest {

    private DefaultMetadataService service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        NullReturningConnectorMetadata connectorMetadata = new NullReturningConnectorMetadata();
        TestMetadataStore store = new TestMetadataStore();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(store, chf, null, dispatcher);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "my_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        registry.register(descriptor);
        registry.initialize();

        service = new DefaultMetadataService(
                registry,
                new TieredMetadataCache(),
                dispatcher,
                new SchemaEvolutionValidator(),
                10_000L,
                Duration.ofMinutes(5));
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void resolveTableThrowsMetadataExceptionWhenTableHandleIsNull() {
        QualifiedTableRef tableRef = new QualifiedTableRef("my_catalog", "my_schema", "nonexistent_table");
        MetadataSessionContext session = testSession();

        assertThatThrownBy(() -> service.resolveTable(session, tableRef))
                .isInstanceOf(MetadataException.class)
                .satisfies(ex -> {
                    MetadataException me = (MetadataException) ex;
                    assertThat(me.getErrorCode()).isEqualTo(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND);
                    assertThat(me.getMessage()).contains("my_catalog");
                    assertThat(me.getMessage()).contains("my_schema");
                    assertThat(me.getMessage()).contains("nonexistent_table");
                });
    }

    private static MetadataSessionContext testSession() {
        return new MetadataSessionContext(
                "test-user", "my_catalog", "my_schema", "UTC", "en",
                Map.of(), "default", "", "test-query-resolve");
    }

    // ── Test infrastructure ──

    /**
     * ConnectorMetadata that always returns null from getTableHandle,
     * simulating a table that does not exist in the catalog.
     */
    static final class NullReturningConnectorMetadata implements ConnectorMetadata {

        @Override
        public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("my_schema");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle h) {
            throw new IllegalStateException("Should not be called when table handle is null");
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle h) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle h) {
            return TableStatistics.UNKNOWN;
        }
    }

    static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }

        @Override
        public ConnectorSplitManager getSplitManager() { throw new UnsupportedOperationException(); }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() { throw new UnsupportedOperationException(); }

        @Override
        public void shutdown() { }
    }

    static final class TestMetadataStore implements MetadataStore {
        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public void deleteCatalogDescriptor(String n) { catalogs.removeIf(d -> d.name().equals(n)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
