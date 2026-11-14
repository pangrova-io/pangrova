package org.pangrova.metadata.service;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.session.MetadataSessionContext;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.exception.MetadataException;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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
        TestCatalogDeployments store = new TestCatalogDeployments();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(memoryTableStores(), store, chf, null, dispatcher);
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
        public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

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

    static final class TestCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return catalogs.removeIf(d -> d.identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
