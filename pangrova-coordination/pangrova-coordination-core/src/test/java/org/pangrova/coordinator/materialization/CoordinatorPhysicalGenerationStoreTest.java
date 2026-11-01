package org.pangrova.coordinator.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.exception.catalog.CatalogNotFoundException;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

/** GC physical generation lifecycle against connector-owned catalogs. */
class CoordinatorPhysicalGenerationStoreTest {

    private static final String CATALOG = "lake";
    private static final String TABLE_NAME = "__pg_mat_" + "a".repeat(32);
    private static final PhysicalTableRef TABLE = new PhysicalTableRef(
            CATALOG, InternalNamespace.SCHEMA_NAME, TABLE_NAME);
    private static final MaterializationId OBJECT = new MaterializationId(
            new TenantId("tenant"), QualifiedName.parse("lake.analytics.orders_mv"));

    @Test
    void existsReportsWhetherTheConnectorOwnsThePhysicalTable() {
        FakeMetadata metadata = new FakeMetadata();
        CoordinatorPhysicalGenerationStore store = store(metadata);

        assertThat(store.exists(OBJECT, TABLE)).isFalse();

        metadata.present = true;
        assertThat(store.exists(OBJECT, TABLE)).isTrue();
        assertThat(metadata.lookups).containsExactly(TABLE.schema() + "." + TABLE.table(),
                TABLE.schema() + "." + TABLE.table());
    }

    @Test
    void dropDropsTheHandleReturnedForTheExactPhysicalTable() {
        FakeMetadata metadata = new FakeMetadata();
        metadata.present = true;

        store(metadata).drop(OBJECT, TABLE);

        assertThat(metadata.dropped).isEqualTo(List.of(
                TABLE.schema() + "." + TABLE.table()));
    }

    @Test
    void dropIsIdempotentWhenThePhysicalTableIsAlreadyGone() {
        FakeMetadata metadata = new FakeMetadata();

        store(metadata).drop(OBJECT, TABLE);

        assertThat(metadata.dropped).isEmpty();
    }

    @Test
    void operationsRunAsTheInternalGcPrincipalBoundToTheCandidateTable() {
        FakeMetadata metadata = new FakeMetadata();
        metadata.present = true;

        store(metadata).exists(OBJECT, TABLE);

        assertThat(metadata.session.getUser()).isEqualTo("system:materialization-gc");
        assertThat(metadata.session.getCatalog()).contains(CATALOG);
        assertThat(metadata.session.getSchema()).contains(InternalNamespace.SCHEMA_NAME);
    }

    @Test
    void unknownCatalogFailsClosedInsteadOfClaimingTheTableIsAbsent() {
        CoordinatorPhysicalGenerationStore store = store(new FakeMetadata());

        assertThatThrownBy(() -> store.exists(OBJECT,
                new PhysicalTableRef("unknown", InternalNamespace.SCHEMA_NAME, TABLE_NAME)))
                .isInstanceOf(CatalogNotFoundException.class)
                .hasMessageContaining("unknown");
    }

    private static CoordinatorPhysicalGenerationStore store(ConnectorMetadata metadata) {
        Connector connector = new FakeConnector(metadata);
        CatalogRegistry registry = new CatalogRegistry(new EmptyStore(),
                descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        registry.register(new CatalogDescriptor(CATALOG, CatalogType.CONNECTOR, "iceberg",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        return new CoordinatorPhysicalGenerationStore(registry);
    }

    private static final class FakeHandle implements ConnectorTableHandle {
        private final SchemaTableName name;

        private FakeHandle(SchemaTableName name) {
            this.name = name;
        }
    }

    private static final class FakeMetadata implements ConnectorMetadata {
        private boolean present;
        private final List<String> lookups = new java.util.ArrayList<>();
        private final List<String> dropped = new java.util.ArrayList<>();
        private ConnectorSession session;

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session,
                SchemaTableName tableName) {
            this.session = session;
            lookups.add(tableName.toString());
            return present ? new FakeHandle(tableName) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                ConnectorTableHandle table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                ConnectorTableHandle table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session,
                ConnectorTableHandle table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session,
                Optional<String> schemaName) {
            return List.of();
        }

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle table) {
            dropped.add(((FakeHandle) table).name.toString());
        }
    }

    private static final class FakeConnector implements Connector {
        private final ConnectorMetadata metadata;

        private FakeConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class EmptyStore implements MetadataStore {
        @Override
        public void saveTable(TableMetadata metadata) {
        }

        @Override
        public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void deleteTable(String catalog, String schema, String table) {
        }

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) {
            return List.of();
        }

        @Override
        public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
        }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.empty();
        }

        @Override
        public void deleteCatalogDescriptor(String name) {
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.of();
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.empty();
        }

        @Override
        public void updateSyncTime(String catalogName, Instant syncTime) {
        }

        @Override
        public void recordSyncHistory(SyncResult result) {
        }

        @Override
        public void saveSchemas(String catalogName, List<String> schemas) {
        }
    }
}
