package org.pangrova.coordinator.metadata.read;

import org.pangrova.coordinator.metadata.MetadataPlanningTestSessions;

import org.apache.calcite.schema.lookup.LikePattern;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LazyMetadataSchemaSystemTableLookupTest {

    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";

    @Test
    void caseInsensitiveVirtualLookupUsesCanonicalBaseAndSuffixWithoutEnumerating() {
        TestMetadata metadata = new TestMetadata();
        var lookup = schema(metadata).tables();

        var snapshots = lookup.getIgnoreCase("ORDERS$SNAPSHOTS");
        var history = lookup.getIgnoreCase("orders$HiStOrY");
        var tags = lookup.getIgnoreCase("ORDERS$TAGS");
        var options = lookup.getIgnoreCase("orders$OpTiOnS");
        var schemas = lookup.getIgnoreCase("ORDERS$ScHeMaS");
        var branches = lookup.getIgnoreCase("orders$BrAnChEs");

        assertThat(snapshots).isNotNull();
        assertThat(snapshots.name()).isEqualTo("Orders$snapshots");
        assertThat(history).isNotNull();
        assertThat(history.name()).isEqualTo("Orders$history");
        assertThat(tags).isNotNull();
        assertThat(tags.name()).isEqualTo("Orders$tags");
        assertThat(options).isNotNull();
        assertThat(options.name()).isEqualTo("Orders$options");
        assertThat(schemas).isNotNull();
        assertThat(schemas.name()).isEqualTo("Orders$schemas");
        assertThat(branches).isNotNull();
        assertThat(branches.name()).isEqualTo("Orders$branches");
        assertThat(metadata.lookups).contains("Orders$snapshots", "Orders$history",
                "Orders$tags", "Orders$options", "Orders$schemas", "Orders$branches");
        assertThat(lookup.getNames(LikePattern.any())).containsExactly("Orders");
    }

    @Test
    void ordinaryAndCaseSensitiveTablesStillResolve() {
        var lookup = schema(new TestMetadata()).tables();

        assertThat(lookup.getIgnoreCase("ORDERS").name()).isEqualTo("Orders");
        assertThat(lookup.get("Orders")).isNotNull();
        assertThat(lookup.get("Orders$history")).isNotNull();
    }

    @Test
    void namedTableLookupListsNamesButDoesNotLoadUnrelatedTableMetadata() {
        TestMetadata metadata = new TestMetadata();
        for (int i = 0; i < 1_000; i++) {
            metadata.tableNames.add("Unrelated" + i);
        }
        var lookup = schema(metadata).tables();

        assertThat(lookup.getIgnoreCase("Orders").name()).isEqualTo("Orders");
        assertThat(lookup.get("Orders")).isNotNull();
        assertThat(metadata.listCalls).isEqualTo(1);
        assertThat(metadata.lookups).containsExactly("Orders", "Orders");
    }

    @Test
    void nameEnumerationDoesNotLoadTableMetadataAndHonorsDdlInvalidation() {
        TestMetadata metadata = new TestMetadata();
        var lazySchema = schema(metadata);
        var lookup = lazySchema.tables();

        assertThat(lookup.getNames(LikePattern.any())).containsExactly("Orders");
        assertThat(metadata.listCalls).isEqualTo(1);
        assertThat(metadata.lookups).isEmpty();

        metadata.tableNames.add("Fresh");
        assertThat(lookup.getNames(new LikePattern("F%"))).isEmpty();
        assertThat(metadata.listCalls).isEqualTo(1);

        lazySchema.invalidateCache();
        assertThat(lookup.getNames(new LikePattern("F%"))).containsExactly("Fresh");
        assertThat(metadata.listCalls).isEqualTo(2);
        assertThat(metadata.lookups).isEmpty();
    }

    @Test
    void externallyDroppedTableDoesNotResolveFromCachedName() {
        TestMetadata metadata = new TestMetadata();
        var lookup = schema(metadata).tables();

        assertThat(lookup.getIgnoreCase("ORDERS")).isNotNull();
        metadata.tableNames.remove("Orders");

        assertThat(lookup.getIgnoreCase("ORDERS")).isNull();
        assertThat(metadata.listCalls).isEqualTo(1);
    }

    @Test
    void unknownSuffixAndMissingBaseNeverResolveAsVirtualTables() {
        TestMetadata metadata = new TestMetadata();
        var lookup = schema(metadata).tables();

        assertThat(lookup.getIgnoreCase("ORDERS$FILES")).isNull();
        assertThat(lookup.get("Orders$files")).isNull();
        assertThat(lookup.getIgnoreCase("MISSING$SNAPSHOTS")).isNull();
        assertThat(metadata.lookups).doesNotContain("Orders$files", "Missing$snapshots");
    }

    @Test
    void systemTableBackendFailurePropagatesInsteadOfBecomingNotFound() {
        TestMetadata metadata = new TestMetadata();
        metadata.failSystemLookup = true;
        var lookup = schema(metadata).tables();

        assertThatThrownBy(() -> lookup.getIgnoreCase("ORDERS$SNAPSHOTS"))
                .isInstanceOf(MetadataResolutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Orders$snapshots");
    }

    @Test
    void externallyCreatedBaseTableResolvesBeforePhysicalNameCacheExpires() {
        TestMetadata metadata = new TestMetadata();
        var lookup = schema(metadata).tables();
        assertThat(lookup.getIgnoreCase("ORDERS")).isNotNull();
        assertThat(lookup.getNames(LikePattern.any())).containsExactly("Orders");

        // This simulates a second coordinator creating a table while this
        // coordinator still holds its cached physical names.
        metadata.tableNames.add("Fresh");
        assertThat(lookup.getNames(LikePattern.any())).containsExactly("Orders");

        var snapshots = lookup.getIgnoreCase("FRESH$SNAPSHOTS");
        assertThat(snapshots).isNotNull();
        assertThat(snapshots.name()).isEqualTo("Fresh$snapshots");
        assertThat(metadata.lookups).contains("Fresh$snapshots");
        assertThat(metadata.lookups).doesNotContain("FRESH$snapshots");
    }

    @Test
    void uncachedBaseNameBackendFailureIsNotReportedAsMissingTable() {
        TestMetadata metadata = new TestMetadata();
        var lookup = schema(metadata).tables();
        assertThat(lookup.getIgnoreCase("ORDERS")).isNotNull();
        metadata.failListTables = true;

        assertThatThrownBy(() -> lookup.getIgnoreCase("FRESH$HISTORY"))
                .isInstanceOf(MetadataResolutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FRESH");
    }

    private static LazyMetadataSchema schema(TestMetadata metadata) {
        Connector connector = new Connector() {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session) {
                return metadata;
            }

            @Override
            public ConnectorSplitManager getSplitManager() {
                throw new UnsupportedOperationException("not used by metadata lookup");
            }

            @Override
            public ConnectorPageSourceProvider getPageSourceProvider() {
                throw new UnsupportedOperationException("not used by metadata lookup");
            }
        };
        CatalogRegistry registry = new CatalogRegistry(new EmptyStore(),
                descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        registry.register(new CatalogDescriptor(CATALOG, CatalogType.CONNECTOR,
                "test-provider", CatalogState.ACTIVE, Map.of(), "", null));
        return new LazyMetadataSchema(registry, CATALOG, SCHEMA, MetadataPlanningTestSessions.session());
    }

    private record TestHandle(String name) implements ConnectorTableHandle {}

    private static final class TestMetadata implements ConnectorMetadata {
        private final List<String> lookups = new ArrayList<>();
        private final List<String> tableNames = new ArrayList<>(List.of("Orders"));
        private int listCalls;
        private boolean failSystemLookup;
        private boolean failListTables;

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of(SCHEMA);
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            listCalls++;
            if (failListTables) {
                throw new IllegalStateException("catalog listing unavailable");
            }
            return tableNames.stream().map(name -> new SchemaTableName(SCHEMA, name)).toList();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName table) {
            lookups.add(table.table());
            if (failSystemLookup && table.table().endsWith("$snapshots")) {
                throw new IllegalStateException("catalog metadata unavailable");
            }
            String base = table.table().split("\\$", 2)[0];
            boolean supported = table.table().equals(base)
                    || table.table().equals(base + "$snapshots")
                    || table.table().equals(base + "$history")
                    || table.table().equals(base + "$tags")
                    || table.table().equals(base + "$options")
                    || table.table().equals(base + "$schemas")
                    || table.table().equals(base + "$branches");
            return tableNames.contains(base) && supported ? new TestHandle(table.table()) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            String name = ((TestHandle) table).name();
            String column = name.endsWith("$snapshots") ? "snapshot_id"
                    : name.endsWith("$history") ? "made_current_at"
                    : name.endsWith("$tags") ? "tag_name"
                    : name.endsWith("$options") ? "key"
                    : name.endsWith("$schemas") ? "schema_id"
                    : name.endsWith("$branches") ? "branch_name" : "id";
            return new ConnectorTableMetadata(new SchemaTableName(SCHEMA, name),
                    List.of(column), List.of(new ConnectorTableMetadata.Column(column, PangrovaType.BIGINT)),
                    Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }
    }

    private static final class EmptyStore implements MetadataStore {
        @Override public void saveTable(org.pangrova.metadata.model.TableMetadata table) {}
        @Override public Optional<org.pangrova.metadata.model.TableMetadata> loadTable(
                String catalog, String schema, String table) { return Optional.empty(); }
        @Override public void deleteTable(String catalog, String schema, String table) {}
        @Override public List<QualifiedTableRef> searchTables(String pattern) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String name) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String catalogName) { return Optional.empty(); }
        @Override public void updateSyncTime(String catalogName, Instant syncTime) {}
        @Override public void recordSyncHistory(SyncResult result) {}
        @Override public void saveSchemas(String catalogName, List<String> schemas) {}
    }
}
