package org.pangrova.coordinator.metadata.read;

import org.pangrova.planner.metadata.catalog.LazyMetadataSchema;
import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.metadata.MetadataPlanningTestSessions;

import org.apache.calcite.schema.lookup.LikePattern;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.exception.MetadataResolutionException;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

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
    void namedLookupUsesOnlyTheAuthenticatedDeclarationWhenCalciteRequestsColumns() {
        var metadata = new TestMetadata();
        var lookup = schema(metadata).tables();
        var table = lookup.get("Orders");
        assertThat(table).isNotNull();
        assertThat(metadata.declarationCalls).isZero();
        assertThat(table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl()).getFieldNames())
                .containsExactly("id");
        assertThat(metadata.declarationCalls).isEqualTo(1);
        assertThat(lookup.getIgnoreCase("ORDERS").entity().getRowType(
                new org.apache.calcite.jdbc.JavaTypeFactoryImpl()).getFieldNames()).containsExactly("id");
        assertThat(metadata.lookups).containsExactly("Orders", "ORDERS");
        assertThat(metadata.declarationCalls).isOne();
    }

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
        var manifests = lookup.getIgnoreCase("ORDERS$MaNiFeStS");
        var files = lookup.getIgnoreCase("ORDERS$FiLeS");
        var entries = lookup.getIgnoreCase("ORDERS$EnTrIeS");
        var refs = lookup.getIgnoreCase("ORDERS$ReFs");

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
        assertThat(manifests).isNotNull();
        assertThat(manifests.name()).isEqualTo("Orders$manifests");
        assertThat(files).isNotNull();
        assertThat(files.name()).isEqualTo("Orders$files");
        assertThat(entries).isNotNull();
        assertThat(entries.name()).isEqualTo("Orders$entries");
        assertThat(refs).isNotNull();
        assertThat(refs.name()).isEqualTo("Orders$refs");
        assertThat(metadata.lookups).contains("Orders$snapshots", "Orders$history",
                "Orders$tags", "Orders$options", "Orders$schemas", "Orders$branches",
                "Orders$manifests", "Orders$files", "Orders$entries", "Orders$refs");
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
    void exactSpellingDoesNotEnumerateOrLoadUnrelatedTables() {
        TestMetadata metadata = new TestMetadata();
        for (int i = 0; i < 1_000; i++) {
            metadata.tableNames.add("Unrelated" + i);
        }
        var lookup = schema(metadata).tables();

        assertThat(lookup.getIgnoreCase("Orders").name()).isEqualTo("Orders");
        assertThat(lookup.get("Orders")).isNotNull();
        assertThat(metadata.listCalls).isZero();
        assertThat(metadata.lookups).containsExactly("Orders", "Orders");
    }

    @Test
    void calciteSchemaAccessorsDoNotLoadUnrelatedTableHandles() {
        TestMetadata metadata = new TestMetadata();
        for (int i = 0; i < 1_000; i++) {
            metadata.tableNames.add("Unrelated" + i);
        }
        var schema = schema(metadata);

        assertThat(schema.getTable("Orders")).isNotNull();
        assertThat(metadata.listCalls).isZero();
        assertThat(schema.getTableNames()).hasSize(1_001);
        assertThat(metadata.listCalls).isOne();
        assertThat(metadata.lookups).containsExactly("Orders");
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

        MetadataSchemaBuilder.invalidateTableCaches();
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

        assertThat(lookup.getIgnoreCase("ORDERS$UNSUPPORTED")).isNull();
        assertThat(lookup.get("Orders$unsupported")).isNull();
        assertThat(lookup.getIgnoreCase("MISSING$SNAPSHOTS")).isNull();
        assertThat(metadata.lookups).doesNotContain("Orders$unsupported", "Missing$snapshots");
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
        assertThat(metadata.listCalls).isEqualTo(2);
    }

    @Test
    void missingBaseTableOnlyEnumeratesOncePerResolution() {
        TestMetadata metadata = new TestMetadata();
        var lookup = schema(metadata).tables();

        assertThat(lookup.getIgnoreCase("MISSING$SNAPSHOTS")).isNull();
        assertThat(metadata.listCalls).isOne();
        assertThat(lookup.getIgnoreCase("MISSING")).isNull();
        assertThat(metadata.listCalls).isEqualTo(2);
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
        CatalogRegistry registry = new CatalogRegistry(memoryTableStores(), new EmptyStore(),
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
        private int declarationCalls;
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
        public boolean supportsExactTableNameLookup() {
            return true;
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
                    || table.table().equals(base + "$refs")
                    || table.table().equals(base + "$tags")
                    || table.table().equals(base + "$options")
                    || table.table().equals(base + "$schemas")
                    || table.table().equals(base + "$branches")
                    || table.table().equals(base + "$manifests")
                    || table.table().equals(base + "$files")
                    || table.table().equals(base + "$entries");
            return tableNames.contains(base) && supported ? new TestHandle(table.table()) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            declarationCalls++;
            String name = ((TestHandle) table).name();
            String column = name.endsWith("$snapshots") ? "snapshot_id"
                    : name.endsWith("$history") ? "made_current_at"
                    : name.endsWith("$tags") ? "tag_name"
                    : name.endsWith("$options") ? "key"
                    : name.endsWith("$schemas") ? "schema_id"
                    : name.endsWith("$branches") ? "branch_name"
                    : name.endsWith("$manifests") ? "path"
                    : name.endsWith("$files") || name.endsWith("$entries") ? "file_path" : "id";
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

    private static final class EmptyStore implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) { return Optional.empty(); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
