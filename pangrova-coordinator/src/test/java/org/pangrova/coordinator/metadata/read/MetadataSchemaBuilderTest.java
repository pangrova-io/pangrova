package org.pangrova.coordinator.metadata.read;

import org.pangrova.coordinator.metadata.MetadataPlanningTestSessions;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.MetadataGateway;
import org.pangrova.metadata.api.binding.QueryMetadataLifecyclePort;
import org.pangrova.metadata.api.binding.TableResolutionPort;
import org.pangrova.metadata.api.catalog.CatalogLifecyclePort;
import org.pangrova.metadata.api.control.ColumnTagPort;
import org.pangrova.metadata.api.mutation.TableDdlPort;
import org.pangrova.metadata.api.binding.ResolvedTable;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests MetadataSchemaAdapter bridging MetadataService to Calcite schema.
 * Validates: Requirements 15.2, 15.5.
 */
class MetadataSchemaBuilderTest {

    @Test
    void buildRootSchemaWithNoCatalogs() {
        var stub = new StubMetadataService(List.of());
        MetadataGateway emptyService = new MetadataGateway(stub, stub, stub, stub, stub);
        MetadataSchemaBuilder adapter = new MetadataSchemaBuilder(emptyService);

        CalciteSchema schema = adapter.buildRootSchema(MetadataPlanningTestSessions.session());

        assertThat(schema).isNotNull();
        assertThat(schema.getSubSchemaMap()).isEmpty();
    }

    @Test
    void buildRootSchemaWithCatalogAndSchemas() {
        var stub = new StubMetadataService(List.of("iceberg_catalog"));
        MetadataGateway service = new MetadataGateway(stub, stub, stub, stub, stub);
        stub.addSchemas("iceberg_catalog", List.of("default", "analytics"));
        MetadataSchemaBuilder adapter = new MetadataSchemaBuilder(service);

        CalciteSchema schema = adapter.buildRootSchema(MetadataPlanningTestSessions.session());

        assertThat(schema).isNotNull();
        // Catalog is registered as a sub-schema
        assertThat(schema.getSubSchemaMap()).containsKey("iceberg_catalog");
        // Schemas are registered under the catalog
        CalciteSchema catalogSchema = schema.getSubSchema("iceberg_catalog", false);
        assertThat(catalogSchema).isNotNull();
        assertThat(catalogSchema.getSubSchemaMap()).containsKey("default");
        assertThat(catalogSchema.getSubSchemaMap()).containsKey("analytics");
    }

    /**
     * Minimal MetadataService for testing schema adapter without real catalog backend.
     */
    private static final class StubMetadataService implements CatalogLifecyclePort, TableDdlPort, TableResolutionPort, QueryMetadataLifecyclePort, ColumnTagPort {
    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
            String columnName, String tagKey, String value) {}

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
            String columnName, String tagKey) {}

    @Override
    public java.util.Map<String, String> listTags(
            MetadataSessionContext session, QualifiedTableRef tableRef, String columnName) {
        return java.util.Map.of();
    }

    @Override
    public int clearTableTags(MetadataSessionContext session, java.util.UUID tableId) {
        return 0;
    }

    @Override
    public int renameTableTags(MetadataSessionContext session,
            QualifiedTableRef from, QualifiedTableRef to) {
        return 0;
    }

    @Override
    public Optional<CatalogDescriptor> getCatalogDescriptor(String catalogName) {
        return Optional.empty();
    }

    @Override
    public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String catalogName, String incarnationId, long revision) {
        return Optional.empty();
    }


        private final List<String> catalogs;
        private final java.util.Map<String, List<String>> schemas = new java.util.HashMap<>();

        StubMetadataService(List<String> catalogs) {
            this.catalogs = catalogs;
        }

        void addSchemas(String catalog, List<String> schemaNames) {
            schemas.put(catalog, schemaNames);
        }

        @Override
        public List<String> listCatalogs(MetadataSessionContext session) {
            return catalogs;
        }

        @Override
        public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
            return schemas.getOrDefault(catalogName, List.of());
        }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade) { }

        @Override
        public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
            return null;
        }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef,
                                org.pangrova.metadata.api.schema.TableMetadata metadata) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void beginQuery(org.pangrova.metadata.api.session.MetadataSessionContext context) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }
    }
}
