package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.service.ResolvedTable;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests MetadataSchemaAdapter bridging MetadataService to Calcite schema.
 * Validates: Requirements 15.2, 15.5.
 */
class MetadataSchemaBuilderTest {

    @Test
    void buildRootSchemaWithNoCatalogs() {
        MetadataService emptyService = new StubMetadataService(List.of());
        MetadataSchemaBuilder adapter = new MetadataSchemaBuilder(emptyService);

        CalciteSchema schema = adapter.buildRootSchema(MetadataPlanningTestSessions.session());

        assertThat(schema).isNotNull();
        assertThat(schema.getSubSchemaMap()).isEmpty();
    }

    @Test
    void buildRootSchemaWithCatalogAndSchemas() {
        StubMetadataService service = new StubMetadataService(List.of("iceberg_catalog"));
        service.addSchemas("iceberg_catalog", List.of("default", "analytics"));
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
    private static final class StubMetadataService implements MetadataService {

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
                                org.pangrova.metadata.model.TableMetadata metadata) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void beginQuery(String queryId) { }

        /** Intentional no-op: test only exercises listCatalogs/listSchemas. */
        @Override
        public void cleanupQuery(String queryId) { }
    }
}
