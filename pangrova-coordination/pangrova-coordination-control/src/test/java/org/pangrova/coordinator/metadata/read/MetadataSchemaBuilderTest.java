package org.pangrova.coordinator.metadata.read;

import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;

import org.pangrova.coordinator.metadata.MetadataPlanningTestSessions;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.resolution.MetadataService;
import org.pangrova.metadata.api.resolution.ResolvedTable;
import org.pangrova.metadata.api.session.MetadataSessionContext;
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

    @Test
    void repeatedNamespaceLookupsUseOneListingPerAdmittedQuery() {
        StubMetadataService service = new StubMetadataService(List.of("lake"));
        service.addSchemas("lake", List.of("live"));
        var builder = new MetadataSchemaBuilder(service);
        var firstQuery = new org.pangrova.coordinator.task.TestConnectorQuery("namespace-first");
        var nextQuery = new org.pangrova.coordinator.task.TestConnectorQuery("namespace-next");
        try {
            var first = builder.buildRootSchema(firstQuery.session("lake", "live"));
            for (int i = 0; i < 8; i++) {
                assertThat(first.getSubSchema("lake", false).getSubSchema("live", false)).isNotNull();
            }
            assertThat(service.catalogListings).isOne();
            assertThat(service.schemaListings).isOne();

            service.addSchemas("lake", List.of("live", "added"));
            var next = builder.buildRootSchema(nextQuery.session("lake", "live"));
            assertThat(next.getSubSchema("lake", false).getSubSchema("added", false)).isNotNull();
            assertThat(service.catalogListings).isEqualTo(2);
            assertThat(service.schemaListings).isEqualTo(2);
        } finally {
            firstQuery.seal();
            firstQuery.retire();
            nextQuery.seal();
            nextQuery.retire();
        }
    }

    /**
     * Minimal MetadataService for testing schema adapter without real catalog backend.
     */
    private static final class StubMetadataService implements MetadataService {
        @Override
        public java.util.Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            throw new UnsupportedOperationException("Fixture does not provide catalog deployments");
        }

        @Override
        public java.util.Optional<CatalogDescriptor> findCatalogDescriptorRevision(
                String name, String incarnation, long revision) {
            throw new UnsupportedOperationException("Fixture does not provide deployment history");
        }


        private final List<String> catalogs;
        private final java.util.Map<String, List<String>> schemas = new java.util.HashMap<>();
        private int catalogListings;
        private int schemaListings;

        StubMetadataService(List<String> catalogs) {
            this.catalogs = catalogs;
        }

        void addSchemas(String catalog, List<String> schemaNames) {
            schemas.put(catalog, schemaNames);
        }

        @Override
        public List<String> listCatalogs(MetadataSessionContext session) {
            catalogListings++;
            return catalogs;
        }

        @Override
        public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
            schemaListings++;
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
                                org.pangrova.metadata.api.object.TableMetadata metadata) { }

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
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }
    }
}
