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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Catalog backend failures must not become a successful empty catalog lookup.
 */
class MetadataSchemaBuilderCatalogUnavailableTest {

    @Test
    void dynamicSchemaLookupPropagatesBackendFailure() {
        MetadataService failingService = new FailingMetadataService();
        MetadataSchemaBuilder adapter = new MetadataSchemaBuilder(failingService);

        CalciteSchema root = adapter.buildRootSchema(MetadataPlanningTestSessions.session());
        assertThatThrownBy(() -> root.plus().getSubSchema("broken_catalog").getSubSchemaNames())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Connection refused");
    }

    /**
     * MetadataService that fails on listSchemas to simulate catalog backend unavailability.
     */
    private static final class FailingMetadataService implements MetadataService {
        @Override
        public java.util.Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            throw new UnsupportedOperationException("Fixture does not provide catalog deployments");
        }

        @Override
        public java.util.Optional<CatalogDescriptor> findCatalogDescriptorRevision(
                String name, String incarnation, long revision) {
            throw new UnsupportedOperationException("Fixture does not provide deployment history");
        }


        @Override
        public List<String> listCatalogs(MetadataSessionContext session) {
            return List.of("broken_catalog");
        }

        @Override
        public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
            throw new RuntimeException("Connection refused to catalog backend");
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
