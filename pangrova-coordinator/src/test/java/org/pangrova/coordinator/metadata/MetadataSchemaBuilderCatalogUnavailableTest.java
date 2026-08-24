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
 * Tests graceful degradation when catalog backend is unavailable.
 * Catalog is registered with empty schema list instead of failing.
 * Validates: Requirement 15.5 (fault isolation for catalog backends).
 */
class MetadataSchemaBuilderCatalogUnavailableTest {

    @Test
    void buildRootSchemaRegistersEmptyCatalogOnBackendFailure() {
        MetadataService failingService = new FailingMetadataService();
        MetadataSchemaBuilder adapter = new MetadataSchemaBuilder(failingService);

        CalciteSchema root = adapter.buildRootSchema();

        assertThat(root.plus().getSubSchema("broken_catalog")).isNotNull();
        assertThat(root.plus().getSubSchema("broken_catalog")
                .getSubSchemaNames()).isEmpty();
    }

    /**
     * MetadataService that fails on listSchemas to simulate catalog backend unavailability.
     */
    private static final class FailingMetadataService implements MetadataService {

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
