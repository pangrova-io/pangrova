package org.pangrova.server.coordinator.bootstrap;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementCompletionEvent;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.enums.IndexState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class QueryAssemblyWiringTest {

    @Test
    void queryOnlyRoleConnectsItsActualIndexStoreToQueryReads() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("query_index_wiring");
        var store = new JdbcIndexMetadataStore(dataSource, CoordinatorTableWriteAuthority.create()::requireCurrent);
        var now = Instant.parse("2026-01-01T00:00:00Z");
        store.registerIndex(new IndexDescriptor("query-index", "by_value", "c", "s", "t",
                List.of("value"), IndexType.BITMAP, Map.of(), IndexState.READY,
                null, 0, null, null, now, now, "tenant"));
        var catalogs = new CatalogRegistry(
                memoryTableStores(), new PostgresCatalogDeploymentRepository(dataSource));
        try (var modules = new ModuleIntegrationBootstrap()) {
            modules.setCatalogRegistry(catalogs);
            modules.startIndexQuery(store, null, ModelAuthorityFixtures.artifacts().builds(dataSource));
            assertThat(modules.getIndexingBootstrap()).isNull();
            var reads = QueryBootstrap.createIndexedReads(modules, catalogs);
            assertThat(reads).isNotNull();
            assertThat(reads.isIndexFilteringEnabled()).isTrue();
            assertThat(reads.lookupIndexes("tenant", "c.s.t", "value"))
                    .extracting(IndexDescriptor::indexId).containsExactly("query-index");
            assertThat(reads.lookupIndexes("another-tenant", "c.s.t", "value")).isEmpty();
        }
    }

    private static StatementCompletionEvent event(StatementCategory category, String table) {
        var statement = new CreateTableStatement(Identifier.of("c", "s", "t"), List.of(), false,
                List.of(), List.of(), Optional.empty(), Map.of());
        return new StatementCompletionEvent("query", statement,
                new StatementResult(List.of(), List.of(), table, category), null);
    }
}
