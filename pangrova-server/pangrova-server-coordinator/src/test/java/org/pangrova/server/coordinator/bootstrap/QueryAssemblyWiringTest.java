package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementCompletionEvent;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.cache.CacheEntryMetadata;
import org.pangrova.planner.core.cache.CacheHint;
import org.pangrova.planner.core.cache.CacheKey;
import org.pangrova.cache.api.CachedResult;
import org.pangrova.cache.memory.InMemoryL2CacheStore;
import org.pangrova.planner.core.cache.QueryResultCache;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.lifecycle.IndexState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class QueryAssemblyWiringTest {
    @Test
    void actualStatementListenersInvalidateMutatedTablesAndSchemaChanges() throws Exception {
        try (var cache = new QueryResultCache(new InMemoryL2CacheStore())) {
            var compiler = new SqlCompiler().withQueryResultCache(cache);
            var schemaEvents = new AtomicInteger();
            var listeners = QueryExecutionBootstrap
                    .statementListeners(compiler, event -> schemaEvents.incrementAndGet(), null)
                    .listeners();
            var first = CacheKey.of("select * from c.s.first", "tenant", "reader", Map.of(), "policy");
            var second = CacheKey.of("select * from c.s.second", "tenant", "reader", Map.of(), "policy");
            cache.put(first, new CachedResult(new byte[]{1}, Instant.now()),
                    new CacheEntryMetadata(Set.of("c.s.first"), "reader", ""));
            cache.put(second, new CachedResult(new byte[]{2}, Instant.now()),
                    new CacheEntryMetadata(Set.of("c.s.second"), "reader", ""));

            listeners.forEach(listener -> listener.onCompleted(event(StatementCategory.DML, "c.s.first")));
            assertThat(cache.get(first, CacheHint.NONE)).isEmpty();
            assertThat(cache.get(second, CacheHint.NONE)).isPresent();
            listeners.forEach(listener -> listener.onCompleted(event(StatementCategory.DDL, null)));
            assertThat(cache.get(second, CacheHint.NONE)).isEmpty();
            assertThat(schemaEvents.get()).isEqualTo(2);
        }
    }

    @Test
    void queryOnlyRoleConnectsItsActualIndexStoreToQueryReads() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("query_index_wiring");
        var store = new JdbcIndexMetadataStore(dataSource, org.pangrova.metadata.service.TableControlWriteAuthorities.postgres()::requireCurrent);
        var now = Instant.parse("2026-01-01T00:00:00Z");
        store.registerIndex(new IndexDescriptor("query-index", "by_value", "c", "s", "t",
                List.of("value"), IndexType.BITMAP, Map.of(), IndexState.READY,
                null, 0, null, null, now, now, "tenant"));
        try (var modules = new ModuleIntegrationBootstrap()) {
            modules.startIndexQuery(store, null);
            assertThat(modules.getIndexingBootstrap()).isNull();
            var reads = QueryBootstrap.createIndexedReads(modules,
                    new CatalogRegistry(new PostgresCatalogDeploymentRepository(dataSource)));
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
