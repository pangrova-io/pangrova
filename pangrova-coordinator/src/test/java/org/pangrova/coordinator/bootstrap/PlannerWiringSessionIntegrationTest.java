package org.pangrova.coordinator.bootstrap;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.store.jdbc.JdbcMetadataStore;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.spi.connector.ConnectorFactory;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.security.UserIdentity;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PostgreSQL directory and plugin/bootstrap chain; no metadata or planner substitutes. */
class PlannerWiringSessionIntegrationTest {
    @TempDir Path warehouse;

    @Test
    void startupServicesNeedNoQueryIdentityAndSchemasRemainTenantScopedAndDynamic() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("planner_scope");
        var store = new JdbcMetadataStore(dataSource);
        store.saveCatalogDescriptor(new CatalogDescriptor("scoped_lance", CatalogType.CONNECTOR,
                "lance", CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString()), "", Set.of()));
        var factory = new PluginConnectorHandleFactory(ConnectorFactory.class.getClassLoader(),
                new SecretResolver(), ignored -> Optional.empty(), descriptor -> Optional.of(
                        new org.pangrova.metadata.store.jdbc.source.JdbcManagedSourceCatalog(dataSource,
                                new org.pangrova.spi.connector.metadata.ManagedSourceCatalog.Deployment(
                                        descriptor.name(), descriptor.incarnationId(), descriptor.revision()),
                                ignored -> { throw new IllegalStateException("No write execution in this read fixture"); },
                                new org.pangrova.query.transaction.JdbcSourcePublicationTransaction())));
        MetadataBootstrap metadata = MetadataBootstrap.initialize(store, factory);
        try {
            var context = new QueryModuleLoader(CoordinatorBootstrapConfig.withDefaults("scope-test", 0),
                    metadata, null, null).load();
            SqlCompiler template = context.require(SqlCompiler.class);
            assertThat(context.require(QueryCacheService.class)).isNotNull();
            assertThat(QueryCacheService.resolveCache(template)).isSameAs(template.queryResultCache());
            assertThatThrownBy(() -> template.compileToResult("SELECT 1"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("forSession");
            assertDispatcherStartup(context, metadata, template);
            assertDynamicTenantSchemas(metadata, template);
            assertRealBackendErrorsPropagate(dataSource, metadata);
            template.queryResultCache().close();
        } finally {
            metadata.shutdown();
            factory.shutdown();
        }
    }

    private static void assertDispatcherStartup(
            org.pangrova.coordinator.bootstrap.module.DefaultModuleContext context,
            MetadataBootstrap metadata, SqlCompiler template) {
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        try (var dispatcher = new RemoteTaskDispatcher(
                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                org.pangrova.testing.TestObservability.TRACING)) {
            var deps = QueryDispatcherContext.withDefaults(template, null, null, null, dispatcher,
                    context.require(org.pangrova.coordinator.scheduler.TaskScheduler.class), workers,
                    new CoordinatorTaskTracker(), metadata.catalogRegistry(), null, null, 0L);
            assertThatCode(() -> new QueryDispatcher(deps, null, null)).doesNotThrowAnyException();
        }
    }

    private static void assertDynamicTenantSchemas(MetadataBootstrap metadata, SqlCompiler template) {
        ConnectorSession alpha = session("alpha", "alice");
        ConnectorSession beta = session("beta", "bob");
        var handle = metadata.catalogRegistry().getCatalog("scoped_lance");
        var alphaRoot = metadata.buildCalciteSchema(alpha);
        var betaRoot = metadata.buildCalciteSchema(beta);
        var alphaCatalog = alphaRoot.getSubSchema("scoped_lance", false);
        var betaCatalog = betaRoot.getSubSchema("scoped_lance", false);
        assertThat(alphaCatalog.getSubSchemaMap()).isEmpty();
        handle.getMetadata(alpha).createSchema(alpha, "alpha_only");
        assertThat(alphaCatalog.getSubSchemaMap()).containsOnlyKeys("alpha_only");
        assertThat(betaCatalog.getSubSchemaMap()).isEmpty();
        handle.getMetadata(beta).createSchema(beta, "beta_only");
        assertThat(betaCatalog.getSubSchemaMap()).containsOnlyKeys("beta_only");
        assertThat(alphaCatalog.getSubSchemaMap()).containsOnlyKeys("alpha_only");
        var alphaCompiler = template.forSession(alpha);
        var betaCompiler = template.forSession(beta);
        assertThat(alphaCompiler.planner()).isNotSameAs(betaCompiler.planner());
        assertThat(alphaCompiler.queryResultCache()).isSameAs(betaCompiler.queryResultCache());
        assertThat(alphaCompiler.forSession(alpha)).isSameAs(alphaCompiler);
        assertThatThrownBy(() -> alphaCompiler.forSession(beta))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another session");
        assertThat(alphaCompiler.compileToPlannedQuery("SELECT 1")).isNotNull();
        assertThat(alphaCompiler.subPlanReoptimizer()).isNotNull();
        var missing = SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "missing-tenant", Map.of("tenant_id", "forged", "pangrova.tenant_id", "forged"),
                new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), null));
        assertThatThrownBy(() -> template.forSession(missing))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tenant_id");
    }

    private static ConnectorSession session(String tenant, String user) {
        return SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "scope-" + tenant, Map.of("catalog", "scoped_lance"),
                new UserIdentity(user, Set.of(), Map.of(), Instant.EPOCH), tenant));
    }

    private static void assertRealBackendErrorsPropagate(
            javax.sql.DataSource dataSource, MetadataBootstrap metadata) throws Exception {
        var session = session("alpha", "alice");
        var root = metadata.buildCalciteSchema(session);
        var catalog = root.getSubSchema("scoped_lance", false);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE managed_source_schemas RENAME TO temporarily_unavailable_schemas");
            try {
                assertThatThrownBy(catalog::getSubSchemaMap)
                        .isInstanceOf(RuntimeException.class).hasRootCauseInstanceOf(java.sql.SQLException.class);
            } finally {
                statement.execute("ALTER TABLE temporarily_unavailable_schemas RENAME TO managed_source_schemas");
            }
            statement.execute("ALTER TABLE managed_source_heads RENAME TO temporarily_unavailable_tables");
            try {
                var collector = new org.pangrova.planner.stats.StatisticsCollector(
                        new org.pangrova.coordinator.metadata.read.CatalogStatisticsProvider(
                                new org.pangrova.coordinator.metadata.read.PlanningReadBindings(
                                        metadata.catalogRegistry(), session)));
                assertThatThrownBy(() -> collector.getStatistics(new QualifiedTableName("scoped_lance", "alpha_only", "items")))
                        .isInstanceOf(org.pangrova.metadata.api.error.MetadataResolutionException.class)
                        .hasRootCauseInstanceOf(java.sql.SQLException.class);
            } finally {
                statement.execute("ALTER TABLE temporarily_unavailable_tables RENAME TO managed_source_heads");
            }
        }
    }
}
