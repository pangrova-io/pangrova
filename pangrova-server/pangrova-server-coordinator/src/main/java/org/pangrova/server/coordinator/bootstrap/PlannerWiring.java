package org.pangrova.server.coordinator.bootstrap;

import java.util.List;

import org.pangrova.planner.metadata.catalog.CatalogStatisticsProvider;
import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.materialization.KernelMaterializedViewRewriteCatalog;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlannerContext;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.mv.MaterializedViewRewriteCatalog;
import org.pangrova.coordinator.dynamictable.DurableDynamicTableResolver;
import org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.statistics.StatisticsCollector;
import org.pangrova.security.api.authorization.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the SQL compiler and its metadata-aware planner: catalog statistics, the materialized-view
 * registry (recovered from the metadata store), and the plan-result cache. Extracted from
 * QueryBootstrap to keep bootstrap wiring under the 500-line quality gate.
 */
final class PlannerWiring {

    private static final Logger LOG = LoggerFactory.getLogger(PlannerWiring.class);

    private final CoordinatorBootstrapConfig config;
    private final MetadataBootstrap metadataBootstrap;
    private final org.pangrova.materialization.usecase.ReaderPinLeases readerPins;
    private final AccessControl accessControl;

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap) {
        this(config, metadataBootstrap, null, null);
    }

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap,
            org.pangrova.materialization.usecase.ReaderPinLeases readerPins) {
        this(config, metadataBootstrap, readerPins, null);
    }

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap,
            org.pangrova.materialization.usecase.ReaderPinLeases readerPins,
            AccessControl accessControl) {
        this.config = config;
        this.metadataBootstrap = metadataBootstrap;
        this.readerPins = readerPins;
        this.accessControl = accessControl;
    }

    /**
     * Builds the compiler. Without a metadata bootstrap the planner has no catalog schema, so it
     * falls back to a bare planner that can still parse and convert but cannot resolve tables.
     */
    SqlCompiler buildSqlCompiler(RuntimeFilterRegistry reg) {
        SqlCompiler compiler = new SqlCompiler(new PangrovaPlanner(),
                new org.pangrova.planner.substrait.SubstraitConverter(),
                new org.pangrova.planner.substrait.SubstraitPlanSerializer());
        if (metadataBootstrap == null) {
            return compiler;
        }
        return compiler.withSessionPlanner(
                session -> buildMetadataAwarePlanner(reg, session));
    }

    private PangrovaPlanner buildMetadataAwarePlanner(RuntimeFilterRegistry reg,
            org.pangrova.connector.api.session.ConnectorSession session) {
        var readBindings = new org.pangrova.planner.metadata.catalog.PlanningReadBindings(
                metadataBootstrap.catalogRegistry(), session);
        var rootSchema = new MetadataSchemaBuilder(
                metadataBootstrap.metadataService(), metadataBootstrap.catalogRegistry())
                .buildRootSchema(session, readBindings);
        var stats = new StatisticsCollector(
                new CatalogStatisticsProvider(readBindings,
                        metadataBootstrap.catalogRegistry().tableStatistics()));
        var mvCatalog = buildMaterializedViewRewriteCatalog(session);
        var path = session.getCatalog().map(catalog -> session.getSchema()
                .map(schema -> List.of(catalog, schema)).orElse(List.of(catalog))).orElse(List.of(""));
        DynamicTableRewriter dynamicTableRewriter = null;
        var materializations = metadataBootstrap.materializationCatalog();
        if (materializations != null) {
            String tenantId = session.getProperties().getOrDefault("tenant_id", "default");
            java.util.function.Consumer<
                    org.pangrova.materialization.api.port.PublishedMaterialization> pinner =
                    readerPin(session);
            dynamicTableRewriter = new DynamicTableRewriter(new DurableDynamicTableResolver(
                    materializations, tenantId, session.getCatalog().orElse(null),
                    session.getSchema().orElse(null), pinner),
                    DynamicTableRewriter.isInternalExecution(session));
        }
        return new PangrovaPlanner(rootSchema, path,
                new PlannerContext(stats, mvCatalog,
                        DynamicPartitionPruningConfig.DEFAULT, reg,
                        dynamicTableRewriter, null, CboConfig.DEFAULT, CostModelConfig.SSD_DEFAULT));
    }

    /**
     * Rewrite candidates are per session because authorization, tenant and default
     * schema all participate; the catalog is cheap and reads published state lazily.
     */
    private MaterializedViewRewriteCatalog buildMaterializedViewRewriteCatalog(
            org.pangrova.connector.api.session.ConnectorSession session) {
        var materializations = metadataBootstrap.materializationCatalog();
        if (materializations == null) {
            return null;
        }
        String tenantId = session.getProperties().getOrDefault("tenant_id", "default");
        return KernelMaterializedViewRewriteCatalog.create(materializations, tenantId, session,
                accessControl, java.time.Instant.now(), readerPin(session)).orElse(null);
    }

    /** Leases every published generation this admitted query resolves for execution. */
    private java.util.function.Consumer<
            org.pangrova.materialization.api.port.PublishedMaterialization> readerPin(
                    org.pangrova.connector.api.session.ConnectorSession session) {
        return new org.pangrova.coordinator.materialization.MaterializationReadPin(
                java.util.Optional.ofNullable(readerPins), session);
    }

}
