package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;

import org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;

import javax.sql.DataSource;

import org.pangrova.coordinator.ai.embedding.DurableGeneratedValueBuildService;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner;
import org.pangrova.coordinator.feature.DurableFeatureExportService;
import org.pangrova.coordinator.feature.FeatureViewStatementService;
import org.pangrova.coordinator.feature.FeatureSourceCompiler;
import org.pangrova.coordinator.scheduler.placement.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.ai.postgresql.assembly.AiInfrastructureStores;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.security.api.authorization.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/** Wires durable generated-value and feature-view materialization services. */
final class QueryMaterializationWiring {

    private static final Logger LOG = LoggerFactory.getLogger(QueryMaterializationWiring.class);

    private final Dependencies dependencies;
    private final PostgresGenerationStores artifactStores;

    QueryMaterializationWiring(Dependencies dependencies) {
        this.dependencies = dependencies;
        artifactStores = new PostgresGenerationStores(dependencies.modelAuthority(),
                CoordinatorTableWriteAuthority.create());
    }

    GeneratedValueMaterializationCoordinator createGeneratedValue(CatalogRegistry catalog) {
        IndexingBootstrap indexing = dependencies.moduleWiring() == null
                ? null : dependencies.moduleWiring().getIndexingBootstrap();
        if (catalog == null || dependencies.metadataDataSource() == null
                || dependencies.artifactBuildStore() == null || indexing == null) {
            LOG.info("event=generated_value_materialization_inactive "
                    + "reason=durable_commit_processor_unavailable");
            return null;
        }
        var submissions = artifactStores.generatedSubmissions(dependencies.metadataDataSource());
        var service = new DurableGeneratedValueBuildService(
                dependencies.artifactBuildStore(), submissions,
                new SnapshotGenerationSplitPlanner(catalog), workerSelector(),
                dependencies.config().baseIndexPath(), catalog, new OtelExecutionTraceContextFactory());
        var coordinator = new GeneratedValueMaterializationCoordinator(catalog, service,
                dependencies.metadataDataSource(), new JdbcWriteCommitJournal(
                        dependencies.metadataDataSource()), dependencies.modelAuthority(),
                new org.pangrova.persistence.security.postgresql.tenant.PostgresDeferredTenantAuthority(),
                org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate::lockPublication,
                CoordinatorTableWriteAuthority.create());
        var registeredCatalogs = indexing.registerDurableCommitProcessor(coordinator);
        coordinator.activateCatalogs(registeredCatalogs);
        LOG.info("event=generated_value_materialization_wired catalogs={}", registeredCatalogs);
        return coordinator;
    }

    org.pangrova.coordinator.generation.read.GeneratedValueReadResolver createGeneratedValueReads(
            CatalogRegistry catalog) {
        if (dependencies.artifactBuildStore() == null) {
            return null;
        }
        java.util.Objects.requireNonNull(catalog, "Published generation reads require the catalog registry");
        java.util.Objects.requireNonNull(dependencies.metadataDataSource(),
                "Published generation reads require their configured persistence provider");
        return new org.pangrova.coordinator.generation.read.GeneratedValueReadResolver(
                catalog, dependencies.artifactBuildStore(),
                artifactStores.generatedPublications(dependencies.metadataDataSource()));
    }

    Function<FeatureSourceCompiler, FeatureViewStatementService> createFeatureView(CatalogRegistry catalog) {
        if (catalog == null || dependencies.artifactBuildStore() == null
                || dependencies.aiInfrastructureStores() == null
                || dependencies.moduleWiring() == null
                || dependencies.moduleWiring().getIndexingBootstrap() == null) {
            return null;
        }
        var exportService = new DurableFeatureExportService(
                dependencies.aiInfrastructureStores().catalogExports(),
                new SnapshotGenerationSplitPlanner(catalog),
                dependencies.config().baseIndexPath(), catalog);
        return compiler -> new FeatureViewStatementService(
                dependencies.aiInfrastructureStores().features(), exportService,
                dependencies.accessControl(), compiler);
    }

    private DefaultWorkerSelector workerSelector() {
        return new DefaultWorkerSelector(
                dependencies.workerManager(), dependencies.taskScheduler(),
                dependencies.splitAssignmentManager().cacheAffinityAssigner(), dependencies.config().spillBudget(),
                dependencies.embeddingModelRegistry());
    }

    record Dependencies(
            CoordinatorBootstrapConfig config,
            ModuleIntegrationBootstrap moduleWiring,
            DataSource metadataDataSource,
            GenerationJobStore artifactBuildStore,
            WorkerManager workerManager,
            TaskScheduler taskScheduler,
            SplitAssignmentManager splitAssignmentManager,
            AiInfrastructureStores aiInfrastructureStores,
            AccessControl accessControl,
            org.pangrova.ai.control.embedding.EmbeddingModelRegistry
                    embeddingModelRegistry,
            CurrentModelAuthority modelAuthority) {
    }
}
