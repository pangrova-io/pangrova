package org.pangrova.coordinator.bootstrap;

import org.pangrova.persistence.jdbc.query.transaction.JdbcWriteCommitJournal;

import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;

import javax.sql.DataSource;

import org.pangrova.coordinator.ai.embedding.DurableGeneratedValueBuildService;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner;
import org.pangrova.coordinator.feature.DurableFeatureExportService;
import org.pangrova.coordinator.feature.FeatureViewStatementService;
import org.pangrova.coordinator.feature.FeatureSourceCompiler;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.ai.control.assembly.AiInfrastructureStores;
import org.pangrova.persistence.api.generation.job.GenerationJobStore;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.spi.security.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/** Wires durable generated-value and feature-view materialization services. */
final class QueryMaterializationWiring {

    private static final Logger LOG = LoggerFactory.getLogger(QueryMaterializationWiring.class);

    private final Dependencies dependencies;
    private final GenerationStores artifactStores;

    QueryMaterializationWiring(Dependencies dependencies) {
        this.dependencies = dependencies;
        artifactStores = new GenerationStores(dependencies.modelAuthority());
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
                dependencies.config().baseIndexPath(), catalog);
        var coordinator = new GeneratedValueMaterializationCoordinator(catalog, service,
                dependencies.metadataDataSource(), new JdbcWriteCommitJournal(
                        dependencies.metadataDataSource()), dependencies.modelAuthority());
        var registeredCatalogs = indexing.registerDurableCommitProcessor(coordinator);
        coordinator.activateCatalogs(registeredCatalogs);
        LOG.info("event=generated_value_materialization_wired catalogs={}", registeredCatalogs);
        return coordinator;
    }

    void bindGeneratedValueReads(
            org.pangrova.coordinator.bootstrap.module.DefaultModuleContext moduleContext,
            CatalogRegistry catalog) {
        var indexedReads = moduleContext.optional(
                org.pangrova.coordinator.adapter.indexing.IndexAwareSplitFetcher.class);
        if (indexedReads != null && dependencies.artifactBuildStore() != null) {
            indexedReads.bindGeneratedValueReads(new org.pangrova.coordinator.generation.GeneratedValueReadResolver(
                    catalog, dependencies.artifactBuildStore(),
                    artifactStores.generatedPublications(dependencies.metadataDataSource())));
        }
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
                dependencies.splitAssignmentManager(), dependencies.config().spillBudget(),
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
