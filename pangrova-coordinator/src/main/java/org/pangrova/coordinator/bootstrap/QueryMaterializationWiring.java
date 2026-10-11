package org.pangrova.coordinator.bootstrap;

import javax.sql.DataSource;

import org.pangrova.coordinator.ai.embedding.DurableGeneratedValueBuildService;
import org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator;
import org.pangrova.coordinator.artifact.SnapshotArtifactSplitPlanner;
import org.pangrova.coordinator.feature.DurableFeatureExportService;
import org.pangrova.coordinator.feature.FeatureViewStatementService;
import org.pangrova.coordinator.feature.FeatureSourceCompiler;
import org.pangrova.coordinator.query.execution.dispatch.dag.DefaultWorkerSelector;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.ai.AiInfrastructureStores;
import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.spi.security.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/** Wires durable generated-value and feature-view materialization services. */
final class QueryMaterializationWiring {

    private static final Logger LOG = LoggerFactory.getLogger(QueryMaterializationWiring.class);

    private final Dependencies dependencies;

    QueryMaterializationWiring(Dependencies dependencies) {
        this.dependencies = dependencies;
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
        var metadataStore = new JdbcGeneratedValueArtifactPublicationStore(
                dependencies.metadataDataSource());
        var service = new DurableGeneratedValueBuildService(
                dependencies.artifactBuildStore(), metadataStore,
                new SnapshotArtifactSplitPlanner(catalog), workerSelector(),
                dependencies.config().baseIndexPath(), catalog);
        var coordinator = new GeneratedValueMaterializationCoordinator(catalog, service,
                dependencies.metadataDataSource(), new org.pangrova.metadata.coordination.JdbcWriteCommitJournal(
                        dependencies.metadataDataSource()));
        var registeredCatalogs = indexing.registerDurableCommitProcessor(coordinator);
        coordinator.activateCatalogs(registeredCatalogs);
        LOG.info("event=generated_value_materialization_wired catalogs={}", registeredCatalogs);
        return coordinator;
    }

    void bindGeneratedValueReads(
            org.pangrova.coordinator.bootstrap.module.DefaultModuleContext moduleContext, CatalogRegistry catalog) {
        var indexedReads = moduleContext.optional(org.pangrova.coordinator.index.IndexAwareSplitFetcher.class);
        if (indexedReads != null && dependencies.artifactBuildStore() != null) {
            indexedReads.bindGeneratedValueReads(new org.pangrova.coordinator.artifact.GeneratedValueReadResolver(
                    catalog, dependencies.artifactBuildStore(),
                    new JdbcGeneratedValueArtifactPublicationStore(dependencies.metadataDataSource())));
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
                dependencies.artifactBuildStore(),
                new SnapshotArtifactSplitPlanner(catalog),
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
            ArtifactBuildStore artifactBuildStore,
            WorkerManager workerManager,
            TaskScheduler taskScheduler,
            SplitAssignmentManager splitAssignmentManager,
            AiInfrastructureStores aiInfrastructureStores,
            AccessControl accessControl,
            org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry
                    embeddingModelRegistry) {
    }
}
