/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.ai.cache.SemanticCache;
import org.pangrova.coordinator.ai.index.lifecycle.VectorIndexHealthAssessor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Bootstraps and schedules the self-intelligence subsystem: semantic cache,
 * pattern recording, layout advisors, vector index health, and embedding pipeline.
 *
 * <p>Extracted from QueryBootstrap to keep that class within the 500-line limit
 * while centralising all AI lifecycle management in one place.</p>
 */
final class SelfIntelligenceBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(SelfIntelligenceBootstrap.class);

    private final CoordinatorBootstrapConfig config;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final ScheduledExecutorService schedulerExecutor;
    private final CatalogRegistry catalogRegistry;
    private final javax.sql.DataSource metadataDataSource;
    private final org.pangrova.security.secret.SecretResolver secretResolver;

    private org.pangrova.planner.advisor.QueryPatternRecorder patternRecorder;
    private SemanticCache semanticCache;
    private org.pangrova.coordinator.ai.layout.DataLayoutAdvisor layoutAdvisor;
    private org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry embeddingModelRegistry;
    private VectorIndexHealthAssessor vectorIndexHealthAssessor;
    private org.pangrova.coordinator.ai.index.lifecycle.IndexRecommendationEngine indexRecommendationEngine;
    private org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler indexRebuildScheduler;
    private org.pangrova.coordinator.ai.embedding.EmbeddingQueueCoordinator embeddingQueueCoordinator;
    private org.pangrova.coordinator.ai.embedding.PersistentEmbeddingQueueCoordinator persistentEmbeddingQueueCoordinator;

    SelfIntelligenceBootstrap(
            CoordinatorBootstrapConfig config,
            ModuleIntegrationBootstrap moduleWiring,
            ScheduledExecutorService schedulerExecutor,
            CatalogRegistry catalogRegistry,
            javax.sql.DataSource metadataDataSource,
            org.pangrova.security.secret.SecretResolver secretResolver) {
        this.config = config;
        this.moduleWiring = moduleWiring;
        this.schedulerExecutor = schedulerExecutor;
        this.catalogRegistry = catalogRegistry;
        this.metadataDataSource = metadataDataSource;
        this.secretResolver = secretResolver;
    }

    void initialize(QueryExecutionBootstrap executionWiring) {
        patternRecorder = new org.pangrova.planner.advisor.QueryPatternRecorder(10_000);
        semanticCache = new org.pangrova.coordinator.ai.cache.InMemorySemanticCache(1_000);
        layoutAdvisor = new org.pangrova.coordinator.ai.layout.DataLayoutAdvisor(patternRecorder);
        embeddingModelRegistry = createEmbeddingModelRegistry();
        vectorIndexHealthAssessor = new VectorIndexHealthAssessor();
        indexRecommendationEngine = new org.pangrova.coordinator.ai.index.lifecycle.IndexRecommendationEngine();
        initIndexRebuildScheduler();
        if (executionWiring != null) {
            executionWiring.registerEmbeddingModelTasks(embeddingModelRegistry);
            LOG.info("event=embedding_model_tasks_registered");
        }
        scheduleLayoutAdvisor();
        scheduleIndexLifecycle();
        LOG.info("event=self_intelligence_initialized semanticCacheSize={} maxTurns={} patternCapacity={}",
                1_000, 50, 10_000);
    }

    void initEmbeddingPipeline() {
        embeddingQueueCoordinator = new org.pangrova.coordinator.ai.embedding.EmbeddingQueueCoordinator();
        if (metadataDataSource != null) {
            tryInitPersistentQueue();
        }
        registerEmbeddingCommitListener();
    }

    boolean hasPersistentQueue() {
        return persistentEmbeddingQueueCoordinator != null;
    }

    void wireIntoQueryDispatcher(
            org.pangrova.coordinator.query.execution.QueryDispatcher queryDispatcher) {
        if (queryDispatcher == null) {
            return;
        }
        queryDispatcher.setSelfIntelligence(semanticCache, patternRecorder);
        wireEmbeddingProvider(queryDispatcher);
        LOG.info("event=self_intelligence_wired_into_pipeline");
    }

    /**
     * Wires an embedding provider into the query pipeline so completed queries are
     * embedded and stored in the semantic cache. Gated by the embedding feature flag;
     * the provider resolves the catalog's default embedding model lazily, so it stays
     * inert (skipping cache writes) until a model is registered — no zero-vector
     * pollution and no per-query log noise.
     */
    private void wireEmbeddingProvider(
            org.pangrova.coordinator.query.execution.QueryDispatcher queryDispatcher) {
        if (!config.embeddingEnabled()) {
            LOG.info("event=semantic_cache_embedding_disabled reason=embedding_feature_off");
            return;
        }
        if (secretResolver == null || embeddingModelRegistry == null) {
            LOG.info("event=semantic_cache_embedding_skip reason=missing_secret_resolver_or_registry");
            return;
        }
        var provider = new org.pangrova.coordinator.ai.embedding.provider.RegistryBackedEmbeddingProvider(
                embeddingModelRegistry, secretResolver);
        queryDispatcher.setEmbeddingProvider(provider);
        LOG.info("event=semantic_cache_embedding_wired note=activates_when_default_model_registered");
    }

    private org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry createEmbeddingModelRegistry() {
        if (metadataDataSource != null) {
            try {
                var jdbcRegistry = new org.pangrova.coordinator.ai.embedding.JdbcEmbeddingModelRegistry(
                        metadataDataSource, "admin", "users");
                jdbcRegistry.initSchema();
                LOG.info("event=jdbc_embedding_model_registry_initialized");
                return jdbcRegistry;
            } catch (Exception e) {
                LOG.warn("event=jdbc_embedding_model_registry_failed fallback=in_memory error={}", e.getMessage());
            }
        }
        return new org.pangrova.coordinator.ai.embedding.ConcurrentMapEmbeddingModelRegistry("admin", "users");
    }

    private void tryInitPersistentQueue() {
        try {
            var workStore = new org.pangrova.coordinator.ai.embedding.JdbcEmbeddingWorkStore(metadataDataSource);
            workStore.initSchema();
            persistentEmbeddingQueueCoordinator =
                    new org.pangrova.coordinator.ai.embedding.PersistentEmbeddingQueueCoordinator(workStore);
            persistentEmbeddingQueueCoordinator.recover();
            LOG.info("event=persistent_embedding_queue_initialized");
        } catch (Exception e) {
            LOG.warn("event=persistent_embedding_queue_failed fallback=in_memory error={}", e.getMessage());
        }
    }

    private void registerEmbeddingCommitListener() {
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        if (indexing == null || catalogRegistry == null) {
            LOG.info("event=embedding_commit_listener_skip reason=missing_indexing_or_catalog");
            return;
        }
        org.pangrova.planner.embedding.EmbeddingColumnResolver resolver =
                buildEmbeddingColumnResolver(indexing);
        org.pangrova.coordinator.ai.embedding.AutoEmbeddingCommitListener listener =
                persistentEmbeddingQueueCoordinator != null
                        ? new org.pangrova.coordinator.ai.embedding.AutoEmbeddingCommitListener(
                                resolver, persistentEmbeddingQueueCoordinator)
                        : new org.pangrova.coordinator.ai.embedding.AutoEmbeddingCommitListener(
                                resolver, embeddingQueueCoordinator);
        indexing.registerAdditionalCommitListener(listener, catalogRegistry);
        LOG.info("event=embedding_commit_listener_registered persistent={}", hasPersistentQueue());
    }

    private static org.pangrova.planner.embedding.EmbeddingColumnResolver buildEmbeddingColumnResolver(
            IndexingBootstrap indexing) {
        var store = indexing.getMetadataStore();
        if (store == null) {
            return tableName -> java.util.List.of();
        }
        return tableName -> resolveEmbeddingColumns(store, tableName);
    }

    private static java.util.List<org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor>
            resolveEmbeddingColumns(
                    org.pangrova.metadata.index.IndexMetadataStore store, String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return java.util.List.of();
        }
        try {
            return doResolveEmbeddingColumns(store, tableName);
        } catch (Exception e) {
            LOG.debug("event=embedding_column_resolve_failed table={} error={}", tableName, e.getMessage());
            return java.util.List.of();
        }
    }

    private static java.util.List<org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor>
            doResolveEmbeddingColumns(
                    org.pangrova.metadata.index.IndexMetadataStore store, String tableName) {
        String[] parts = tableName.split("\\.");
        if (parts.length < 3) {
            return java.util.List.of();
        }
        var descriptors = store.findIndexesByTable(parts[0], parts[1], parts[2]);
        var result = new java.util.ArrayList<org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor>();
        for (var desc : descriptors) {
            buildColumnDescriptor(desc).ifPresent(result::add);
        }
        return java.util.List.copyOf(result);
    }

    private static java.util.Optional<org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor>
            buildColumnDescriptor(org.pangrova.spi.index.IndexDescriptor desc) {
        if (desc.indexType() != org.pangrova.spi.index.IndexType.VECTOR) {
            return java.util.Optional.empty();
        }
        String modelName = desc.properties().get("embedding_model");
        String sourceColumnsStr = desc.properties().get("source_columns");
        if (modelName == null || sourceColumnsStr == null || desc.columnNames().isEmpty()) {
            return java.util.Optional.empty();
        }
        int dimension = parseDimension(desc.properties().get("dimension"));
        if (dimension <= 0) {
            return java.util.Optional.empty();
        }
        String modeStr = desc.properties().get("embedding_mode");
        var sourceColumns = java.util.List.of(sourceColumnsStr.split(","));
        var mode = "ASYNC".equalsIgnoreCase(modeStr)
                ? org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor.EmbeddingMode.ASYNC
                : org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor.EmbeddingMode.SYNC;
        return java.util.Optional.of(new org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor(
                desc.columnNames().get(0), modelName, sourceColumns, mode, dimension));
    }

    private static int parseDimension(String dimStr) {
        if (dimStr == null) return 0;
        try {
            return Integer.parseInt(dimStr);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private void initIndexRebuildScheduler() {
        org.pangrova.spi.coordination.DistributedLease lease = moduleWiring != null
                ? moduleWiring.getDistributedLease() : null;
        String coordId = config.coordinatorId();
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        java.util.function.BiFunction<org.pangrova.spi.index.IndexDescriptor,
                org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler.RebuildAction,
                java.util.concurrent.CompletableFuture<Void>> rebuildFn = buildRebuildFunction(indexing);
        indexRebuildScheduler = new org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler(
                schedulerExecutor, lease, coordId, vectorIndexHealthAssessor, rebuildFn);
        LOG.info("event=index_rebuild_scheduler_initialized coordinatorId={}", coordId);
    }

    private static java.util.function.BiFunction<org.pangrova.spi.index.IndexDescriptor,
            org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler.RebuildAction,
            java.util.concurrent.CompletableFuture<Void>> buildRebuildFunction(IndexingBootstrap indexing) {
        if (indexing != null && indexing.getBuildCoordinator() != null) {
            return (descriptor, action) ->
                    indexing.getBuildCoordinator()
                            .submitBuildTask(descriptor, java.util.List.of(), null, null)
                            .thenAccept(result -> {});
        }
        return (descriptor, action) -> java.util.concurrent.CompletableFuture.completedFuture(null);
    }

    private void scheduleLayoutAdvisor() {
        schedulerExecutor.scheduleWithFixedDelay(() -> {
            try {
                runLayoutAdvisorCycle();
            } catch (Exception e) {
                LOG.debug("event=layout_advisor_cycle_error", e);
            }
        }, 10, 60, TimeUnit.MINUTES);
    }

    private void runLayoutAdvisorCycle() {
        if (layoutAdvisor == null || patternRecorder == null || patternRecorder.size() == 0) {
            return;
        }
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        if (indexing == null || indexing.getMetadataStore() == null) {
            return;
        }
        LOG.debug("event=layout_advisor_cycle patternCount={}", patternRecorder.size());
        var readyIndexes = indexing.getMetadataStore()
                .findIndexesByState(org.pangrova.spi.index.IndexState.READY);
        java.util.Set<String> tableNames = new java.util.HashSet<>();
        for (var idx : readyIndexes) {
            tableNames.add(idx.catalogName() + "." + idx.schemaName() + "." + idx.tableName());
        }
        for (String tableName : tableNames) {
            layoutAdvisor.recommend(tableName).ifPresent(r ->
                    LOG.info("event=layout_recommendation table={} partitionKey={} sortKey={} sampleSize={}",
                            r.tableName(), r.recommendedPartitionKey(),
                            r.recommendedSortKey(), r.sampleSize()));
        }
    }

    private void scheduleIndexLifecycle() {
        schedulerExecutor.scheduleWithFixedDelay(() -> {
            try {
                runIndexHealthCheck();
            } catch (Exception e) {
                LOG.debug("event=index_health_assessment_error", e);
            }
        }, 15, 30, TimeUnit.MINUTES);

        schedulerExecutor.scheduleWithFixedDelay(() -> {
            try {
                runIndexRecommendation();
            } catch (Exception e) {
                LOG.debug("event=index_recommendation_error", e);
            }
        }, 20, 60, TimeUnit.MINUTES);
    }

    private void runIndexHealthCheck() {
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        if (indexing == null || indexing.getMetadataStore() == null || vectorIndexHealthAssessor == null) {
            LOG.debug("event=index_health_assessment_skip reason=missing_dependencies");
            return;
        }
        var readyIndexes = indexing.getMetadataStore().findIndexesByState(org.pangrova.spi.index.IndexState.READY);
        var staleIndexes = indexing.getMetadataStore().findIndexesByState(org.pangrova.spi.index.IndexState.STALE);
        var allVectorIndexes = collectVectorIndexes(readyIndexes, staleIndexes);
        LOG.debug("event=index_health_assessment_cycle vectorIndexCount={}", allVectorIndexes.size());
        var criticalAssessments = identifyCriticalIndexes(allVectorIndexes);
        if (!criticalAssessments.isEmpty() && indexRebuildScheduler != null) {
            int scheduled = indexRebuildScheduler.scheduleFromAssessments(criticalAssessments);
            LOG.info("event=index_health_rebuilds_scheduled count={} critical={}", scheduled,
                    criticalAssessments.size());
        }
    }

    private static List<org.pangrova.spi.index.IndexDescriptor> collectVectorIndexes(
            Iterable<org.pangrova.spi.index.IndexDescriptor> ready,
            Iterable<org.pangrova.spi.index.IndexDescriptor> stale) {
        var result = new ArrayList<org.pangrova.spi.index.IndexDescriptor>();
        for (var idx : ready) {
            if (idx.indexType() == org.pangrova.spi.index.IndexType.VECTOR) result.add(idx);
        }
        for (var idx : stale) {
            if (idx.indexType() == org.pangrova.spi.index.IndexType.VECTOR) result.add(idx);
        }
        return result;
    }

    private List<org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler.IndexWithAssessment>
            identifyCriticalIndexes(List<org.pangrova.spi.index.IndexDescriptor> descriptors) {
        var result = new ArrayList<
                org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler.IndexWithAssessment>();
        for (var descriptor : descriptors) {
            long totalFiles = Long.parseLong(descriptor.properties().getOrDefault("total_data_files", "100"));
            long indexedFiles = Long.parseLong(descriptor.properties().getOrDefault("indexed_data_files", "100"));
            var assessment = vectorIndexHealthAssessor.assess(descriptor, totalFiles, indexedFiles);
            if (assessment.level() == VectorIndexHealthAssessor.HealthLevel.CRITICAL) {
                result.add(new org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler
                        .IndexWithAssessment(descriptor, assessment));
            }
        }
        return result;
    }

    private void runIndexRecommendation() {
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        if (indexing == null || indexing.getMetadataStore() == null || indexRecommendationEngine == null) {
            LOG.debug("event=index_recommendation_skip reason=missing_dependencies");
            return;
        }
        var readyIndexes = indexing.getMetadataStore().findIndexesByState(org.pangrova.spi.index.IndexState.READY);
        int evaluated = 0;
        for (var descriptor : readyIndexes) {
            if (descriptor.indexType() != org.pangrova.spi.index.IndexType.VECTOR) {
                continue;
            }
            evaluated += evaluateIndexRecommendation(descriptor);
        }
        LOG.debug("event=index_recommendation_cycle evaluated={}", evaluated);
    }

    private int evaluateIndexRecommendation(org.pangrova.spi.index.IndexDescriptor descriptor) {
        int dimension = Integer.parseInt(descriptor.properties().getOrDefault("dimension", "128"));
        long vectorCount = Long.parseLong(descriptor.properties().getOrDefault("vector_count", "0"));
        long availableMemory = Long.parseLong(
                descriptor.properties().getOrDefault("memory_budget", String.valueOf(1024L * 1024L * 1024L)));
        var recommendation = indexRecommendationEngine.evaluateExisting(
                descriptor, vectorCount, dimension, availableMemory);
        if (recommendation.isPresent()) {
            LOG.info("event=algorithm_change_recommended indexId={} newAlgorithm={} reason={}",
                    descriptor.indexId(), recommendation.get().algorithm(), recommendation.get().reason());
        }
        return 1;
    }
}
