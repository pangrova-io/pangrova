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

import org.pangrova.coordinator.indexing.ai.lifecycle.IndexRecommendationEngine;
import org.pangrova.coordinator.indexing.ai.lifecycle.VectorIndexHealthAssessor;
import org.pangrova.spi.indexing.state.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Bootstraps and schedules control-plane workload analysis, vector-index health,
 * and durable embedding jobs. Data-plane AI and retrieval never execute here.
 *
 * <p>Extracted from QueryBootstrap to keep that class within the 500-line limit
 * while centralising all AI lifecycle management in one place.</p>
 */
final class SelfIntelligenceBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(SelfIntelligenceBootstrap.class);
    private static final int PATTERN_CAPACITY = 10_000;
    private static final long LAYOUT_INITIAL_DELAY_MINUTES = 10;
    private static final long LAYOUT_INTERVAL_MINUTES = 60;
    private static final long INDEX_HEALTH_INITIAL_DELAY_MINUTES = 15;
    private static final long INDEX_HEALTH_INTERVAL_MINUTES = 30;
    private static final long INDEX_RECOMMENDATION_INITIAL_DELAY_MINUTES = 20;
    private static final long INDEX_RECOMMENDATION_INTERVAL_MINUTES = 60;

    private final CoordinatorBootstrapConfig config;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final ScheduledExecutorService schedulerExecutor;
    private final org.pangrova.ai.control.embedding.EmbeddingModelRegistry
            embeddingModelRegistry;

    private org.pangrova.planner.advisor.QueryPatternRecorder patternRecorder;
    private org.pangrova.coordinator.ai.layout.DataLayoutAdvisor layoutAdvisor;
    private VectorIndexHealthAssessor vectorIndexHealthAssessor;
    private org.pangrova.coordinator.indexing.ai.lifecycle.IndexRecommendationEngine indexRecommendationEngine;
    private org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler indexRebuildScheduler;
    private java.util.concurrent.ScheduledFuture<?> layoutFuture;
    private java.util.concurrent.ScheduledFuture<?> healthFuture;
    private java.util.concurrent.ScheduledFuture<?> recommendationFuture;

    SelfIntelligenceBootstrap(
            CoordinatorBootstrapConfig config,
            ModuleIntegrationBootstrap moduleWiring,
            ScheduledExecutorService schedulerExecutor,
            org.pangrova.ai.control.embedding.EmbeddingModelRegistry
                    embeddingModelRegistry) {
        this.config = config;
        this.moduleWiring = moduleWiring;
        this.schedulerExecutor = schedulerExecutor;
        this.embeddingModelRegistry = embeddingModelRegistry;
    }

    void initialize(QueryExecutionBootstrap executionWiring) {
        patternRecorder = new org.pangrova.planner.advisor.QueryPatternRecorder(PATTERN_CAPACITY);
        layoutAdvisor = new org.pangrova.coordinator.ai.layout.DataLayoutAdvisor(patternRecorder);
        vectorIndexHealthAssessor = new VectorIndexHealthAssessor();
        indexRecommendationEngine = new IndexRecommendationEngine();
        initIndexRebuildScheduler();
        if (executionWiring != null && embeddingModelRegistry != null) {
            executionWiring.registerEmbeddingModelTasks(embeddingModelRegistry);
            LOG.info("event=embedding_model_tasks_registered");
        }
        scheduleLayoutAdvisor();
        scheduleIndexLifecycle();
        LOG.info("event=self_intelligence_initialized patternCapacity={}", PATTERN_CAPACITY);
    }

    void wireIntoQueryDispatcher(
            org.pangrova.coordinator.query.execution.QueryDispatcher queryDispatcher) {
        if (queryDispatcher == null) {
            return;
        }
        queryDispatcher.setSelfIntelligence(patternRecorder);
        LOG.info("event=self_intelligence_wired_into_pipeline");
    }

    private void initIndexRebuildScheduler() {
        org.pangrova.persistence.api.coordination.DistributedLease lease = moduleWiring != null
                ? moduleWiring.getDistributedLease() : null;
        String coordId = config.coordinatorId();
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        java.util.function.BiFunction<org.pangrova.spi.indexing.descriptor.IndexDescriptor,
                org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler.RebuildAction,
                java.util.concurrent.CompletableFuture<Void>> rebuildFn = buildRebuildFunction(indexing);
        indexRebuildScheduler = new org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler(
                schedulerExecutor, lease, coordId, vectorIndexHealthAssessor, rebuildFn);
        LOG.info("event=index_rebuild_scheduler_initialized coordinatorId={}", coordId);
    }

    static java.util.function.BiFunction<org.pangrova.spi.indexing.descriptor.IndexDescriptor,
            org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler.RebuildAction,
            java.util.concurrent.CompletableFuture<Void>> buildRebuildFunction(IndexingBootstrap indexing) {
        if (indexing == null || indexing.getDdlExecutor() == null) {
            return (descriptor, action) -> java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("durable Native AI index build service is not configured"));
        }
        return (descriptor, action) -> java.util.concurrent.CompletableFuture.failedFuture(
                new org.pangrova.spi.security.AccessDeniedException(
                        "Scheduled index rebuild requires current trusted authorization for "
                                + descriptor.indexId() + "; a service principal cannot be synthesized"));
    }

    private void scheduleLayoutAdvisor() {
        layoutFuture = schedulerExecutor.scheduleWithFixedDelay(() -> {
            try {
                runLayoutAdvisorCycle();
            } catch (Exception e) {
                LOG.debug("event=layout_advisor_cycle_error", e);
            }
        }, LAYOUT_INITIAL_DELAY_MINUTES, LAYOUT_INTERVAL_MINUTES, TimeUnit.MINUTES);
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
                .findIndexesByState(IndexState.READY);
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
        healthFuture = schedulerExecutor.scheduleWithFixedDelay(() -> {
            try {
                runIndexHealthCheck();
            } catch (Exception e) {
                LOG.debug("event=index_health_assessment_error", e);
            }
        }, INDEX_HEALTH_INITIAL_DELAY_MINUTES, INDEX_HEALTH_INTERVAL_MINUTES, TimeUnit.MINUTES);

        recommendationFuture = schedulerExecutor.scheduleWithFixedDelay(() -> {
            try {
                runIndexRecommendation();
            } catch (Exception e) {
                LOG.debug("event=index_recommendation_error", e);
            }
        }, INDEX_RECOMMENDATION_INITIAL_DELAY_MINUTES,
                INDEX_RECOMMENDATION_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    /** Stops periodic admission; the shared scheduler must still join its entered callbacks. */
    void stopScheduling() {
        if (layoutFuture != null) layoutFuture.cancel(false);
        if (healthFuture != null) healthFuture.cancel(false);
        if (recommendationFuture != null) recommendationFuture.cancel(false);
    }

    private void runIndexHealthCheck() {
        IndexingBootstrap indexing = moduleWiring != null ? moduleWiring.getIndexingBootstrap() : null;
        if (indexing == null || indexing.getMetadataStore() == null || vectorIndexHealthAssessor == null) {
            LOG.debug("event=index_health_assessment_skip reason=missing_dependencies");
            return;
        }
        var readyIndexes = indexing.getMetadataStore().findIndexesByState(IndexState.READY);
        var staleIndexes = indexing.getMetadataStore().findIndexesByState(IndexState.STALE);
        var allVectorIndexes = collectVectorIndexes(readyIndexes, staleIndexes);
        LOG.debug("event=index_health_assessment_cycle vectorIndexCount={}", allVectorIndexes.size());
        var criticalAssessments = identifyCriticalIndexes(allVectorIndexes);
        if (!criticalAssessments.isEmpty() && indexRebuildScheduler != null) {
            int scheduled = indexRebuildScheduler.scheduleFromAssessments(criticalAssessments);
            LOG.info("event=index_health_rebuilds_scheduled count={} critical={}", scheduled,
                    criticalAssessments.size());
        }
    }

    private static List<org.pangrova.spi.indexing.descriptor.IndexDescriptor> collectVectorIndexes(
            Iterable<org.pangrova.spi.indexing.descriptor.IndexDescriptor> ready,
            Iterable<org.pangrova.spi.indexing.descriptor.IndexDescriptor> stale) {
        var result = new ArrayList<org.pangrova.spi.indexing.descriptor.IndexDescriptor>();
        for (var idx : ready) {
            if (idx.indexType() == org.pangrova.spi.indexing.descriptor.IndexType.VECTOR) result.add(idx);
        }
        for (var idx : stale) {
            if (idx.indexType() == org.pangrova.spi.indexing.descriptor.IndexType.VECTOR) result.add(idx);
        }
        return result;
    }

    private List<org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler.IndexWithAssessment>
            identifyCriticalIndexes(List<org.pangrova.spi.indexing.descriptor.IndexDescriptor> descriptors) {
        var result = new ArrayList<
                org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler.IndexWithAssessment>();
        for (var descriptor : descriptors) {
            long totalFiles = Long.parseLong(descriptor.properties().getOrDefault("total_data_files", "100"));
            long indexedFiles = Long.parseLong(descriptor.properties().getOrDefault("indexed_data_files", "100"));
            var assessment = vectorIndexHealthAssessor.assess(descriptor, totalFiles, indexedFiles);
            if (assessment.level() == VectorIndexHealthAssessor.HealthLevel.CRITICAL) {
                result.add(new org.pangrova.coordinator.indexing.ai.lifecycle.IndexRebuildScheduler
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
        var readyIndexes = indexing.getMetadataStore().findIndexesByState(IndexState.READY);
        int evaluated = 0;
        for (var descriptor : readyIndexes) {
            if (descriptor.indexType() != org.pangrova.spi.indexing.descriptor.IndexType.VECTOR) {
                continue;
            }
            evaluated += evaluateIndexRecommendation(descriptor);
        }
        LOG.debug("event=index_recommendation_cycle evaluated={}", evaluated);
    }

    private int evaluateIndexRecommendation(org.pangrova.spi.indexing.descriptor.IndexDescriptor descriptor) {
        int dimension = Integer.parseInt(descriptor.properties().getOrDefault("dimension", "128"));
        long vectorCount = Long.parseLong(descriptor.properties().getOrDefault("vector_count", "0"));
        long availableMemory = Long.parseLong(
                descriptor.properties().getOrDefault("memory_budget", String.valueOf(1024L * 1024L * 1024L)));
        var recommendation = indexRecommendationEngine.evaluateExisting(
                descriptor, vectorCount, dimension, availableMemory);
        if (recommendation.isPresent()) {
            LOG.info("event=algorithm_change_recommended indexId={} newAlgorithm={} reason={}",
                    descriptor.indexId(),
                    recommendation.get().algorithm().getCapabilityId(),
                    recommendation.get().reason());
        }
        return 1;
    }
}
