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

import org.pangrova.coordinator.ai.index.lifecycle.VectorIndexHealthAssessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final CoordinatorBootstrapConfig config;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final ScheduledExecutorService schedulerExecutor;
    private final org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry
            embeddingModelRegistry;

    private org.pangrova.planner.advisor.QueryPatternRecorder patternRecorder;
    private org.pangrova.coordinator.ai.layout.DataLayoutAdvisor layoutAdvisor;
    private VectorIndexHealthAssessor vectorIndexHealthAssessor;
    private org.pangrova.coordinator.ai.index.lifecycle.IndexRecommendationEngine indexRecommendationEngine;
    private org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler indexRebuildScheduler;

    SelfIntelligenceBootstrap(
            CoordinatorBootstrapConfig config,
            ModuleIntegrationBootstrap moduleWiring,
            ScheduledExecutorService schedulerExecutor,
            org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry
                    embeddingModelRegistry) {
        this.config = config;
        this.moduleWiring = moduleWiring;
        this.schedulerExecutor = schedulerExecutor;
        this.embeddingModelRegistry = embeddingModelRegistry;
    }

    void initialize(QueryExecutionBootstrap executionWiring) {
        patternRecorder = new org.pangrova.planner.advisor.QueryPatternRecorder(10_000);
        layoutAdvisor = new org.pangrova.coordinator.ai.layout.DataLayoutAdvisor(patternRecorder);
        vectorIndexHealthAssessor = new VectorIndexHealthAssessor();
        indexRecommendationEngine = new org.pangrova.coordinator.ai.index.lifecycle.IndexRecommendationEngine();
        initIndexRebuildScheduler();
        if (executionWiring != null && embeddingModelRegistry != null) {
            executionWiring.registerEmbeddingModelTasks(embeddingModelRegistry);
            LOG.info("event=embedding_model_tasks_registered");
        }
        scheduleLayoutAdvisor();
        scheduleIndexLifecycle();
        LOG.info("event=self_intelligence_initialized patternCapacity={}", 10_000);
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

    static java.util.function.BiFunction<org.pangrova.spi.index.IndexDescriptor,
            org.pangrova.coordinator.ai.index.lifecycle.IndexRebuildScheduler.RebuildAction,
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
                    descriptor.indexId(),
                    recommendation.get().algorithm().getCapabilityId(),
                    recommendation.get().reason());
        }
        return 1;
    }
}
