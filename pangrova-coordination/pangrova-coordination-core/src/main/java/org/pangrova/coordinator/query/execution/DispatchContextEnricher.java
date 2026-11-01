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
package org.pangrova.coordinator.query.execution;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.indexing.planner.vector.VectorSearchNode;
import org.pangrova.indexing.planner.vector.VectorSearchRewriter;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.rerank.RerankerInjectionRule;
import org.pangrova.planner.rerank.RerankerPlanNode;
import org.pangrova.planner.rerank.FullTextScoringNode;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Comparator;
import java.util.Locale;

/**
 * Enriches a {@link DispatchQueryContext} with vector search and reranker metadata
 * extracted from the query plan and original SQL.
 */
final class DispatchContextEnricher {

    private static final Logger LOG = LoggerFactory.getLogger(DispatchContextEnricher.class);

    private final IndexAwareSplitFetcher indexAwareSplitFetcher;

    DispatchContextEnricher(IndexAwareSplitFetcher indexAwareSplitFetcher) {
        this.indexAwareSplitFetcher = indexAwareSplitFetcher;
    }

    DispatchQueryContext enrichWithVectorSearch(
            PlannedQuery plannedQuery, DispatchQueryContext ctx,
            String queryId, String tenantId) {
        var vectorNode = VectorSearchRewriter.tryRewrite(
                plannedQuery.plan(),
                (table, column) -> lookupVectorIndexes(tenantId, table, column));
        if (vectorNode.isEmpty()) {
            return ctx;
        }
        var vsNode = bindVectorSearchMetadata(vectorNode.get(), ctx, tenantId);
        LOG.info("event=vector_search_detected queryId={} table={} column={} topK={} metric={} indexId={}",
                queryId, vsNode.tableName(), vsNode.vectorColumn(),
                vsNode.topK(), vsNode.metric(), vsNode.indexId());
        return ctx.withVectorSearch(vsNode);
    }

    DispatchQueryContext enrichWithReranker(
            DispatchQueryContext ctx, String queryId, String tenantId) {
        if (ctx.originalSql() == null || ctx.originalSql().isBlank()) {
            return ctx;
        }
        if (ctx.compiler() == null) {
            return ctx;
        }
        Statement stmt = ctx.compiler().parseToAst(ctx.originalSql());
        if (!(stmt instanceof Query query)) {
            return ctx;
        }
        String tableName = extractTableName(query);
        int topK = extractTopK(query);
        var rerankerPlan = RerankerInjectionRule.analyze(query, tableName, topK);
        if (rerankerPlan.isEmpty()) {
            return ctx;
        }
        LOG.info("event=reranker_detected queryId={} strategy={} sources={} topK={} hybrid={}",
                queryId,
                rerankerPlan.get().strategy().getClass().getSimpleName(),
                rerankerPlan.get().sources().size(),
                rerankerPlan.get().topK(),
                rerankerPlan.get().isHybrid());
        return ctx.withRerankerPlan(bindFullTextIndexes(rerankerPlan.get(), ctx, tenantId));
    }

    private static String extractTableName(Query query) {
        if (query.from().isEmpty()) {
            return "";
        }
        TableRef first = query.from().get(0);
        return String.join(".", first.name().parts());
    }

    private static int extractTopK(Query query) {
        if (query.limit().isEmpty()) {
            return 0;
        }
        Expression limitExpr = query.limit().get();
        if (limitExpr instanceof Literal lit
                && lit.value() instanceof LiteralValue.IntegerValue iv) {
            return (int) iv.value();
        }
        return 0;
    }

    private List<IndexDescriptor> lookupVectorIndexes(
            String tenantId, String tableName, String columnName) {
        return lookupIndexes(tenantId, tableName, columnName);
    }

    private VectorSearchNode bindVectorSearchMetadata(
            VectorSearchNode node,
            DispatchQueryContext ctx,
            String tenantId) {
        IndexDescriptor descriptor = lookupIndexes(
                tenantId, node.tableName(), node.vectorColumn()).stream()
                .filter(index -> index.indexType() == IndexType.VECTOR)
                .filter(index -> index.state() == IndexState.READY)
                .filter(index -> node.indexId() == null || node.indexId().equals(index.indexId()))
                .findFirst()
                .orElse(null);
        if (descriptor == null) {
            return node;
        }
        return node.withSearchMetadata(
                sourceSnapshot(ctx, node.tableName()),
                indexSnapshot(descriptor),
                freshnessEpochMillis(descriptor));
    }

    private RerankerPlanNode bindFullTextIndexes(
            RerankerPlanNode plan, DispatchQueryContext ctx, String tenantId) {
        List<RerankerPlanNode.RetrievalSource> boundSources = plan.sources().stream()
                .map(source -> bindFullTextSource(source, ctx, tenantId))
                .toList();
        return new RerankerPlanNode(
                plan.strategy(), boundSources, plan.topK(),
                plan.queryText(), plan.targetColumns());
    }

    private RerankerPlanNode.RetrievalSource bindFullTextSource(
            RerankerPlanNode.RetrievalSource source,
            DispatchQueryContext ctx,
            String tenantId) {
        if (!(source instanceof RerankerPlanNode.FullTextSource fullText)) {
            return source;
        }
        FullTextScoringNode node = fullText.scoringNode();
        IndexDescriptor descriptor = lookupIndexes(
                tenantId, node.tableName(), node.column()).stream()
                .filter(index -> index.indexType() == IndexType.INVERTED)
                .filter(index -> index.state() == IndexState.READY)
                .filter(DispatchContextEnricher::isNativeBm25)
                .filter(index -> !indexSnapshot(index).isBlank())
                .max(Comparator.comparingInt(IndexDescriptor::generation)
                        .thenComparing(IndexDescriptor::indexId))
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "MATCH/BM25_SCORE requires a READY Native BM25 index with "
                                + "analyzer=standard-v1 for " + node.tableName()
                                + "." + node.column()));
        FullTextScoringNode bound = node.withIndexBinding(
                descriptor.indexId(),
                descriptor.generation(),
                sourceSnapshot(ctx, node.tableName()),
                indexSnapshot(descriptor),
                freshnessEpochMillis(descriptor));
        return new RerankerPlanNode.FullTextSource(bound);
    }

    private List<IndexDescriptor> lookupIndexes(
            String tenantId, String tableName, String columnName) {
        if (indexAwareSplitFetcher == null) {
            return List.of();
        }
        return indexAwareSplitFetcher.lookupIndexes(tenantId, tableName, columnName);
    }

    private static boolean isNativeBm25(IndexDescriptor descriptor) {
        String algorithm = descriptor.properties().getOrDefault("algorithm", "BM25")
                .trim().toUpperCase(Locale.ROOT).replace('-', '_');
        String analyzer = descriptor.properties().getOrDefault(
                "analyzer", "standard-v1").trim().toLowerCase(Locale.ROOT);
        return algorithm.equals("BM25") && analyzer.equals("standard-v1");
    }

    private static String sourceSnapshot(DispatchQueryContext ctx, String tableName) {
        Long snapshot = ctx.snapshotOverrides().get(tableName);
        return snapshot == null ? "" : String.valueOf(snapshot);
    }

    private static String indexSnapshot(IndexDescriptor descriptor) {
        if (descriptor.lastIndexedSnapshotId() != null
                && !descriptor.lastIndexedSnapshotId().isBlank()) {
            return descriptor.lastIndexedSnapshotId();
        }
        if (descriptor.buildSnapshotId() != null
                && !descriptor.buildSnapshotId().isBlank()) {
            return descriptor.buildSnapshotId();
        }
        return "";
    }

    private static long freshnessEpochMillis(IndexDescriptor descriptor) {
        return descriptor.updatedAt() == null ? 0 : descriptor.updatedAt().toEpochMilli();
    }

}
