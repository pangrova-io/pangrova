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

import com.google.protobuf.ByteString;
import org.pangrova.coordinator.index.IndexAwareSplitFetcher;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.rerank.RerankerInjectionRule;
import org.pangrova.planner.rerank.RerankerPlanNode;
import org.pangrova.planner.vector.VectorSearchNode;
import org.pangrova.protocol.v1.VectorSearchOperator;
import org.pangrova.protocol.v1.VectorValue;
import org.pangrova.spi.index.IndexDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            PlannedQuery plannedQuery, DispatchQueryContext ctx, String queryId) {
        var vectorNode = plannedQuery.tryDetectVectorSearch(this::lookupVectorIndexes);
        if (vectorNode.isEmpty()) {
            return ctx;
        }
        var vsNode = vectorNode.get();
        LOG.info("event=vector_search_detected queryId={} table={} column={} topK={} metric={} indexId={}",
                queryId, vsNode.tableName(), vsNode.vectorColumn(),
                vsNode.topK(), vsNode.metric(), vsNode.indexId());
        return ctx.withVectorSearch(vsNode);
    }

    DispatchQueryContext enrichWithReranker(DispatchQueryContext ctx, String queryId) {
        if (ctx.originalSql() == null || ctx.originalSql().isBlank()) {
            return ctx;
        }
        if (ctx.compiler() == null) {
            return ctx;
        }
        try {
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
            return ctx.withRerankerPlan(rerankerPlan.get());
        } catch (Exception e) {
            LOG.debug("event=reranker_analysis_skipped queryId={} reason={}", queryId, e.getMessage());
            return ctx;
        }
    }

    static CoordinatorSessionContext injectVectorSearchProperties(
            CoordinatorSessionContext session, VectorSearchNode vsNode) {
        VectorValue queryVec = VectorValue.newBuilder()
                .addAllValues(toFloatList(vsNode.queryVector()))
                .setDimension(vsNode.queryVector().length)
                .build();
        var builder = VectorSearchOperator.newBuilder()
                .setTableName(vsNode.tableName())
                .setVectorColumn(vsNode.vectorColumn())
                .setQueryVector(queryVec)
                .setTopK(vsNode.topK())
                .setDistanceMetric(vsNode.metric().name());
        if (vsNode.indexId() != null) {
            builder.setIndexId(vsNode.indexId());
        }
        if (vsNode.indexGeneration() >= 0) {
            builder.putSearchParams("_index_generation", String.valueOf(vsNode.indexGeneration()));
        }
        for (String preFilter : vsNode.serializedPreFilters()) {
            builder.addFilterExpressions(ByteString.copyFromUtf8(preFilter));
        }
        byte[] operatorBytes = builder.build().toByteArray();
        String encoded = Base64.getEncoder().encodeToString(operatorBytes);
        Map<String, String> augmented = new HashMap<>(session.sessionProperties());
        augmented.put("pangrova.vector_search_operator", encoded);
        return new CoordinatorSessionContext(
                session.queryId(), Map.copyOf(augmented),
                session.userIdentity(), session.tenantId());
    }

    static CoordinatorSessionContext injectRerankerProperties(
            CoordinatorSessionContext session, RerankerPlanNode rerankerPlan) {
        Map<String, String> augmented = new HashMap<>(session.sessionProperties());
        augmented.put("pangrova.reranker_strategy", strategyToPropertyValue(rerankerPlan.strategy()));
        augmented.put("pangrova.reranker_top_k", String.valueOf(rerankerPlan.topK()));
        if (rerankerPlan.queryText() != null) {
            augmented.put("pangrova.reranker_query_text", rerankerPlan.queryText());
        }
        augmented.put("pangrova.reranker_source_count", String.valueOf(rerankerPlan.sources().size()));
        augmented.put("pangrova.reranker_hybrid", String.valueOf(rerankerPlan.isHybrid()));
        return new CoordinatorSessionContext(
                session.queryId(), Map.copyOf(augmented),
                session.userIdentity(), session.tenantId());
    }

    private static String strategyToPropertyValue(RerankerPlanNode.RerankerStrategy strategy) {
        if (strategy instanceof RerankerPlanNode.RrfStrategy rrf) {
            return "rrf:" + rrf.k();
        } else if (strategy instanceof RerankerPlanNode.LinearCombinationStrategy lc) {
            return "weighted:" + lc.weight1() + ":" + lc.weight2();
        } else if (strategy instanceof RerankerPlanNode.CrossEncoderStrategy ce) {
            return "cross_encoder:" + ce.modelName() + ":" + ce.topN();
        } else if (strategy instanceof RerankerPlanNode.CascadeStrategy cascade) {
            return "cascade:" + cascade.stages().size();
        }
        return "unknown";
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

    private static List<Float> toFloatList(float[] arr) {
        List<Float> list = new ArrayList<>(arr.length);
        for (float v : arr) {
            list.add(v);
        }
        return list;
    }

    private List<IndexDescriptor> lookupVectorIndexes(String tableName, String columnName) {
        if (indexAwareSplitFetcher == null) {
            return List.of();
        }
        try {
            return indexAwareSplitFetcher.lookupIndexes(tableName, columnName);
        } catch (Exception e) {
            LOG.debug("event=vector_index_lookup_failed table={} column={} error={}",
                    tableName, columnName, e.getMessage());
            return List.of();
        }
    }
}
