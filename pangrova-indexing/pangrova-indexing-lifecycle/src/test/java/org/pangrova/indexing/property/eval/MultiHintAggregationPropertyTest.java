package org.pangrova.indexing.property.eval;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import org.pangrova.indexing.evaluation.IndexEvaluationService;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 14: Multi-hint aggregation.
 *
 * <p>For any list of evaluation results, aggregateResults follows the rule:
 * any SKIP → SKIP, any KEEP + no SKIP → KEEP, all UNKNOWN → UNKNOWN.</p>
 *
 * <p><b>Validates: Requirement 18.1</b></p>
 */
class MultiHintAggregationPropertyTest {

    @Property(tries = 200)
    void anySkipProducesSkip(@ForAll("resultListsWithSkip") List<IndexEvalResult> results) {
        IndexEvalResult aggregated = evaluateHints(results);
        assertThat(aggregated)
                .as("Any SKIP in results must produce SKIP")
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 200)
    void keepWithoutSkipProducesKeep(@ForAll("resultListsKeepNoSkip") List<IndexEvalResult> results) {
        IndexEvalResult aggregated = evaluateHints(results);
        assertThat(aggregated)
                .as("KEEP + no SKIP must produce KEEP")
                .isEqualTo(IndexEvalResult.KEEP);
    }

    @Property(tries = 100)
    void allUnknownProducesUnknown(@ForAll("unknownOnlyLists") List<IndexEvalResult> results) {
        IndexEvalResult aggregated = evaluateHints(results);
        assertThat(aggregated)
                .as("All UNKNOWN must produce UNKNOWN")
                .isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Provide
    Arbitrary<List<IndexEvalResult>> resultListsWithSkip() {
        return Arbitraries.of(IndexEvalResult.values()).list().ofMinSize(1).ofMaxSize(10)
                .filter(list -> list.contains(IndexEvalResult.SKIP));
    }

    @Provide
    Arbitrary<List<IndexEvalResult>> resultListsKeepNoSkip() {
        return Arbitraries.of(IndexEvalResult.KEEP, IndexEvalResult.UNKNOWN)
                .list().ofMinSize(1).ofMaxSize(10)
                .filter(list -> list.contains(IndexEvalResult.KEEP));
    }

    @Provide
    Arbitrary<List<IndexEvalResult>> unknownOnlyLists() {
        return Arbitraries.just(IndexEvalResult.UNKNOWN).list().ofMinSize(1).ofMaxSize(10);
    }
    private static IndexEvalResult evaluateHints(List<IndexEvalResult> results) {
        var cache = new IndexCache(1_000_000);
        var descriptors = new ArrayList<IndexDescriptor>();
        for (int i = 0; i < results.size(); i++) {
            String id = "hint-" + i;
            cache.put(id, "file", 1, new ResultData(results.get(i)));
            descriptors.add(new IndexDescriptor(id, id, "cat", "schema", "table",
                    List.of("c"), IndexType.HASH, Map.of(), IndexState.READY,
                    null, 1, "1", "1", Instant.EPOCH, Instant.EPOCH, "tenant"));
        }
        IndexStrategyProvider provider = new IndexStrategyProvider() {
            public org.pangrova.indexing.api.codec.TypeSerializer<?> payloadSerializer() {
                throw new UnsupportedOperationException("Evaluation-only test provider");
            }
            public IndexType supportedType() { return IndexType.HASH; }
            public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
                throw new UnsupportedOperationException("Evaluation-only test provider");
            }
            public IndexEvalResult evaluate(IndexData data, IndexPredicate predicate) {
                return ((ResultData) data).result();
            }
        };
        var service = new IndexEvaluationService(new IndexEngineRegistry(List.of(provider)), cache, null, null);
        return service.aggregateResults(descriptors, "file", null, null, id -> "index/" + id);
    }

    private record ResultData(IndexEvalResult result) implements IndexData {
        public IndexType indexType() { return IndexType.HASH; }
        public long estimatedSizeInBytes() { return 32; }
    }
}
