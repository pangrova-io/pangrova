package org.pangrova.worker.property.task;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.worker.task.index.IndexEvalSplitFilter;

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
        IndexEvalResult aggregated = IndexEvalSplitFilter.aggregateResults(results);
        assertThat(aggregated)
                .as("Any SKIP in results must produce SKIP")
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 200)
    void keepWithoutSkipProducesKeep(@ForAll("resultListsKeepNoSkip") List<IndexEvalResult> results) {
        IndexEvalResult aggregated = IndexEvalSplitFilter.aggregateResults(results);
        assertThat(aggregated)
                .as("KEEP + no SKIP must produce KEEP")
                .isEqualTo(IndexEvalResult.KEEP);
    }

    @Property(tries = 100)
    void allUnknownProducesUnknown(@ForAll("unknownOnlyLists") List<IndexEvalResult> results) {
        IndexEvalResult aggregated = IndexEvalSplitFilter.aggregateResults(results);
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
}
