package org.pangrova.indexing.engine.inverted;

import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.data.text.InvertedIndexData;
import org.pangrova.spi.index.predicate.TextMatch;
import org.pangrova.spi.index.predicate.TextMatchMode;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the 26.8 fix: {@link InvertedStrategyProvider} posting lists
 * store rowIds (one entry per token occurrence) rather than positions,
 * giving {@link Bm25Scorer} correct (rowId, tf) semantics and letting
 * {@link PostingListMerger} prune rows missing any phrase token.
 */
class InvertedRowAwarePostingsTest {

    private final InvertedStrategyProvider provider = new InvertedStrategyProvider();

    @Test
    void postingListStoresOneRowIdPerOccurrence() {
        List<List<Comparable<?>>> rows = List.of(
                List.of("alpha alpha beta"),  // row 0: alpha tf=2, beta tf=1
                List.of("alpha gamma"));      // row 1: alpha tf=1, gamma tf=1

        Iterator<List<Comparable<?>>> iter = rows.iterator();
        InvertedIndexData data = (InvertedIndexData) provider.build(iter,
                Map.of("inverted.store_positions", "true"));

        // alpha occurs twice in row 0 and once in row 1 → posting = [0, 0, 1]
        assertThat(data.postings().get("alpha"))
                .as("alpha posting must contain one rowId per occurrence")
                .containsExactly(0, 0, 1);
        assertThat(data.postings().get("beta")).containsExactly(0);
        assertThat(data.postings().get("gamma")).containsExactly(1);
    }

    @Test
    void bm25TermFrequencyIsRepeatedRowIdCount() {
        List<List<Comparable<?>>> rows = List.of(
                List.of("alpha alpha alpha"),  // row 0: alpha tf=3
                List.of("alpha"));             // row 1: alpha tf=1

        Iterator<List<Comparable<?>>> iter = rows.iterator();
        InvertedIndexData data = (InvertedIndexData) provider.build(iter,
                Map.of("inverted.store_positions", "true"));

        List<Bm25Scorer.ScoredRow> scored = provider.score(
                data, "alpha", new int[] { 3, 1 }, 2,
                Map.of("inverted.store_positions", "true"));

        // Row 0 has higher TF and longer document, but TF gain dominates here.
        assertThat(scored).hasSize(2);
        Bm25Scorer.ScoredRow row0 = scored.stream().filter(r -> r.rowId() == 0).findFirst().orElseThrow();
        Bm25Scorer.ScoredRow row1 = scored.stream().filter(r -> r.rowId() == 1).findFirst().orElseThrow();
        assertThat(row0.score()).isGreaterThan(row1.score());
    }

    @Test
    void phraseMatchSkipsRowsMissingAnyToken() {
        // Row 0 has alpha+beta, row 1 has alpha+gamma. Phrase "alpha beta"
        // must produce KEEP (row 0 carries both); phrase "alpha delta" must
        // produce SKIP (no row carries delta).
        List<List<Comparable<?>>> rows = List.of(
                List.of("alpha beta"),
                List.of("alpha gamma"));

        Iterator<List<Comparable<?>>> iter = rows.iterator();
        InvertedIndexData data = (InvertedIndexData) provider.build(iter,
                Map.of("inverted.store_positions", "true"));

        TextMatch keep = new TextMatch("c", List.of("alpha", "beta"), TextMatchMode.MATCH_PHRASE);
        assertThat(provider.evaluate(data, keep)).isEqualTo(IndexEvalResult.KEEP);

        TextMatch skip = new TextMatch("c", List.of("alpha", "delta"), TextMatchMode.MATCH_PHRASE);
        assertThat(provider.evaluate(data, skip)).isEqualTo(IndexEvalResult.SKIP);
    }
}
