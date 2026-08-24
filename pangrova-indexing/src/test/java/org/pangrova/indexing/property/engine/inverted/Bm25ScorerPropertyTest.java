package org.pangrova.indexing.property.engine.inverted;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.indexing.engine.inverted.Bm25Scorer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for BM25 scoring correctness.
 *
 * <p><b>Validates: Requirements 4.5</b></p>
 */
class Bm25ScorerPropertyTest {

    @Property(tries = 200)
    void scoresAreAlwaysNonNegative(
            @ForAll("postingsWithDocuments") PostingsFixture fixture) {
        List<Bm25Scorer.ScoredRow> results = Bm25Scorer.score(
                fixture.postings, fixture.queryTokens,
                fixture.documentLengths, fixture.totalDocuments);

        assertThat(results).allSatisfy(row ->
                assertThat(row.score()).as("BM25 score must be non-negative").isGreaterThanOrEqualTo(0.0));
    }

    @Property(tries = 200)
    void resultsAreSortedByScoreDescending(
            @ForAll("postingsWithDocuments") PostingsFixture fixture) {
        List<Bm25Scorer.ScoredRow> results = Bm25Scorer.score(
                fixture.postings, fixture.queryTokens,
                fixture.documentLengths, fixture.totalDocuments);

        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i - 1).score())
                    .as("Results must be sorted by score descending")
                    .isGreaterThanOrEqualTo(results.get(i).score());
        }
    }

    @Property(tries = 200)
    void documentWithMoreTermMatchesScoresHigher(
            @ForAll @IntRange(min = 5, max = 50) int totalDocs) {
        // Document 0 contains both "alpha" and "beta"
        // Document 1 contains only "alpha"
        Map<String, List<Integer>> postings = new HashMap<>();
        postings.put("alpha", List.of(0, 1));
        postings.put("beta", List.of(0));

        int[] docLengths = new int[totalDocs];
        for (int i = 0; i < totalDocs; i++) {
            docLengths[i] = 10;
        }

        List<Bm25Scorer.ScoredRow> results = Bm25Scorer.score(
                postings, List.of("alpha", "beta"), docLengths, totalDocs);

        assertThat(results).hasSizeGreaterThanOrEqualTo(2);
        Bm25Scorer.ScoredRow doc0 = results.stream().filter(r -> r.rowId() == 0).findFirst().orElseThrow();
        Bm25Scorer.ScoredRow doc1 = results.stream().filter(r -> r.rowId() == 1).findFirst().orElseThrow();
        assertThat(doc0.score())
                .as("Document matching more query terms should score higher")
                .isGreaterThan(doc1.score());
    }

    @Property(tries = 100)
    void idfIsHigherForRarerTerms(
            @ForAll @IntRange(min = 10, max = 1000) int totalDocs) {
        int rareDf = 1;
        int commonDf = totalDocs / 2;

        double rareIdf = Bm25Scorer.computeIdf(totalDocs, rareDf);
        double commonIdf = Bm25Scorer.computeIdf(totalDocs, commonDf);

        assertThat(rareIdf)
                .as("Rare terms (low df) should have higher IDF than common terms (high df)")
                .isGreaterThan(commonIdf);
    }

    @Property(tries = 100)
    void idfIsAlwaysNonNegative(
            @ForAll @IntRange(min = 1, max = 10000) int totalDocs,
            @ForAll @IntRange(min = 1, max = 10000) int df) {
        int effectiveDf = Math.min(df, totalDocs);
        double idf = Bm25Scorer.computeIdf(totalDocs, effectiveDf);
        assertThat(idf).as("IDF must be non-negative").isGreaterThanOrEqualTo(0.0);
    }

    @Property(tries = 100)
    void termScoreSaturatesWithHighTf(
            @ForAll @DoubleRange(min = 0.5, max = 3.0) double k1,
            @ForAll @DoubleRange(min = 0.0, max = 1.0) double b) {
        double avgdl = 100.0;
        int dl = 100;

        double scoreTf1 = Bm25Scorer.computeTermScore(1, k1, b, dl, avgdl);
        double scoreTf10 = Bm25Scorer.computeTermScore(10, k1, b, dl, avgdl);
        double scoreTf100 = Bm25Scorer.computeTermScore(100, k1, b, dl, avgdl);

        assertThat(scoreTf10).isGreaterThan(scoreTf1);
        assertThat(scoreTf100).isGreaterThan(scoreTf10);
        // Saturation: the gap between tf=10 and tf=100 should be smaller than tf=1 and tf=10
        double gap1to10 = scoreTf10 - scoreTf1;
        double gap10to100 = scoreTf100 - scoreTf10;
        assertThat(gap10to100)
                .as("BM25 term score should saturate (diminishing returns for higher tf)")
                .isLessThan(gap1to10);
    }

    @Property(tries = 200)
    void emptyQueryReturnsNoResults(
            @ForAll("postingsWithDocuments") PostingsFixture fixture) {
        List<Bm25Scorer.ScoredRow> results = Bm25Scorer.score(
                fixture.postings, List.of(), fixture.documentLengths, fixture.totalDocuments);

        assertThat(results).isEmpty();
    }

    @Property(tries = 200)
    void queryWithNoMatchingTermsReturnsNoResults(
            @ForAll("postingsWithDocuments") PostingsFixture fixture) {
        List<Bm25Scorer.ScoredRow> results = Bm25Scorer.score(
                fixture.postings, List.of("nonexistent_xyz_term"),
                fixture.documentLengths, fixture.totalDocuments);

        assertThat(results).isEmpty();
    }

    @Provide
    Arbitrary<PostingsFixture> postingsWithDocuments() {
        return Arbitraries.integers().between(3, 20).flatMap(numDocs ->
                Arbitraries.integers().between(1, 5).flatMap(numTerms -> {
                    Map<String, List<Integer>> postings = new HashMap<>();
                    List<String> queryTokens = new ArrayList<>();
                    for (int t = 0; t < numTerms; t++) {
                        String term = "term" + t;
                        queryTokens.add(term);
                        List<Integer> posting = new ArrayList<>();
                        for (int d = 0; d < numDocs; d++) {
                            if ((d + t) % 3 != 0) {
                                posting.add(d);
                            }
                        }
                        if (!posting.isEmpty()) {
                            postings.put(term, posting);
                        }
                    }
                    int[] docLengths = new int[numDocs];
                    for (int d = 0; d < numDocs; d++) {
                        docLengths[d] = 5 + (d * 3) % 20;
                    }
                    return Arbitraries.just(new PostingsFixture(postings, queryTokens, docLengths, numDocs));
                }));
    }

    record PostingsFixture(
            Map<String, List<Integer>> postings,
            List<String> queryTokens,
            int[] documentLengths,
            int totalDocuments) {}
}
