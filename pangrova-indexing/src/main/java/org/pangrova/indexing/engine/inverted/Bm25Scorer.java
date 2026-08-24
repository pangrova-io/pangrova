package org.pangrova.indexing.engine.inverted;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes BM25 relevance scores for full-text search queries against an inverted index.
 *
 * <p>BM25 formula per term:
 * <pre>
 *   score(t, d) = IDF(t) * (tf(t,d) * (k1 + 1)) / (tf(t,d) + k1 * (1 - b + b * dl / avgdl))
 * </pre>
 * where:
 * <ul>
 *   <li>IDF(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)</li>
 *   <li>tf(t,d) = term frequency of t in document d</li>
 *   <li>N = total number of documents</li>
 *   <li>df(t) = number of documents containing term t</li>
 *   <li>dl = document length (token count)</li>
 *   <li>avgdl = average document length across corpus</li>
 * </ul>
 *
 * <p>Thread-safe and stateless. All state is passed via parameters.</p>
 */
public final class Bm25Scorer {

    /** Default BM25 k1 parameter controlling term frequency saturation. */
    public static final double DEFAULT_K1 = 1.2;

    /** Default BM25 b parameter controlling document length normalization. */
    public static final double DEFAULT_B = 0.75;

    /** Smoothing constant added in the IDF ratio (Lucene-style +0.5). */
    private static final double IDF_SMOOTHING = 0.5;

    private Bm25Scorer() {}

    /**
     * Scores all documents in the corpus against the given query tokens.
     *
     * <p>Each posting list entry is a rowId; one entry is appended per token
     * occurrence in a row (so duplicates within a row produce the term
     * frequency). {@link InvertedStrategyProvider} produces this encoding
     * when {@code store_positions=true}; without positions, posting lists are
     * empty (term-set only) and this scorer returns no results.</p>
     *
     * @param postings       term to list of rowIds (one entry per occurrence)
     * @param queryTokens    tokenized query terms
     * @param documentLengths per-document token counts indexed by row ID
     * @param totalDocuments total number of documents in the corpus
     * @param k1             BM25 k1 parameter
     * @param b              BM25 b parameter
     * @return scored rows sorted by score descending, only includes rows with score greater than 0
     */
    public static List<ScoredRow> score(Map<String, List<Integer>> postings,
                                        List<String> queryTokens,
                                        int[] documentLengths,
                                        int totalDocuments,
                                        double k1,
                                        double b) {
        if (postings.isEmpty() || queryTokens.isEmpty() || totalDocuments == 0) {
            return List.of();
        }

        double avgdl = computeAverageDocumentLength(documentLengths, totalDocuments);
        double[] scores = new double[documentLengths.length];

        for (String token : queryTokens) {
            List<Integer> posting = postings.get(token);
            if (posting == null || posting.isEmpty()) {
                continue;
            }
            int df = countDistinctDocuments(posting);
            double idf = computeIdf(totalDocuments, df);
            accumulateTermScores(posting, idf, k1, b, documentLengths, avgdl, scores);
        }

        return collectScoredRows(scores);
    }

    private static void accumulateTermScores(List<Integer> posting,
                                             double idf,
                                             double k1,
                                             double b,
                                             int[] documentLengths,
                                             double avgdl,
                                             double[] scores) {
        Map<Integer, Integer> termFrequencies = computeTermFrequencies(posting);
        for (Map.Entry<Integer, Integer> entry : termFrequencies.entrySet()) {
            int rowId = entry.getKey();
            int tf = entry.getValue();
            if (rowId < 0 || rowId >= documentLengths.length) {
                continue;
            }
            int dl = documentLengths[rowId];
            double tfScore = computeTermScore(tf, k1, b, dl, avgdl);
            scores[rowId] += idf * tfScore;
        }
    }


    /**
     * Scores documents using default BM25 parameters (k1=1.2, b=0.75).
     */
    public static List<ScoredRow> score(Map<String, List<Integer>> postings,
                                        List<String> queryTokens,
                                        int[] documentLengths,
                                        int totalDocuments) {
        return score(postings, queryTokens, documentLengths, totalDocuments, DEFAULT_K1, DEFAULT_B);
    }

    /**
     * Computes the IDF (Inverse Document Frequency) component.
     *
     * @param totalDocuments total documents in corpus (N)
     * @param documentFrequency number of documents containing the term (df)
     * @return IDF value, always non-negative
     */
    public static double computeIdf(int totalDocuments, int documentFrequency) {
        return Math.log((totalDocuments - documentFrequency + IDF_SMOOTHING)
                / (documentFrequency + IDF_SMOOTHING) + 1.0);
    }

    /**
     * Computes the term frequency saturation component of BM25.
     */
    public static double computeTermScore(int tf, double k1, double b, int dl, double avgdl) {
        double numerator = tf * (k1 + 1.0);
        double denominator = tf + k1 * (1.0 - b + b * dl / avgdl);
        return numerator / denominator;
    }

    private static double computeAverageDocumentLength(int[] documentLengths, int totalDocuments) {
        long totalLength = 0;
        for (int len : documentLengths) {
            totalLength += len;
        }
        return (double) totalLength / totalDocuments;
    }

    private static int countDistinctDocuments(List<Integer> posting) {
        return (int) posting.stream().distinct().count();
    }

    private static Map<Integer, Integer> computeTermFrequencies(List<Integer> posting) {
        Map<Integer, Integer> frequencies = new HashMap<>();
        for (int rowId : posting) {
            frequencies.merge(rowId, 1, Integer::sum);
        }
        return frequencies;
    }

    private static List<ScoredRow> collectScoredRows(double[] scores) {
        List<ScoredRow> result = new ArrayList<>();
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] > 0.0) {
                result.add(new ScoredRow(i, scores[i]));
            }
        }
        result.sort(Collections.reverseOrder());
        return Collections.unmodifiableList(result);
    }

    /**
     * A document row with its BM25 relevance score.
     *
     * @param rowId row identifier within the data file
     * @param score BM25 relevance score (higher is more relevant)
     */
    public record ScoredRow(long rowId, double score) implements Comparable<ScoredRow> {

        public ScoredRow {
            if (score < 0.0) {
                throw new IllegalArgumentException("score must be non-negative: " + score);
            }
        }

        @Override
        public int compareTo(ScoredRow other) {
            return Double.compare(this.score, other.score);
        }
    }
}
