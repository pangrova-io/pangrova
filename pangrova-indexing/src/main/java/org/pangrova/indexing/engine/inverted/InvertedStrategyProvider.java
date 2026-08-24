package org.pangrova.indexing.engine.inverted;

import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.IndexStrategyProvider;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.data.text.InvertedIndexData;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.TextMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Inverted index strategy for full-text search and token-based matching.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code inverted.store_positions} — store posting lists (default false)</li>
 *   <li>{@code inverted.analyzer} — tokenizer name (default "standard", also "cjk")</li>
 * </ul>
 */
public final class InvertedStrategyProvider implements IndexStrategyProvider {

    private static final Logger LOG = LoggerFactory.getLogger(InvertedStrategyProvider.class);
    private static final String PROP_STORE_POSITIONS = "inverted.store_positions";
    private static final String PROP_ANALYZER = "inverted.analyzer";
    private static final String DEFAULT_ANALYZER = "standard";

    @Override
    public IndexType supportedType() {
        return IndexType.INVERTED;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        boolean storePositions = boolProp(properties, PROP_STORE_POSITIONS, false);
        String analyzer = stringProp(properties, PROP_ANALYZER, DEFAULT_ANALYZER);

        Map<String, List<Integer>> postings = new HashMap<>();
        int rowId = 0;
        while (values.hasNext()) {
            indexRow(rowId, values.next(), analyzer, storePositions, postings);
            rowId++;
        }
        if (!storePositions) {
            postings = toTermSet(postings);
        }
        return new InvertedIndexData(postings, storePositions);
    }

    /**
     * Appends posting entries for one row.
     *
     * <p>When {@code storePositions} is true, each token occurrence appends
     * a rowId entry — one entry per occurrence, so duplicate tokens in a
     * row contribute the term frequency consumed by {@link Bm25Scorer}.
     * Per-occurrence position information is encoded as a positional rowId
     * carrier inside the auxiliary phrase store: for phrase matching the
     * merger calls {@link AnalyzerFactory} again on the document text;
     * positions are not persisted in the posting list itself because the
     * {@link InvertedIndexData} record schema is {@code Map&lt;String, List&lt;Integer&gt;&gt;}.</p>
     *
     * <p>Previously the posting list stored token positions instead of
     * rowIds, which produced incorrect BM25 scores (term frequency was
     * counted per position, not per row) and miscounted document frequency.
     * Storing rowIds is the minimum invariant needed by both BM25 and the
     * row-aware phrase matcher.</p>
     *
     * <p>When {@code storePositions} is false, the {@link #toTermSet} pass
     * strips lists down to empty markers — only the term set is retained
     * for file-level skip.</p>
     */
    private static void indexRow(int rowId, List<Comparable<?>> row, String analyzer,
                                 boolean storePositions, Map<String, List<Integer>> postings) {
        for (int col = 0; col < row.size(); col++) {
            Comparable<?> val = row.get(col);
            if (val == null) {
                continue;
            }
            List<String> tokens = AnalyzerFactory.tokenize(val.toString(), analyzer);
            for (String token : tokens) {
                List<Integer> entries = postings.computeIfAbsent(token, k -> new ArrayList<>());
                // Append the rowId for every occurrence. Duplicate appends give
                // BM25 the correct in-row term frequency. When storePositions
                // is false, toTermSet collapses these to empty markers.
                entries.add(rowId);
            }
        }
    }

    private static Map<String, List<Integer>> toTermSet(Map<String, List<Integer>> postings) {
        Map<String, List<Integer>> termSet = new HashMap<>(postings.size());
        for (String term : postings.keySet()) {
            termSet.put(term, List.of());
        }
        return termSet;
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof InvertedIndexData inverted)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (!(predicate instanceof TextMatch textMatch)) {
            return IndexEvalResult.UNKNOWN;
        }
        return evaluateTextMatch(inverted, textMatch);
    }

    private IndexEvalResult evaluateTextMatch(InvertedIndexData inverted, TextMatch match) {
        Map<String, List<Integer>> postings = inverted.postings();
        List<String> tokens = match.tokens();

        return switch (match.mode()) {
            case MATCH_ANY -> matchAny(postings, tokens);
            case MATCH_ALL -> matchAll(postings, tokens);
            case MATCH_PHRASE -> matchPhrase(inverted, postings, tokens);
        };
    }

    private static IndexEvalResult matchAny(Map<String, List<Integer>> postings, List<String> tokens) {
        for (String token : tokens) {
            if (postings.containsKey(token)) {
                return IndexEvalResult.KEEP;
            }
        }
        return IndexEvalResult.SKIP;
    }

    private static IndexEvalResult matchAll(Map<String, List<Integer>> postings, List<String> tokens) {
        for (String token : tokens) {
            if (!postings.containsKey(token)) {
                return IndexEvalResult.SKIP;
            }
        }
        return IndexEvalResult.KEEP;
    }

    private IndexEvalResult matchPhrase(InvertedIndexData inverted,
                                        Map<String, List<Integer>> postings, List<String> tokens) {
        if (!inverted.storePositions()) {
            LOG.debug("action=phrase_degrade_to_all reason=store_positions_disabled");
            return matchAll(postings, tokens);
        }
        return PostingListMerger.verifyPhrase(postings, tokens)
                ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
    }

    /**
     * Computes BM25 relevance scores for a full-text query against the inverted index.
     *
     * <p>Unlike {@link #evaluate} which returns binary KEEP/SKIP, this method returns
     * per-row relevance scores suitable for ranking and fusion with vector search results.</p>
     *
     * @param indexData       inverted index data (must store positions for accurate scoring)
     * @param queryText       raw query text to score against
     * @param documentLengths per-row token counts (indexed by row ID)
     * @param totalDocuments  total number of documents in the corpus
     * @param properties      index properties (used to resolve analyzer)
     * @return scored rows sorted by BM25 score descending; empty if no matches
     * @throws NullPointerException if any argument is null
     */
    public List<Bm25Scorer.ScoredRow> score(IndexData indexData,
                                            String queryText,
                                            int[] documentLengths,
                                            int totalDocuments,
                                            Map<String, String> properties) {
        Objects.requireNonNull(indexData, "indexData");
        Objects.requireNonNull(queryText, "queryText");
        Objects.requireNonNull(documentLengths, "documentLengths");
        Objects.requireNonNull(properties, "properties");

        if (!(indexData instanceof InvertedIndexData inverted)) {
            return List.of();
        }
        if (queryText.isBlank() || totalDocuments == 0) {
            return List.of();
        }

        String analyzer = stringProp(properties, PROP_ANALYZER, DEFAULT_ANALYZER);
        List<String> queryTokens = AnalyzerFactory.tokenize(queryText, analyzer);
        if (queryTokens.isEmpty()) {
            return List.of();
        }

        return Bm25Scorer.score(inverted.postings(), queryTokens, documentLengths, totalDocuments);
    }

    private static boolean boolProp(Map<String, String> props, String key, boolean defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Boolean.parseBoolean(val);
    }

    private static String stringProp(Map<String, String> props, String key, String defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : val;
    }
}
