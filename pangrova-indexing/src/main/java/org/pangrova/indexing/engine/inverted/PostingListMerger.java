package org.pangrova.indexing.engine.inverted;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Phrase verification over rowId-keyed posting lists.
 *
 * <p>The posting lists produced by {@link InvertedStrategyProvider} store
 * one rowId per token occurrence (no positions). With that encoding the
 * strictest sound guarantee phrase matching can give at index-eval time is
 * "all phrase tokens co-occur in the same row" (a superset of true phrase
 * matches). This eliminates the previous false-positive of accepting
 * phrases that crossed row boundaries; precise phrase verification with
 * positions still has to fall back to scanning the data file, but the
 * posting-list step now correctly prunes rows missing any phrase token.</p>
 */
final class PostingListMerger {

    private PostingListMerger() {}

    /**
     * Returns true if at least one rowId appears in the posting list of
     * every phrase token. This is a sound superset of true positional
     * phrase matching: positives may need data-file re-verification, but
     * negatives are guaranteed (a token absent from a row cannot be part
     * of a phrase in that row).
     */
    static boolean verifyPhrase(Map<String, List<Integer>> postings, List<String> tokens) {
        if (tokens.isEmpty()) return false;
        if (tokens.size() == 1) return postings.containsKey(tokens.get(0));

        Set<Integer> candidateRows = null;
        for (String token : tokens) {
            List<Integer> entries = postings.get(token);
            if (entries == null || entries.isEmpty()) {
                return false;
            }
            Set<Integer> rows = new HashSet<>(entries);
            if (candidateRows == null) {
                candidateRows = rows;
            } else {
                candidateRows.retainAll(rows);
                if (candidateRows.isEmpty()) {
                    return false;
                }
            }
        }
        // The loop ran at least once (tokens.size() >= 2), so candidateRows is set.
        return !candidateRows.isEmpty();
    }
}
