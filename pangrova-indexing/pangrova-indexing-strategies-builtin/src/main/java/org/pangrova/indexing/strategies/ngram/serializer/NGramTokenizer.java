package org.pangrova.indexing.strategies.ngram.serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Splits strings into character n-grams for N-gram bloom filter indexing.
 */
public final class NGramTokenizer {

    private NGramTokenizer() {}

    /**
     * Extracts all character n-grams of the specified size from the input string.
     *
     * @param text     input string
     * @param ngramSize n-gram size (e.g., 3 for trigrams)
     * @return list of n-gram strings
     */
    public static List<String> tokenize(String text, int ngramSize) {
        if (text == null || text.length() < ngramSize) {
            return List.of();
        }
        String lower = text.toLowerCase(Locale.ROOT);
        List<String> ngrams = new ArrayList<>(lower.length() - ngramSize + 1);
        for (int i = 0; i <= lower.length() - ngramSize; i++) {
            ngrams.add(lower.substring(i, i + ngramSize));
        }
        return ngrams;
    }
}
