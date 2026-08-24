// Licensed to Pangrova under one or more contributor license agreements.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.worker.ai.text;

/**
 * Immutable request describing a BM25 batch scoring invocation.
 *
 * <p>Input layout:
 * <ul>
 *   <li>{@code idfs}: IDF values per term, length = numTerms
 *   <li>{@code termFreqs}: term frequency matrix [numTerms][numDocs] flattened row-major
 *   <li>{@code docLengths}: document lengths, length = numDocs
 * </ul>
 *
 * @param idfs         IDF values per term (length = numTerms)
 * @param termFreqs    term frequency matrix flattened [numTerms * numDocs]
 * @param docLengths   document lengths (length = numDocs)
 * @param numDocs      number of documents
 * @param numTerms     number of query terms
 * @param k1           BM25 k1 parameter (typically 1.2)
 * @param b            BM25 b parameter (typically 0.75)
 * @param avgDocLength average document length in corpus
 */
record Bm25ScoreRequest(double[] idfs, int[] termFreqs, int[] docLengths,
                        int numDocs, int numTerms,
                        double k1, double b, double avgDocLength) {
}
