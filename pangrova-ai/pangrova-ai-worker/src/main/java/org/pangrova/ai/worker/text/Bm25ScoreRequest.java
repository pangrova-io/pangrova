// Licensed to Pangrova under one or more contributor license agreements.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.ai.worker.text;

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
    Bm25ScoreRequest {
        requireArrays(idfs, termFreqs, docLengths);
        requireDimensions(idfs, termFreqs, docLengths, numDocs, numTerms);
        requireParameters(k1, b, avgDocLength);
        idfs = idfs.clone();
        termFreqs = termFreqs.clone();
        docLengths = docLengths.clone();
    }

    private static void requireArrays(double[] idfs, int[] termFreqs, int[] docLengths) {
        if (idfs == null || termFreqs == null || docLengths == null) {
            throw new IllegalArgumentException("BM25 input arrays must not be null");
        }
    }

    private static void requireDimensions(
            double[] idfs, int[] termFreqs, int[] docLengths, int numDocs, int numTerms) {
        if (numDocs < 0 || numTerms < 0) {
            throw new IllegalArgumentException("numDocs and numTerms must be non-negative");
        }
        if (idfs.length != numTerms) {
            throw new IllegalArgumentException("idfs length must equal numTerms");
        }
        if (docLengths.length != numDocs) {
            throw new IllegalArgumentException("docLengths length must equal numDocs");
        }
        if (termFreqs.length != Math.multiplyExact(numDocs, numTerms)) {
            throw new IllegalArgumentException("termFreqs length must equal numDocs * numTerms");
        }
    }

    private static void requireParameters(double k1, double b, double avgDocLength) {
        if (!Double.isFinite(k1) || k1 <= 0.0) {
            throw new IllegalArgumentException("Invalid BM25 k1 parameter");
        }
        if (!Double.isFinite(b) || b < 0.0 || b > 1.0) {
            throw new IllegalArgumentException("Invalid BM25 b parameter");
        }
        if (!Double.isFinite(avgDocLength) || avgDocLength <= 0.0) {
            throw new IllegalArgumentException("Invalid BM25 average document length");
        }
    }

    @Override
    public double[] idfs() {
        return idfs.clone();
    }

    @Override
    public int[] termFreqs() {
        return termFreqs.clone();
    }

    @Override
    public int[] docLengths() {
        return docLengths.clone();
    }
}
