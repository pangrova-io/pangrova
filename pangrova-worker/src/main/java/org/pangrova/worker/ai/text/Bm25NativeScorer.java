// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.worker.ai.text;

import org.pangrova.worker.jni.NativeLibraryLoader;

/**
 * BM25 batch scorer with native SIMD acceleration.
 *
 * <p>Delegates to native C++ implementation via JNI when the native library is
 * available and functional. Falls back to a pure-Java implementation otherwise.
 *
 * <p>Input layout:
 * <ul>
 *   <li>{@code idfs}: IDF values per term, length = numTerms
 *   <li>{@code termFreqs}: term frequency matrix [numTerms][numDocs] flattened row-major
 *   <li>{@code docLengths}: document lengths, length = numDocs
 * </ul>
 *
 * <p>Output: BM25 scores per document, length = numDocs (sum of per-term scores).
 */
public final class Bm25NativeScorer {

    static {
        NativeLibraryLoader.load();
    }

    private Bm25NativeScorer() {}

    /**
     * Compute BM25 scores for a batch of documents against multiple query terms.
     *
     * @param request scoring inputs (see {@link Bm25ScoreRequest})
     * @return BM25 scores for each document (length = numDocs)
     */
    public static double[] scoreBatch(Bm25ScoreRequest request) {
        if (NativeLibraryLoader.isFunctional()) {
            try {
                return nativeScoreBatchRequest(request);
            } catch (UnsatisfiedLinkError e) {
                // Native binding for the record-based entry point is unavailable —
                // degrade to the pure-Java implementation for the process lifetime.
                NativeLibraryLoader.markUnsupported();
            }
        }
        return javaFallbackScoreBatch(request);
    }

    private static native double[] nativeScoreBatchRequest(Bm25ScoreRequest request);

    /**
     * Pure-Java fallback BM25 scorer.
     */
    private static double[] javaFallbackScoreBatch(Bm25ScoreRequest request) {
        double[] scores = new double[request.numDocs()];
        double k1 = request.k1();
        double b = request.b();
        double avgDocLength = request.avgDocLength();
        for (int t = 0; t < request.numTerms(); t++) {
            double idf = request.idfs()[t];
            for (int d = 0; d < request.numDocs(); d++) {
                double tf = request.termFreqs()[t * request.numDocs() + d];
                double dl = request.docLengths()[d];
                double dlNorm = 1.0 - b + b * (dl / avgDocLength);
                scores[d] += idf * (tf * (k1 + 1.0)) / (tf + k1 * dlNorm);
            }
        }
        return scores;
    }
}
