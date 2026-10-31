// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.ai.worker.text;

import org.pangrova.worker.nativebridge.jni.NativeLibraryLoader;

/**
 * BM25 batch scorer with native SIMD acceleration.
 *
 * <p>Delegates to the canonical native C++ data-plane implementation through JNI.
 * Native runtime or ABI failures are surfaced to the caller; BM25 has no JVM
 * execution path.
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
        return nativeScoreBatch(
                request.idfs(), request.termFreqs(), request.docLengths(),
                request.numDocs(), request.numTerms(), request.k1(), request.b(),
                request.avgDocLength());
    }

    // CHECKSTYLE.OFF: ParameterNumber -- Stable JNI ABI mirrored by JniBridge.cpp.
    private static native double[] nativeScoreBatch(
            double[] idfs, int[] termFreqs, int[] docLengths,
            int numDocs, int numTerms, double k1, double b, double avgDocLength);
    // CHECKSTYLE.ON: ParameterNumber

}
