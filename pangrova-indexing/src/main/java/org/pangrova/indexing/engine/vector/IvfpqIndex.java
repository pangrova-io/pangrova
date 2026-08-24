package org.pangrova.indexing.engine.vector;

import java.util.Collections;
import java.util.List;

/**
 * In-memory IVFPQ index structure used during build and search.
 *
 * <p>Package-private: only used by {@link IvfpqEngine} and {@link IvfpqCodec}.
 * Stores IVF centroids, PQ codebook, and inverted lists of encoded vectors.</p>
 */
final class IvfpqIndex {

    private final float[][] centroids;       // [nlist][dimension]
    private final float[][][] codebook;      // [pqM][ksub][subDim]
    private final List<List<EncodedVector>> invertedLists; // [nlist] → list of encoded vectors
    private final int dimension;
    private final int pqM;
    private final int nbits;
    private final int vectorCount;

    IvfpqIndex(float[][] centroids, float[][][] codebook,
               List<List<EncodedVector>> invertedLists,
               int dimension, int pqM, int nbits, int vectorCount) {
        this.centroids = centroids;
        this.codebook = codebook;
        this.invertedLists = invertedLists;
        this.dimension = dimension;
        this.pqM = pqM;
        this.nbits = nbits;
        this.vectorCount = vectorCount;
    }

    float[][] centroids() { return centroids; }
    float[][][] codebook() { return codebook; }
    int nlist() { return centroids.length; }
    int dimension() { return dimension; }
    int pqM() { return pqM; }
    int nbits() { return nbits; }
    int ksub() { return 1 << nbits; }
    int subDim() { return dimension / pqM; }
    int vectorCount() { return vectorCount; }

    List<EncodedVector> invertedList(int clusterId) {
        if (clusterId < 0 || clusterId >= invertedLists.size()) {
            return Collections.emptyList();
        }
        return invertedLists.get(clusterId);
    }

    /** A PQ-encoded vector with its original row offset. */
    record EncodedVector(long rowOffset, int[] codes) {}
}
