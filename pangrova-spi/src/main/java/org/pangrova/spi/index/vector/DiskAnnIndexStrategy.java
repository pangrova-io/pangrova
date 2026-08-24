/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.spi.index.vector;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

/**
 * SPI interface for DiskANN-based approximate nearest neighbor index operations.
 *
 * <p>DiskANN stores the graph structure on SSD with only PQ-compressed codes
 * in memory, enabling 10x memory reduction compared to HNSW at slightly
 * higher query latency.</p>
 *
 * <p>Implementations are loaded via {@link java.util.ServiceLoader} and called
 * from {@code pangrova-indexing}'s VectorStrategyProvider. The implementation
 * lives in {@code pangrova-worker} where native JNI access is available.</p>
 */
@Experimental(reason = "DiskANN native index strategy")
public interface DiskAnnIndexStrategy {

    /**
     * Result of building a DiskANN index.
     *
     * @param vectorCount    number of vectors indexed
     * @param graphSizeBytes approximate size of the graph on disk
     * @param pqCodeSizeBytes size of PQ codes in memory
     */
    record BuildResult(long vectorCount, int graphSizeBytes, int pqCodeSizeBytes) {}

    /**
     * Parameters for DiskANN index construction.
     *
     * @param maxDegree     maximum out-degree of Vamana graph nodes (R parameter)
     * @param buildListSize beam width during graph construction (L parameter)
     * @param pqSubvectors  number of PQ subvectors for compression
     */
    record BuildParams(int maxDegree, int buildListSize, int pqSubvectors) {
        public static final BuildParams DEFAULT = new BuildParams(64, 100, 32);
    }

    /**
     * Builds a DiskANN index from the given vectors and writes it to the output directory.
     *
     * @param vectors         input vectors (row-major)
     * @param dimension       vector dimensionality
     * @param metric          distance metric for the index
     * @param params          build parameters
     * @param outputDirectory directory to write index files to
     * @return build result with statistics
     */
    BuildResult buildIndex(float[][] vectors, int dimension, DistanceMetric metric,
                           BuildParams params, String outputDirectory);

    /**
     * Searches the DiskANN index at the given path.
     *
     * @param indexPath      path to the index directory on disk
     * @param queryVector    query vector
     * @param topK           number of nearest neighbors to return
     * @param searchListSize beam width during search (higher = better recall, slower)
     * @return scored rows ordered by distance ascending
     */
    List<VectorSearchResult.ScoredRow> search(String indexPath, float[] queryVector,
                                               int topK, int searchListSize);

    /**
     * Searches the DiskANN index with a pre-filter bitset.
     *
     * <p>Only rows whose corresponding bit is set in the bitset are considered
     * during reranking, enabling efficient WHERE-clause pushdown into the
     * native search layer. Implementations that do not support native filtering
     * fall back to unfiltered search (callers must post-filter).</p>
     *
     * @param indexPath        path to the index directory on disk
     * @param queryVector      query vector
     * @param topK             number of nearest neighbors to return
     * @param searchListSize   beam width during search
     * @param preFilterBitset  bit-packed filter (bit i set = row i is allowed)
     * @return scored rows ordered by distance ascending
     */
    default List<VectorSearchResult.ScoredRow> searchFiltered(String indexPath, float[] queryVector,
                                                              int topK, int searchListSize,
                                                              byte[] preFilterBitset) {
        return search(indexPath, queryVector, topK, searchListSize);
    }
}
