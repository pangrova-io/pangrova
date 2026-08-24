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
package org.pangrova.worker.execution;

import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.FilteredSearchStrategy;
import org.pangrova.spi.index.vector.VectorIndexLoader;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Worker-side executor for vector ANN search tasks.
 *
 * <p>Delegates all index loading and search to {@link VectorIndexLoader} SPI,
 * which is discovered via {@link java.util.ServiceLoader} at runtime. The
 * Worker never depends on {@code pangrova-indexing} directly.</p>
 *
 * <p>Invoked by the native task execution path when a Substrait plan contains
 * a VectorSearchOperator. Results flow back through the standard result
 * transport.</p>
 *
 * <p>Thread-safe: all state is in the injected (thread-safe) loader.</p>
 */
public final class VectorSearchExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(VectorSearchExecutor.class);

    private final VectorIndexLoader indexLoader;
    private final RetrievalQualitySampler sampler;

    /**
     * @param indexLoader SPI implementation discovered via ServiceLoader
     */
    public VectorSearchExecutor(VectorIndexLoader indexLoader) {
        this(indexLoader, null);
    }

    /**
     * @param indexLoader SPI implementation discovered via ServiceLoader
     * @param sampler     optional sampler for retrieval quality monitoring (null to disable)
     */
    public VectorSearchExecutor(VectorIndexLoader indexLoader, RetrievalQualitySampler sampler) {
        this.indexLoader = indexLoader;
        this.sampler = sampler;
    }

    /**
     * Executes a vector search on a single data file.
     *
     * @param dataFilePath path of the data file to search
     * @param vectorColumn name of the vector column
     * @param queryVector  query embedding
     * @param topK         number of nearest neighbors
     * @param metric       distance metric
     * @param searchParams algorithm-specific parameters (efSearch, nprobe, etc.)
     * @return search result with scored neighbor rows
     */
    public VectorSearchResult execute(
            String dataFilePath,
            String vectorColumn,
            float[] queryVector,
            int topK,
            DistanceMetric metric,
            Map<String, String> searchParams) {
        return execute(dataFilePath, vectorColumn, queryVector, topK, metric, searchParams, null);
    }

    /**
     * Executes a vector search on a single data file with an explicit filter strategy.
     */
    public VectorSearchResult execute(
            String dataFilePath,
            String vectorColumn,
            float[] queryVector,
            int topK,
            DistanceMetric metric,
            Map<String, String> searchParams,
            FilteredSearchStrategy filterStrategy) {

        VectorSearchRequest request = new VectorSearchRequest(
                vectorColumn, queryVector, topK, metric, filterStrategy);

        LOG.debug("action=vector_search file={} column={} topK={} metric={} strategy={}",
                dataFilePath, vectorColumn, topK, metric,
                request.effectiveFilteredSearchStrategy());

        VectorSearchResult result = indexLoader.searchFile(
                dataFilePath, vectorColumn, request, searchParams);

        if (sampler != null) {
            sampler.sampleIfEligible(dataFilePath, vectorColumn, queryVector, topK, metric, result);
        }

        return result;
    }
}
