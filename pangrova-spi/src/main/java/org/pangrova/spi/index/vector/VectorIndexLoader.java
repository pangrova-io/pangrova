package org.pangrova.spi.index.vector;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;

/**
 * SPI for loading and searching vector indexes from the Worker side.
 *
 * <p>This interface bridges the Worker module to the indexing subsystem
 * without introducing a compile-time dependency on {@code pangrova-indexing}.
 * The default implementation ({@code DefaultVectorIndexLoader}) lives in
 * {@code pangrova-indexing} and is discovered via {@link java.util.ServiceLoader}.</p>
 *
 * <p>Implementations must be thread-safe.</p>
 */
@Experimental
public interface VectorIndexLoader {

    /**
     * Loads the vector index for the given data file and executes an ANN search.
     *
     * <p>If no READY vector index exists for the file, the implementation
     * should return an empty result (no neighbors).</p>
     *
     * @param dataFilePath              path of the data file to search
     * @param vectorColumn              name of the vector column
     * @param request                   ANN search request (query vector, topK, metric)
     * @param searchParams              algorithm-specific search parameters (e.g. efSearch, nprobe)
     * @return search result containing scored neighbor rows for this file
     */
    VectorSearchResult searchFile(String dataFilePath,
                                  String vectorColumn,
                                  VectorSearchRequest request,
                                  Map<String, String> searchParams);
}
