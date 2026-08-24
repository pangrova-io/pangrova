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
package org.pangrova.indexing.engine.vector;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DiskAnnIndexStrategy;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges vector strategy dispatch to an optional DiskANN implementation.
 */
final class DiskAnnVectorOperations {

    private static final Logger LOG = LoggerFactory.getLogger(
            DiskAnnVectorOperations.class);

    private final DiskAnnIndexStrategy diskAnnStrategy;

    DiskAnnVectorOperations() {
        this.diskAnnStrategy = loadStrategy();
    }

    /**
     * DiskANN is optional, so an unavailable provider must degrade to {@code null} rather than
     * propagate. Beyond "not registered", the provider can also be registered but unloadable: the
     * engine's static initializer links the native library, so without a native build
     * {@link ServiceLoader#findFirst()} raises {@link ServiceConfigurationError} wrapping an
     * {@link UnsatisfiedLinkError}. Letting that escape aborted the whole index-engine registry
     * (and with it coordinator startup) on any JVM-only build, which defeats the optionality.
     */
    private static DiskAnnIndexStrategy loadStrategy() {
        try {
            DiskAnnIndexStrategy strategy = ServiceLoader.load(DiskAnnIndexStrategy.class)
                    .findFirst()
                    .orElse(null);
            if (strategy == null) {
                LOG.info("event=diskann_strategy_unavailable reason=no_provider_on_classpath");
            }
            return strategy;
        } catch (ServiceConfigurationError e) {
            LOG.info("event=diskann_strategy_unavailable reason=provider_init_failed "
                    + "detail=\"native runtime likely absent; DiskANN operations will be rejected\" "
                    + "error={}", e.toString());
            LOG.debug("event=diskann_strategy_init_failure_detail", e);
            return null;
        }
    }

    VectorIndexData build(List<float[]> vectors, int dimension,
                          DistanceMetric metric, Map<String, String> properties) {
        requireAvailable();
        String outputDir = outputDirectory(properties);
        DiskAnnIndexStrategy.BuildParams params = new DiskAnnIndexStrategy.BuildParams(
                intProp(properties, "max_degree", 64),
                intProp(properties, "build_list_size", 100),
                intProp(properties, "pq_subvectors", 32));
        return buildIndex(vectors, dimension, metric, params, outputDir);
    }

    /**
     * DiskANN has no incremental merge path: the persisted index keeps only the
     * compressed graph plus companion vectors, and this method receives just the
     * NEW batch, not the previously-indexed vectors. Re-encoding the delta alone
     * would silently drop all existing rows (data loss), so this fails fast —
     * callers must rebuild from the complete source vector set via {@link #build}.
     */
    VectorIndexData rebuild(VectorIndexData existingData, List<float[]> newVectors,
                            int dimension, DistanceMetric metric) {
        throw new UnsupportedOperationException(
                "DiskANN does not support incremental build/merge: the on-disk index does not "
                        + "retain existing vectors, so re-encoding only the " + newVectors.size()
                        + " new vectors would drop previously-indexed data. Rebuild from the "
                        + "full vector set via build().");
    }

    List<VectorSearchResult.ScoredRow> search(VectorIndexData vectorData,
                                              VectorSearchRequest request,
                                              Map<String, String> searchParams) {
        requireAvailable();
        return diskAnnStrategy.search(indexPath(vectorData), request.queryVector(),
                request.topK(), searchListSize(searchParams));
    }

    List<VectorSearchResult.ScoredRow> searchFiltered(
            VectorIndexData vectorData, VectorSearchRequest request,
            Map<String, String> searchParams, Set<Long> allowedRows) {
        requireAvailable();
        List<VectorSearchResult.ScoredRow> results = diskAnnStrategy.searchFiltered(
                indexPath(vectorData), request.queryVector(), request.topK(),
                searchListSize(searchParams), buildBitset(allowedRows));
        return results.stream()
                .filter(result -> allowedRows.contains(result.rowOffset()))
                .limit(request.topK())
                .toList();
    }

    private VectorIndexData buildIndex(List<float[]> vectors, int dimension,
                                       DistanceMetric metric,
                                       DiskAnnIndexStrategy.BuildParams params,
                                       String outputDir) {
        DiskAnnIndexStrategy.BuildResult result = diskAnnStrategy.buildIndex(
                vectors.toArray(new float[0][]), dimension, metric, params, outputDir);
        return new VectorIndexData(
                VectorAlgorithmType.DISK_ANN, dimension, metric,
                (int) result.vectorCount(),
                outputDir.getBytes(StandardCharsets.UTF_8),
                Map.of());
    }

    private void requireAvailable() {
        if (diskAnnStrategy != null) {
            return;
        }
        throw new UnsupportedOperationException(
                "DiskANN requires native C++ runtime. Ensure pangrova-worker with "
                        + "native library is on the classpath and DiskAnnIndexStrategy "
                        + "ServiceLoader provider is registered.");
    }

    private static String outputDirectory(Map<String, String> properties) {
        String indexBasePath = System.getProperty("pangrova.index.basePath",
                System.getProperty("java.io.tmpdir"));
        return properties.getOrDefault("output_directory",
                indexBasePath + "/diskann-" + System.nanoTime());
    }

    private static String indexPath(VectorIndexData vectorData) {
        return new String(vectorData.indexPayload(), StandardCharsets.UTF_8);
    }

    private static int searchListSize(Map<String, String> searchParams) {
        return Integer.parseInt(searchParams.getOrDefault("search_list_size", "128"));
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }

    private static byte[] buildBitset(Set<Long> allowedRows) {
        if (allowedRows.isEmpty()) {
            return new byte[0];
        }
        long maxRow = allowedRows.stream().mapToLong(Long::longValue).max().orElse(0);
        byte[] bitset = new byte[(int) ((maxRow / 8) + 1)];
        for (long rowId : allowedRows) {
            bitset[(int) (rowId / 8)] |= (byte) (1 << (int) (rowId % 8));
        }
        return bitset;
    }
}
