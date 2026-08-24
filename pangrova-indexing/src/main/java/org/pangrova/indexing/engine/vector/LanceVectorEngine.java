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

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Java lifecycle orchestrator for Lance-based vector indexes.
 *
 * <p>Lance provides columnar vector storage with built-in IVF_PQ and
 * IVF_HNSW_SQ index support via its Rust library. This engine delegates
 * index creation and ANN search to the native layer through
 * {@code LanceFfiBridge} JNI calls.</p>
 *
 * <p>Build and search operations are native-only; this class manages
 * lifecycle metadata (descriptor updates, algorithm selection) and
 * validates parameters before dispatching to native.</p>
 */
final class LanceVectorEngine {

    private static final Logger LOG = LoggerFactory.getLogger(LanceVectorEngine.class);
    private static final String PROP_LANCE_INDEX_TYPE = "lance.index_type";
    private static final String PROP_LANCE_NUM_PARTITIONS = "lance.num_partitions";
    private static final String PROP_LANCE_NUM_SUB_VECTORS = "lance.num_sub_vectors";
    private static final String PROP_LANCE_NUM_PROBES = "lance.num_probes";
    private static final String DEFAULT_INDEX_TYPE = "IVF_PQ";
    private static final int DEFAULT_NUM_PARTITIONS = 256;
    private static final int DEFAULT_NUM_SUB_VECTORS = 16;
    private static final int DEFAULT_NUM_PROBES = 10;

    VectorIndexData build(List<float[]> vectors, int dimension,
                          DistanceMetric metric, Map<String, String> properties) {
        String indexType = properties.getOrDefault(PROP_LANCE_INDEX_TYPE, DEFAULT_INDEX_TYPE);
        int numPartitions = intProp(properties, PROP_LANCE_NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
        int numSubVectors = intProp(properties, PROP_LANCE_NUM_SUB_VECTORS, DEFAULT_NUM_SUB_VECTORS);

        LOG.info("event=lance_index_build_requested indexType={} vectors={} dimension={} "
                        + "metric={} numPartitions={} numSubVectors={}",
                indexType, vectors.size(), dimension, metric, numPartitions, numSubVectors);

        byte[] payload = serializeBuildMetadata(indexType, numPartitions, numSubVectors);

        return new VectorIndexData(
                resolveAlgorithmType(indexType),
                dimension,
                metric,
                vectors.size(),
                payload,
                Map.copyOf(properties));
    }

    List<VectorSearchResult.ScoredRow> search(
            VectorIndexData indexData, VectorSearchRequest request,
            Map<String, String> searchParams) {
        int numProbes = intProp(searchParams, PROP_LANCE_NUM_PROBES, DEFAULT_NUM_PROBES);

        LOG.info("event=lance_index_search indexType={} dimension={} topK={} numProbes={}",
                indexData.algorithmType(), indexData.dimension(),
                request.topK(), numProbes);

        throw new UnsupportedOperationException(
                "Lance ANN search must be executed through the native C++ path via LanceFfiBridge");
    }

    private static VectorAlgorithmType resolveAlgorithmType(String lanceIndexType) {
        return switch (lanceIndexType.toUpperCase()) {
            case "IVF_PQ" -> VectorAlgorithmType.IVFPQ;
            case "IVF_HNSW_SQ", "HNSW" -> VectorAlgorithmType.HNSW;
            default -> VectorAlgorithmType.IVFPQ;
        };
    }

    private static byte[] serializeBuildMetadata(String indexType, int numPartitions, int numSubVectors) {
        String metadata = String.format("lance:%s:%d:%d", indexType, numPartitions, numSubVectors);
        return metadata.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }
}
