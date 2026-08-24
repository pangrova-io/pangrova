package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * REST DTO mirror of the {@code VectorSearchInfo} proto message
 * (see {@code pangrova/protocol/v1/vector_search_info.proto}). Carried as
 * a first-class field on {@link SubmitTaskRequest} so the coordinator
 * can ship vector ANN execution metadata to the worker without smuggling
 * a Base64-encoded protobuf through {@code session_properties}
 * (F-CHAIN-INDEX-06).
 *
 * <p>Field semantics follow the proto schema; the Jackson serializer
 * writes the JSON form on the wire and the worker copies the same fields
 * into the protobuf {@code TaskRequest.vector_search} when it crosses the
 * JNI boundary into the native engine.</p>
 *
 * @param tableName       fully qualified table name (catalog.schema.table)
 * @param vectorColumn    name of the vector column to search
 * @param queryVector     query embedding values
 * @param topK            number of nearest neighbors per worker
 * @param distanceMetric  distance metric name (L2, COSINE, INNER_PRODUCT)
 * @param indexId         vector index identifier; empty/null triggers brute-force
 * @param searchParams    algorithm-specific parameters (efSearch, nprobe, ...)
 * @param topKOversample  oversample factor; {@code <=0} disables oversampling
 * @param filterPredicate Substrait expression bytes for pre-ANN filtering
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VectorSearchInfoPayload(
        @JsonProperty("table_name") String tableName,
        @JsonProperty("vector_column") String vectorColumn,
        @JsonProperty("query_vector") List<Float> queryVector,
        @JsonProperty("top_k") int topK,
        @JsonProperty("distance_metric") String distanceMetric,
        @JsonProperty("index_id") String indexId,
        @JsonProperty("search_params") Map<String, String> searchParams,
        @JsonProperty("top_k_oversample") int topKOversample,
        @JsonProperty("filter_predicate") List<byte[]> filterPredicate
) {

    public VectorSearchInfoPayload {
        queryVector = queryVector == null ? List.of() : List.copyOf(queryVector);
        searchParams = searchParams == null ? Map.of() : Map.copyOf(searchParams);
        filterPredicate = filterPredicate == null ? List.of() : List.copyOf(filterPredicate);
    }
}
