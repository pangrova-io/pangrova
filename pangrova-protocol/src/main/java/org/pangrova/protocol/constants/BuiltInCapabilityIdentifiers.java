package org.pangrova.protocol.constants;

/** Public capability IDs, versions and reserved column names shared by planning and execution. */
public final class BuiltInCapabilityIdentifiers {
    private BuiltInCapabilityIdentifiers() {}

    public static final String EXACT_TOP_K_ID = "pangrova.retrieval.topk.exact";
    public static final String EXACT_TOP_K_VERSION = "1.0.0";
    public static final String RRF_MERGE_ID = "pangrova.retrieval.merge.rrf";
    public static final String LINEAR_MERGE_ID = "pangrova.retrieval.merge.linear";
    public static final String RETRIEVAL_MERGE_VERSION = "1.0.0";
    public static final String CROSS_ENCODER_RERANK_ID =
            "pangrova.retrieval.rerank.cross_encoder";
    public static final String CROSS_ENCODER_RERANK_VERSION = "1.0.0";
    public static final String AI_TEXT_GENERATION_ID =
            "pangrova.ai.invoke.text_generation";
    public static final String AI_MULTIMODAL_GENERATION_ID =
            "pangrova.ai.invoke.multimodal_generation";
    public static final String AI_EMBEDDING_ID = "pangrova.ai.invoke.embedding";
    public static final String AI_INVOCATION_VERSION = "1.0.0";
    public static final String NATIVE_BUILD_ID = "pangrova-native-0.1.0";
    public static final String DISKANN_ARTIFACT_BUILD_ID =
            "pangrova.artifact.build.diskann";
    public static final String DISKANN_ARTIFACT_BUILD_VERSION = "2.0.0";
    public static final String DISKANN_RETRIEVAL_ID =
            "pangrova.retrieval.vector.diskann";
    public static final String DISKANN_RETRIEVAL_VERSION = "1.0.0";
    public static final String FILTERED_DISKANN_RETRIEVAL_ID =
            "pangrova.retrieval.vector.diskann.filtered";
    public static final String FILTERED_DISKANN_RETRIEVAL_VERSION = "1.0.0";
    public static final String DISKANN_ARTIFACT_FORMAT_ID = "pangrova.diskann.segment";
    public static final int DISKANN_ARTIFACT_FORMAT_MAJOR = 2;
    public static final String HNSW_ARTIFACT_BUILD_ID =
            "pangrova.artifact.build.hnsw";
    public static final String HNSW_ARTIFACT_BUILD_VERSION = "2.0.0";
    public static final String HNSW_RETRIEVAL_ID =
            "pangrova.retrieval.vector.hnsw";
    public static final String HNSW_RETRIEVAL_VERSION = "2.0.0";
    public static final String HNSW_ARTIFACT_FORMAT_ID = "pangrova.hnsw.segment";
    public static final int HNSW_ARTIFACT_FORMAT_MAJOR = 2;
    public static final String IVF_PQ_ARTIFACT_BUILD_ID =
            "pangrova.artifact.build.ivf_pq";
    public static final String IVF_PQ_ARTIFACT_BUILD_VERSION = "2.0.0";
    public static final String IVF_PQ_RETRIEVAL_ID =
            "pangrova.retrieval.vector.ivf_pq";
    public static final String IVF_PQ_RETRIEVAL_VERSION = "2.0.0";
    public static final String IVF_PQ_ARTIFACT_FORMAT_ID = "pangrova.ivfpq.segment";
    public static final int IVF_PQ_ARTIFACT_FORMAT_MAJOR = 2;
    public static final String BM25_ARTIFACT_BUILD_ID =
            "pangrova.artifact.build.bm25";
    public static final String BM25_ARTIFACT_BUILD_VERSION = "1.0.0";
    public static final String BM25_RETRIEVAL_ID =
            "pangrova.retrieval.text.bm25";
    public static final String BM25_RETRIEVAL_VERSION = "1.0.0";
    public static final String BM25_ARTIFACT_FORMAT_ID = "pangrova.bm25.segment";
    public static final int BM25_ARTIFACT_FORMAT_MAJOR = 1;
    public static final String GENERATED_VALUES_ARTIFACT_BUILD_ID =
            "pangrova.artifact.build.generated_values";
    public static final String GENERATED_VALUES_ARTIFACT_BUILD_VERSION = "2.0.0";
    public static final String GENERATED_VALUES_ARTIFACT_FORMAT_ID =
            "pangrova.generated-values.segment";
    public static final int GENERATED_VALUES_ARTIFACT_FORMAT_MAJOR = 2;
    public static final String GENERATED_VALUES_OVERLAY_ID =
            "pangrova.generated-values.overlay";
    public static final String GENERATED_VALUES_OVERLAY_VERSION = "2.0.0";
    public static final String TABULAR_EXPORT_ID =
            "pangrova.artifact.export.tabular";
    public static final String TABULAR_EXPORT_VERSION = "1.0.0";
    public static final String TABULAR_EXPORT_FORMAT_ID =
            "pangrova.tabular-export.arrow-stream";
    public static final int TABULAR_EXPORT_FORMAT_MAJOR = 1;
    /** Reserved Native scan output carrying the source object's physical row position. */
    public static final String PHYSICAL_ROW_POSITION_COLUMN = "$pangrova$row_position";
    /** Reserved Native scan output carrying an artifact-materialized retrieval score. */
    public static final String MATERIALIZED_RETRIEVAL_SCORE_COLUMN =
            "$pangrova$retrieval_score";
}
