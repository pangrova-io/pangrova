package org.pangrova.protocol;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.RowLocatorContract;
import org.pangrova.protocol.v2.RowLocatorKind;
import org.pangrova.spi.annotation.Internal;

import java.util.HexFormat;

/** Canonical semantic identities for capabilities implemented by Pangrova itself. */
@Internal
public final class BuiltInCapabilityCatalog {

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
    private static final byte[] FILTERED_DISKANN_RETRIEVAL_SIGNATURE = HexFormat.of().parseHex(
            "ae26c57846bf7327447975abdaef0ee5d4e03aa855b902cb0192c8b360047716");

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

    private static final byte[] EXACT_TOP_K_SIGNATURE = {
            (byte) 0xEA, 0x4C, 0x6E, 0x76, 0x73, (byte) 0xF6, 0x06, 0x73,
            (byte) 0xA5, 0x1E, (byte) 0xE3, 0x53, (byte) 0x9D, 0x38, 0x66, 0x0D,
            0x03, (byte) 0xDD, (byte) 0x9B, 0x2C, 0x54, 0x1A, 0x23, 0x1B,
            0x46, (byte) 0x84, (byte) 0x81, 0x0E, (byte) 0xFC, (byte) 0xCE, 0x14, 0x09};

    private static final byte[] RRF_MERGE_SIGNATURE = {
            0x32, 0x66, (byte) 0x98, (byte) 0xD1, (byte) 0xA9, (byte) 0x83, (byte) 0x9E, (byte) 0xE9,
            (byte) 0xD5, (byte) 0xC4, (byte) 0x90, 0x41, 0x58, (byte) 0xB6, 0x6A, 0x7B,
            (byte) 0xF2, 0x29, (byte) 0xAB, (byte) 0xFE, (byte) 0xEA, (byte) 0xEC, 0x4E, (byte) 0xD3,
            0x3A, (byte) 0xFD, 0x4F, (byte) 0xD3, (byte) 0xEA, (byte) 0xD6, (byte) 0xF4, (byte) 0xD9};

    private static final byte[] LINEAR_MERGE_SIGNATURE = {
            (byte) 0x82, 0x7C, (byte) 0x89, (byte) 0xE4, 0x37, (byte) 0xA9, 0x73, (byte) 0x85,
            (byte) 0x8D, 0x1F, 0x63, 0x70, 0x2B, 0x2C, 0x7E, 0x25,
            0x74, (byte) 0xC5, (byte) 0x93, (byte) 0xF3, 0x53, 0x72, 0x08, 0x49,
            (byte) 0xD2, 0x46, (byte) 0xA9, 0x0A, 0x42, 0x04, 0x7D, 0x0F};

    private static final byte[] CROSS_ENCODER_RERANK_SIGNATURE = {
            0x1B, 0x4F, (byte) 0xBD, (byte) 0xB7, 0x15, 0x23, 0x23, (byte) 0xD8,
            0x10, 0x48, 0x20, (byte) 0x93, 0x21, 0x74, 0x38, 0x3E,
            0x5C, 0x0D, (byte) 0xAA, 0x62, (byte) 0xFA, 0x17, 0x5C, (byte) 0xE9,
            (byte) 0xDB, (byte) 0xAD, 0x77, (byte) 0xFD, 0x4F, (byte) 0x97, 0x16, 0x3A};

    private static final byte[] AI_TEXT_GENERATION_SIGNATURE = {
            (byte) 0x92, (byte) 0xBD, (byte) 0xD3, 0x26, (byte) 0xE7, (byte) 0x84, (byte) 0x88, (byte) 0xC3,
            0x23, (byte) 0xF1, 0x52, 0x37, 0x63, (byte) 0xA1, 0x0A, (byte) 0x9C,
            (byte) 0xA7, (byte) 0xAE, (byte) 0x85, (byte) 0xD2, (byte) 0x89, (byte) 0xC7, 0x77, 0x11,
            (byte) 0xDB, (byte) 0x83, 0x59, 0x63, (byte) 0xAB, 0x09, (byte) 0xB0, (byte) 0xC5};

    private static final byte[] AI_MULTIMODAL_GENERATION_SIGNATURE = {
            0x75, 0x78, 0x4B, 0x6E, (byte) 0xEE, 0x3F, (byte) 0xE4, 0x20,
            (byte) 0x88, 0x2E, (byte) 0xCB, 0x65, 0x15, (byte) 0x99, 0x15, 0x3C,
            0x13, (byte) 0xDD, 0x04, (byte) 0xA5, 0x14, 0x52, 0x3C, (byte) 0x8D,
            (byte) 0xC8, (byte) 0xF9, (byte) 0xC2, 0x5C, (byte) 0xBD, (byte) 0xE4, 0x0E, (byte) 0xB3};

    private static final byte[] AI_EMBEDDING_SIGNATURE = {
            (byte) 0x9D, 0x33, (byte) 0xF2, (byte) 0x84, 0x54, (byte) 0xD1, (byte) 0xD6, (byte) 0x82,
            (byte) 0xEF, 0x49, (byte) 0x89, (byte) 0xB6, (byte) 0x9C, 0x72, (byte) 0xC3, 0x57,
            0x35, (byte) 0xFA, 0x7D, (byte) 0xC2, (byte) 0x91, (byte) 0xF7, 0x57, (byte) 0xBF,
            0x07, (byte) 0x9E, (byte) 0x9C, (byte) 0xFD, 0x1B, (byte) 0xF3, (byte) 0xA6, (byte) 0xD7};

    private static final byte[] DISKANN_ARTIFACT_BUILD_SIGNATURE = {
            (byte) 0xCC, 0x34, (byte) 0xA0, (byte) 0x99, (byte) 0xDB, (byte) 0xCB, (byte) 0xF4, 0x26,
            (byte) 0xE5, 0x12, (byte) 0x8D, 0x1E, (byte) 0xFE, (byte) 0xF7, (byte) 0xE5, 0x4D,
            (byte) 0xA0, 0x67, 0x69, 0x11, (byte) 0xBB, (byte) 0xC2, 0x21, (byte) 0xDD,
            0x6A, 0x4A, 0x04, (byte) 0xF0, (byte) 0xD4, 0x55, (byte) 0xE8, (byte) 0xDE};

    private static final byte[] DISKANN_RETRIEVAL_SIGNATURE = {
            0x60, 0x3A, 0x3F, 0x4D, (byte) 0xEB, (byte) 0xD0, (byte) 0xD7, (byte) 0xD8,
            0x30, (byte) 0xBD, (byte) 0xF0, 0x4C, 0x52, (byte) 0xF7, (byte) 0x89, (byte) 0xB7,
            0x0E, 0x18, 0x23, 0x47, (byte) 0xBB, 0x7A, 0x7E, 0x52,
            (byte) 0xB2, (byte) 0xE3, 0x0D, (byte) 0xA5, 0x14, (byte) 0xF6, (byte) 0xDA, (byte) 0x82};

    private static final byte[] DISKANN_ARTIFACT_FORMAT_SIGNATURE = {
            (byte) 0xDD, 0x6E, (byte) 0xCA, 0x5D, (byte) 0x83, 0x6F, (byte) 0xFA, (byte) 0xDA,
            0x26, 0x38, 0x4E, 0x13, (byte) 0xF0, 0x5F, 0x68, 0x4A,
            0x48, (byte) 0xEE, 0x22, 0x1F, (byte) 0xED, 0x31, (byte) 0x80, 0x24,
            (byte) 0xF8, (byte) 0xD0, 0x6A, (byte) 0x91, 0x58, (byte) 0xED, 0x13, 0x2F};

    private static final byte[] HNSW_ARTIFACT_BUILD_SIGNATURE = signature(
            "fbe36417d9a1f6fd4e4da3163596b28e80bc1a761d728e259b045ee212948eab");
    private static final byte[] HNSW_RETRIEVAL_SIGNATURE = signature(
            "66a957f2b42f50a0e5665a76b4d0d876b1811418992c9e6e351f5acfc13aa8a7");
    private static final byte[] HNSW_ARTIFACT_FORMAT_SIGNATURE = signature(
            "4ff1a31a9736bd57c40736d18e1a738cd83b05a3b0373ab8689c73766f60135a");
    private static final byte[] IVF_PQ_ARTIFACT_BUILD_SIGNATURE = signature(
            "65efc307591d43d17dc7397b80945b27afdbdbe4f52790be7f8f8a9d4d5b9fc9");
    private static final byte[] IVF_PQ_RETRIEVAL_SIGNATURE = signature(
            "c611f0a6f36eef10dc54fe27d54fa6a043b9b586a2c7abf019237a468c68de90");
    private static final byte[] IVF_PQ_ARTIFACT_FORMAT_SIGNATURE = signature(
            "d841fc5b1b4f83dec9ea0b45d28858fac0cef8efe9fbcb7b26ffc67311e1872a");

    private static final byte[] BM25_ARTIFACT_BUILD_SIGNATURE = {
            (byte) 0x9A, (byte) 0xD9, 0x5B, 0x04, 0x7D, (byte) 0xF1, 0x32, 0x0D,
            (byte) 0xEA, 0x29, 0x53, 0x0B, (byte) 0xAE, 0x56, 0x64, 0x36,
            (byte) 0xB8, (byte) 0xEE, 0x0A, (byte) 0xC7, 0x2A, (byte) 0x9A, 0x78, (byte) 0xA2,
            (byte) 0xF5, (byte) 0xD1, (byte) 0x97, 0x38, (byte) 0xCC, (byte) 0xB6, 0x35, 0x56};

    private static final byte[] BM25_RETRIEVAL_SIGNATURE = {
            (byte) 0xD7, (byte) 0xF4, 0x53, (byte) 0xCA, (byte) 0xB4, 0x6B, (byte) 0xCA, 0x0C,
            (byte) 0xCA, 0x2E, (byte) 0xBF, (byte) 0x86, (byte) 0xFB, (byte) 0xE7, 0x55, (byte) 0x98,
            (byte) 0xD7, 0x09, (byte) 0xAC, (byte) 0xFA, 0x56, 0x52, (byte) 0x9D, (byte) 0xE9,
            0x44, 0x5A, (byte) 0x9A, 0x36, 0x2F, 0x11, 0x33, 0x4C};

    private static final byte[] BM25_ARTIFACT_FORMAT_SIGNATURE = {
            (byte) 0x94, (byte) 0xEB, 0x1C, 0x3A, 0x58, (byte) 0xB2, (byte) 0xD3, (byte) 0xEF,
            (byte) 0x94, 0x7A, 0x3F, (byte) 0xBE, 0x79, 0x49, 0x1A, (byte) 0xE2,
            0x7F, 0x77, 0x28, (byte) 0xAE, (byte) 0xBF, 0x38, 0x45, (byte) 0xFE,
            (byte) 0xBB, 0x30, 0x1A, 0x0B, 0x16, (byte) 0x81, 0x11, 0x06};

    /*
     * ABI: SOURCE_MAP(row_position BIGINT, nullable generated_value VECTOR(FLOAT)) ->
     * GENERATED_VALUES[0] segment v2, ROW_LOCATORS[1], report schema v2. NULL
     * values retain their locator; the segment carries a canonical validity bitmap.
     * This is a
     * generic generated-value materializer; model invocation remains a
     * separate OperatorBinding attached to the Project expression.
     */
    private static final byte[] GENERATED_VALUES_ARTIFACT_BUILD_SIGNATURE = signature(
            "8b22e62d0a4a228c69b8edc3c9e8920481bd6b83c54d168aeb6e62b0fabcbd5f");
    private static final byte[] GENERATED_VALUES_ARTIFACT_FORMAT_SIGNATURE = signature(
            "396997d51c3a824e49bd93b12cad2711a114b94ff8a52ab957829f4aab081698");
    private static final byte[] GENERATED_VALUES_OVERLAY_SIGNATURE = signature(
            "0233b81f55d5f548c0d483b4da2bc8c7557ae742c16df19ef3ba73684a198ff5");
    private static final byte[] TABULAR_EXPORT_SIGNATURE = signature(
            "214efe8e4aab98cd87519a901e8407f3bf4eb64bf8527a86458dd5556002a262");
    private static final byte[] TABULAR_EXPORT_FORMAT_SIGNATURE = signature(
            "8ef0b6b1e7d674a7585107767a655236f2b542012c798fcf9c7a5257775ac2f4");

    private static final byte[] PHYSICAL_ROW_LOCATOR_SIGNATURE = {
            (byte) 0xCD, 0x02, (byte) 0x82, (byte) 0xE4, 0x13, 0x15, (byte) 0x9F, (byte) 0xD4,
            0x13, (byte) 0xEC, (byte) 0xE1, 0x66, 0x3D, (byte) 0xB7, 0x77, (byte) 0xD3,
            (byte) 0xC1, 0x7E, 0x2B, (byte) 0xDE, (byte) 0xE4, (byte) 0xB0, 0x6D, (byte) 0xCC,
            (byte) 0xD2, (byte) 0xAF, (byte) 0xA0, (byte) 0xCC, (byte) 0xBF, 0x4F, 0x22, 0x48};

    private BuiltInCapabilityCatalog() {}

    /** Dynamic UNNEST output is declared by the typed relation, not a scalar signature. */
    public static CapabilityRef unnestPlanRef() {
        return org.pangrova.protocol.function.declaration.FunctionBindingDeclarations
                .structuralReference("pangrova.table.unnest");
    }

    /** Returns the build-independent semantic reference embedded in a query plan. */
    public static CapabilityRef exactTopKPlanRef() {
        return CapabilityRef.newBuilder()
                .setCapabilityId(EXACT_TOP_K_ID)
                .setSemanticVersion(EXACT_TOP_K_VERSION)
                .setSignatureSha256(ByteString.copyFrom(EXACT_TOP_K_SIGNATURE))
                .build();
    }

    /** Returns the exact Native implementation reference advertised by a Worker. */
    public static CapabilityRef exactTopKNativeRef() {
        return exactTopKPlanRef().toBuilder()
                .setImplementationBuildId(NATIVE_BUILD_ID)
                .build();
    }

    /** Native RRF merge plan reference. */
    public static CapabilityRef rrfMergePlanRef() {
        return planRef(RRF_MERGE_ID, RRF_MERGE_SIGNATURE);
    }

    /** Native weighted-linear merge plan reference. */
    public static CapabilityRef linearMergePlanRef() {
        return planRef(LINEAR_MERGE_ID, LINEAR_MERGE_SIGNATURE);
    }

    /** Native bounded rerank relation backed by a governed Worker callback. */
    public static CapabilityRef crossEncoderRerankPlanRef() {
        return CapabilityRef.newBuilder()
                .setCapabilityId(CROSS_ENCODER_RERANK_ID)
                .setSemanticVersion(CROSS_ENCODER_RERANK_VERSION)
                .setSignatureSha256(ByteString.copyFrom(
                        CROSS_ENCODER_RERANK_SIGNATURE))
                .build();
    }

    /** Worker-governed text-generation expression invocation. */
    public static CapabilityRef aiTextGenerationPlanRef() {
        return aiInvocationRef(AI_TEXT_GENERATION_ID, AI_TEXT_GENERATION_SIGNATURE);
    }

    /** Worker-governed multimodal-generation expression invocation. */
    public static CapabilityRef aiMultimodalGenerationPlanRef() {
        return aiInvocationRef(
                AI_MULTIMODAL_GENERATION_ID,
                AI_MULTIMODAL_GENERATION_SIGNATURE);
    }

    /** Worker-governed embedding expression invocation. */
    public static CapabilityRef aiEmbeddingPlanRef() {
        return aiInvocationRef(AI_EMBEDDING_ID, AI_EMBEDDING_SIGNATURE);
    }

    /** Native DiskANN immutable work-page build reference. */
    public static CapabilityRef diskAnnArtifactBuildPlanRef() {
        return CapabilityRef.newBuilder()
                .setCapabilityId(DISKANN_ARTIFACT_BUILD_ID)
                .setSemanticVersion(DISKANN_ARTIFACT_BUILD_VERSION)
                .setSignatureSha256(ByteString.copyFrom(
                        DISKANN_ARTIFACT_BUILD_SIGNATURE))
                .build();
    }

    /** Native DiskANN retrieval over an immutable generic artifact binding. */
    public static CapabilityRef diskAnnRetrievalPlanRef() {
        return CapabilityRef.newBuilder()
                .setCapabilityId(DISKANN_RETRIEVAL_ID)
                .setSemanticVersion(DISKANN_RETRIEVAL_VERSION)
                .setSignatureSha256(ByteString.copyFrom(DISKANN_RETRIEVAL_SIGNATURE))
                .build();
    }

    /** Native predicate-first retrieval in one admitted task with physical row identity. */
    public static CapabilityRef filteredDiskAnnRetrievalPlanRef() {
        return capabilityRef(FILTERED_DISKANN_RETRIEVAL_ID,
                FILTERED_DISKANN_RETRIEVAL_VERSION, FILTERED_DISKANN_RETRIEVAL_SIGNATURE);
    }

    /** Immutable DiskANN segment format with a position-aligned locator sidecar. */
    public static ArtifactFormatRef diskAnnArtifactFormat() {
        return ArtifactFormatRef.newBuilder()
                .setFormatId(DISKANN_ARTIFACT_FORMAT_ID)
                .setMajorVersion(DISKANN_ARTIFACT_FORMAT_MAJOR)
                .setMinorVersion(0)
                .setSchemaSha256(ByteString.copyFrom(DISKANN_ARTIFACT_FORMAT_SIGNATURE))
                .setWriterBuildId(NATIVE_BUILD_ID)
                .build();
    }

    /** Native HNSW immutable work-page build reference. */
    public static CapabilityRef hnswArtifactBuildPlanRef() {
        return capabilityRef(
                HNSW_ARTIFACT_BUILD_ID, HNSW_ARTIFACT_BUILD_VERSION,
                HNSW_ARTIFACT_BUILD_SIGNATURE);
    }

    /** Native HNSW retrieval reference. */
    public static CapabilityRef hnswRetrievalPlanRef() {
        return capabilityRef(
                HNSW_RETRIEVAL_ID, HNSW_RETRIEVAL_VERSION,
                HNSW_RETRIEVAL_SIGNATURE);
    }

    /** Immutable source-object-scoped HNSW graph format. */
    public static ArtifactFormatRef hnswArtifactFormat() {
        return artifactFormat(
                HNSW_ARTIFACT_FORMAT_ID, HNSW_ARTIFACT_FORMAT_MAJOR,
                HNSW_ARTIFACT_FORMAT_SIGNATURE);
    }

    /** Native IVF-PQ immutable work-page build reference. */
    public static CapabilityRef ivfPqArtifactBuildPlanRef() {
        return capabilityRef(
                IVF_PQ_ARTIFACT_BUILD_ID, IVF_PQ_ARTIFACT_BUILD_VERSION,
                IVF_PQ_ARTIFACT_BUILD_SIGNATURE);
    }

    /** Native IVF-PQ retrieval reference. */
    public static CapabilityRef ivfPqRetrievalPlanRef() {
        return capabilityRef(
                IVF_PQ_RETRIEVAL_ID, IVF_PQ_RETRIEVAL_VERSION,
                IVF_PQ_RETRIEVAL_SIGNATURE);
    }

    /** Immutable source-object-scoped IVF-PQ format. */
    public static ArtifactFormatRef ivfPqArtifactFormat() {
        return artifactFormat(
                IVF_PQ_ARTIFACT_FORMAT_ID, IVF_PQ_ARTIFACT_FORMAT_MAJOR,
                IVF_PQ_ARTIFACT_FORMAT_SIGNATURE);
    }

    /** Resolves an index metadata algorithm name to its canonical Native build capability. */
    public static CapabilityRef vectorArtifactBuildPlanRef(String algorithm) {
        return switch (normalizeVectorAlgorithm(algorithm)) {
            case "HNSW" -> hnswArtifactBuildPlanRef();
            case "IVF_PQ" -> ivfPqArtifactBuildPlanRef();
            case "DISK_ANN" -> diskAnnArtifactBuildPlanRef();
            default -> throw new IllegalArgumentException(
                    "Unsupported Native vector index algorithm: " + algorithm);
        };
    }

    /** Resolves an index metadata algorithm name to its canonical Native retrieval capability. */
    public static CapabilityRef vectorRetrievalPlanRef(String algorithm) {
        return switch (normalizeVectorAlgorithm(algorithm)) {
            case "HNSW" -> hnswRetrievalPlanRef();
            case "IVF_PQ" -> ivfPqRetrievalPlanRef();
            case "DISK_ANN" -> diskAnnRetrievalPlanRef();
            default -> throw new IllegalArgumentException(
                    "Unsupported Native vector index algorithm: " + algorithm);
        };
    }

    /** Resolves an index metadata algorithm name to its immutable Native artifact format. */
    public static ArtifactFormatRef vectorArtifactFormat(String algorithm) {
        return switch (normalizeVectorAlgorithm(algorithm)) {
            case "HNSW" -> hnswArtifactFormat();
            case "IVF_PQ" -> ivfPqArtifactFormat();
            case "DISK_ANN" -> diskAnnArtifactFormat();
            default -> throw new IllegalArgumentException(
                    "Unsupported Native vector index algorithm: " + algorithm);
        };
    }

    /** Native immutable BM25 work-page build reference. */
    public static CapabilityRef bm25ArtifactBuildPlanRef() {
        return CapabilityRef.newBuilder()
                .setCapabilityId(BM25_ARTIFACT_BUILD_ID)
                .setSemanticVersion(BM25_ARTIFACT_BUILD_VERSION)
                .setSignatureSha256(ByteString.copyFrom(BM25_ARTIFACT_BUILD_SIGNATURE))
                .build();
    }

    /** Native BM25 retrieval over an immutable generic artifact binding. */
    public static CapabilityRef bm25RetrievalPlanRef() {
        return CapabilityRef.newBuilder()
                .setCapabilityId(BM25_RETRIEVAL_ID)
                .setSemanticVersion(BM25_RETRIEVAL_VERSION)
                .setSignatureSha256(ByteString.copyFrom(BM25_RETRIEVAL_SIGNATURE))
                .build();
    }

    /** Immutable source-object-scoped BM25 postings segment format. */
    public static ArtifactFormatRef bm25ArtifactFormat() {
        return ArtifactFormatRef.newBuilder()
                .setFormatId(BM25_ARTIFACT_FORMAT_ID)
                .setMajorVersion(BM25_ARTIFACT_FORMAT_MAJOR)
                .setMinorVersion(0)
                .setSchemaSha256(ByteString.copyFrom(BM25_ARTIFACT_FORMAT_SIGNATURE))
                .setWriterBuildId(NATIVE_BUILD_ID)
                .build();
    }

    /** Native source-map sink for generic generated values and their row locators. */
    public static CapabilityRef generatedValuesArtifactBuildPlanRef() {
        return capabilityRef(
                GENERATED_VALUES_ARTIFACT_BUILD_ID,
                GENERATED_VALUES_ARTIFACT_BUILD_VERSION,
                GENERATED_VALUES_ARTIFACT_BUILD_SIGNATURE);
    }

    /** Immutable source-object-scoped generated-value sidecar format. */
    public static ArtifactFormatRef generatedValuesArtifactFormat() {
        return artifactFormat(
                GENERATED_VALUES_ARTIFACT_FORMAT_ID,
                GENERATED_VALUES_ARTIFACT_FORMAT_MAJOR,
                GENERATED_VALUES_ARTIFACT_FORMAT_SIGNATURE);
    }

    /** Native scan overlay joining generated values by the physical row locator. */
    public static CapabilityRef generatedValuesOverlayPlanRef() {
        return capabilityRef(
                GENERATED_VALUES_OVERLAY_ID,
                GENERATED_VALUES_OVERLAY_VERSION,
                GENERATED_VALUES_OVERLAY_SIGNATURE);
    }

    /** Native streaming export sink for an arbitrary tabular relation. */
    public static CapabilityRef tabularExportPlanRef() {
        return capabilityRef(
                TABULAR_EXPORT_ID, TABULAR_EXPORT_VERSION,
                TABULAR_EXPORT_SIGNATURE);
    }

    /** Immutable Arrow IPC stream emitted by one source work page. */
    public static ArtifactFormatRef tabularExportFormat() {
        return artifactFormat(
                TABULAR_EXPORT_FORMAT_ID, TABULAR_EXPORT_FORMAT_MAJOR,
                TABULAR_EXPORT_FORMAT_SIGNATURE);
    }

    /** Physical row locator contract for one immutable source object. */
    public static RowLocatorContract physicalRowLocatorContract() {
        return RowLocatorContract.newBuilder()
                .setSchemaVersion(1)
                .setKind(RowLocatorKind.ROW_LOCATOR_KIND_PHYSICAL_ROW_POSITION)
                .setSchemaSha256(ByteString.copyFrom(PHYSICAL_ROW_LOCATOR_SIGNATURE))
                .setSourceObjectScoped(true)
                .build();
    }

    private static CapabilityRef planRef(String id, byte[] signature) {
        return CapabilityRef.newBuilder()
                .setCapabilityId(id)
                .setSemanticVersion(RETRIEVAL_MERGE_VERSION)
                .setSignatureSha256(ByteString.copyFrom(signature))
                .build();
    }

    private static CapabilityRef capabilityRef(
            String id, String version, byte[] signature) {
        return CapabilityRef.newBuilder()
                .setCapabilityId(id)
                .setSemanticVersion(version)
                .setSignatureSha256(ByteString.copyFrom(signature))
                .build();
    }

    private static ArtifactFormatRef artifactFormat(
            String id, int major, byte[] signature) {
        return ArtifactFormatRef.newBuilder()
                .setFormatId(id)
                .setMajorVersion(major)
                .setMinorVersion(0)
                .setSchemaSha256(ByteString.copyFrom(signature))
                .setWriterBuildId(NATIVE_BUILD_ID)
                .build();
    }

    private static byte[] signature(String hexadecimal) {
        return HexFormat.of().parseHex(hexadecimal);
    }

    private static String normalizeVectorAlgorithm(String algorithm) {
        if (algorithm == null || algorithm.isBlank()) {
            throw new IllegalArgumentException("vector index algorithm must not be blank");
        }
        return algorithm.trim().toUpperCase();
    }

    private static CapabilityRef aiInvocationRef(String id, byte[] signature) {
        return CapabilityRef.newBuilder()
                .setCapabilityId(id)
                .setSemanticVersion(AI_INVOCATION_VERSION)
                .setSignatureSha256(ByteString.copyFrom(signature))
                .build();
    }
}
