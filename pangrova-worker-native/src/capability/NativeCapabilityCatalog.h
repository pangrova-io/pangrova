#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "pangrova/protocol/v2/capability.pb.h"

namespace pangrova::capability {

inline constexpr std::string_view kExactTopKRetrievalId =
    "pangrova.retrieval.topk.exact";
inline constexpr std::string_view kExactTopKRetrievalVersion = "1.0.0";
inline constexpr std::string_view kExactTopKRetrievalBuildId =
    "pangrova-native-0.1.0";
inline constexpr std::string_view kRrfRetrievalMergeId =
    "pangrova.retrieval.merge.rrf";
inline constexpr std::string_view kLinearRetrievalMergeId =
    "pangrova.retrieval.merge.linear";
inline constexpr std::string_view kRetrievalMergeVersion = "1.0.0";
inline constexpr std::string_view kRetrievalMergeBuildId =
    "pangrova-native-0.1.0";
inline constexpr std::string_view kRelationDetailEnvelopeTypeUrl =
    "type.googleapis.com/pangrova.protocol.v2.RelationDetailEnvelope";
inline constexpr uint32_t kRetrievalCandidateSchemaVersion = 1;
inline constexpr uint64_t kExactTopKMaxCandidateRows = 100'000;
inline constexpr uint64_t kRetrievalMergeMaxCandidateRows = 1'000'000;
inline constexpr uint64_t kRetrievalMergeMaxCandidateBytes =
    256ULL * 1024 * 1024;
inline constexpr uint32_t kRetrievalMergeMaxFanIn = 64;
inline constexpr std::string_view kCrossEncoderRerankId =
    "pangrova.retrieval.rerank.cross_encoder";
inline constexpr std::string_view kCrossEncoderRerankVersion = "1.0.0";
inline constexpr std::string_view kGovernedRerankFunction =
    "pangrova_ai_rerank_score";
inline constexpr uint64_t kCrossEncoderRerankMaxRows = 100'000;
inline constexpr std::string_view kAiTextGenerationId =
    "pangrova.ai.invoke.text_generation";
inline constexpr std::string_view kAiMultimodalGenerationId =
    "pangrova.ai.invoke.multimodal_generation";
inline constexpr std::string_view kAiEmbeddingId =
    "pangrova.ai.invoke.embedding";
inline constexpr std::string_view kAiInvocationVersion = "1.0.0";
inline constexpr std::string_view kAiInvocationBuildId =
    "pangrova-native-0.1.0";
inline constexpr std::string_view kDiskAnnArtifactBuildId =
    "pangrova.artifact.build.diskann";
inline constexpr std::string_view kDiskAnnArtifactBuildVersion = "2.0.0";
inline constexpr std::string_view kDiskAnnArtifactFormatId =
    "pangrova.diskann.segment";
inline constexpr uint32_t kDiskAnnArtifactFormatMajor = 2;
inline constexpr uint64_t kDiskAnnArtifactMaxRows = 1'000'000;
inline constexpr std::string_view kDiskAnnRetrievalId =
    "pangrova.retrieval.vector.diskann";
inline constexpr std::string_view kDiskAnnRetrievalVersion = "1.0.0";
inline constexpr std::string_view kHnswArtifactBuildId =
    "pangrova.artifact.build.hnsw";
inline constexpr std::string_view kHnswArtifactBuildVersion = "2.0.0";
inline constexpr std::string_view kHnswArtifactFormatId =
    "pangrova.hnsw.segment";
inline constexpr uint32_t kHnswArtifactFormatMajor = 2;
inline constexpr uint64_t kHnswArtifactMaxRows = 1'000'000;
inline constexpr std::string_view kHnswRetrievalId =
    "pangrova.retrieval.vector.hnsw";
inline constexpr std::string_view kHnswRetrievalVersion = "2.0.0";
inline constexpr std::string_view kIvfPqArtifactBuildId =
    "pangrova.artifact.build.ivf_pq";
inline constexpr std::string_view kIvfPqArtifactBuildVersion = "2.0.0";
inline constexpr std::string_view kIvfPqArtifactFormatId =
    "pangrova.ivfpq.segment";
inline constexpr uint32_t kIvfPqArtifactFormatMajor = 2;
inline constexpr uint64_t kIvfPqArtifactMaxRows = 1'000'000;
inline constexpr std::string_view kIvfPqRetrievalId =
    "pangrova.retrieval.vector.ivf_pq";
inline constexpr std::string_view kIvfPqRetrievalVersion = "2.0.0";
inline constexpr std::string_view kBm25ArtifactBuildId =
    "pangrova.artifact.build.bm25";
inline constexpr std::string_view kBm25ArtifactBuildVersion = "1.0.0";
inline constexpr std::string_view kBm25ArtifactFormatId =
    "pangrova.bm25.segment";
inline constexpr uint32_t kBm25ArtifactFormatMajor = 1;
inline constexpr uint64_t kBm25ArtifactMaxRows = 1'000'000;
inline constexpr std::string_view kBm25RetrievalId =
    "pangrova.retrieval.text.bm25";
inline constexpr std::string_view kBm25RetrievalVersion = "1.0.0";
inline constexpr std::string_view kGeneratedValuesArtifactBuildId =
    "pangrova.artifact.build.generated_values";
inline constexpr std::string_view kGeneratedValuesArtifactBuildVersion =
    "2.0.0";
inline constexpr std::string_view kGeneratedValuesArtifactFormatId =
    "pangrova.generated-values.segment";
inline constexpr uint32_t kGeneratedValuesArtifactFormatMajor = 2;
inline constexpr uint64_t kGeneratedValuesArtifactMaxRows = 1'000'000;
inline constexpr std::string_view kGeneratedValuesOverlayId =
    "pangrova.generated-values.overlay";
inline constexpr std::string_view kGeneratedValuesOverlayVersion = "2.0.0";
inline constexpr std::string_view kTabularExportId =
    "pangrova.artifact.export.tabular";
inline constexpr std::string_view kTabularExportVersion = "1.0.0";
inline constexpr std::string_view kTabularExportFormatId =
    "pangrova.tabular-export.arrow-stream";
inline constexpr uint32_t kTabularExportFormatMajor = 1;
inline constexpr uint64_t kTabularExportMaxRows = 100'000'000;

std::string exactTopKRetrievalSignature();

bool isExactTopKRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability);

pangrova::protocol::v2::CapabilityDescriptor
exactTopKRetrievalDescriptor();

std::string rrfRetrievalMergeSignature();
std::string linearRetrievalMergeSignature();

bool isRrfRetrievalMerge(
    const pangrova::protocol::v2::CapabilityRef& capability);
bool isLinearRetrievalMerge(
    const pangrova::protocol::v2::CapabilityRef& capability);

pangrova::protocol::v2::CapabilityDescriptor rrfRetrievalMergeDescriptor();
pangrova::protocol::v2::CapabilityDescriptor linearRetrievalMergeDescriptor();

std::string crossEncoderRerankSignature();
bool isCrossEncoderRerank(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor crossEncoderRerankDescriptor();

std::string aiTextGenerationSignature();
std::string aiMultimodalGenerationSignature();
std::string aiEmbeddingSignature();
bool isAiTextGeneration(
    const pangrova::protocol::v2::CapabilityRef& capability);
bool isAiMultimodalGeneration(
    const pangrova::protocol::v2::CapabilityRef& capability);
bool isAiEmbedding(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor aiTextGenerationDescriptor();
pangrova::protocol::v2::CapabilityDescriptor
aiMultimodalGenerationDescriptor();
pangrova::protocol::v2::CapabilityDescriptor aiEmbeddingDescriptor();

std::string diskAnnArtifactBuildSignature();
std::string diskAnnArtifactFormatSignature();
std::string physicalRowLocatorSignature();
bool isDiskAnnArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::ArtifactFormatRef diskAnnArtifactFormat();
pangrova::protocol::v2::RowLocatorContract physicalRowLocatorContract();
pangrova::protocol::v2::CapabilityDescriptor diskAnnArtifactBuildDescriptor();

std::string diskAnnRetrievalSignature();
bool isDiskAnnRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor diskAnnRetrievalDescriptor();

std::string hnswArtifactBuildSignature();
std::string hnswArtifactFormatSignature();
bool isHnswArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::ArtifactFormatRef hnswArtifactFormat();
pangrova::protocol::v2::CapabilityDescriptor hnswArtifactBuildDescriptor();
std::string hnswRetrievalSignature();
bool isHnswRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor hnswRetrievalDescriptor();

std::string ivfPqArtifactBuildSignature();
std::string ivfPqArtifactFormatSignature();
bool isIvfPqArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::ArtifactFormatRef ivfPqArtifactFormat();
pangrova::protocol::v2::CapabilityDescriptor ivfPqArtifactBuildDescriptor();
std::string ivfPqRetrievalSignature();
bool isIvfPqRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor ivfPqRetrievalDescriptor();

std::string bm25ArtifactBuildSignature();
std::string bm25ArtifactFormatSignature();
bool isBm25ArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::ArtifactFormatRef bm25ArtifactFormat();
pangrova::protocol::v2::CapabilityDescriptor bm25ArtifactBuildDescriptor();

std::string bm25RetrievalSignature();
bool isBm25Retrieval(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor bm25RetrievalDescriptor();

std::string generatedValuesArtifactBuildSignature();
std::string generatedValuesArtifactFormatSignature();
bool isGeneratedValuesArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::ArtifactFormatRef generatedValuesArtifactFormat();
pangrova::protocol::v2::CapabilityDescriptor
generatedValuesArtifactBuildDescriptor();

std::string generatedValuesOverlaySignature();
bool isGeneratedValuesOverlay(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::CapabilityDescriptor generatedValuesOverlayDescriptor();

std::string tabularExportSignature();
std::string tabularExportFormatSignature();
bool isTabularExport(
    const pangrova::protocol::v2::CapabilityRef& capability);
pangrova::protocol::v2::ArtifactFormatRef tabularExportFormat();
pangrova::protocol::v2::CapabilityDescriptor tabularExportDescriptor();

}  // namespace pangrova::capability
