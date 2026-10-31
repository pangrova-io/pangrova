// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "capabilities/artifact/ArtifactRetrievalPlanner.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <unordered_set>

#include <google/protobuf/descriptor.h>

#include "capabilities/artifact/RowLocatorSidecar.h"
#include "capabilities/artifact/ArtifactVectorSearch.h"
#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "indexing/text/bm25/Bm25Analyzer.h"
#include "indexing/text/bm25/Bm25CorpusStatsCache.h"
#include "indexing/text/bm25/Bm25SegmentCache.h"
#include "vector/VectorDistance.h"
#include "vector/VectorTopKHeap.h"
#include "vector/hnsw/HnswIndex.h"
#include "vector/ivfpq/IvfPqIndex.h"

namespace pangrova::artifact {
namespace {

namespace proto = pangrova::protocol::v2;

void collectRetrievalDetails(
    const substrait::Rel& relation,
    std::vector<proto::RetrievalSourceDetail>& output) {
  if (relation.has_extension_single()) {
    proto::RelationDetailEnvelope envelope;
    const auto& extension = relation.extension_single();
    if (extension.has_detail() && extension.detail().UnpackTo(&envelope) &&
        envelope.schema_version() == 1 &&
        envelope.has_retrieval_source() &&
        (pangrova::capability::isExactTopKRetrieval(
             envelope.retrieval_source().invocation().capability()) ||
         pangrova::capability::isFilteredDiskAnnRetrieval(
             envelope.retrieval_source().invocation().capability()) ||
         pangrova::capability::isDiskAnnRetrieval(
             envelope.retrieval_source().invocation().capability()) ||
         pangrova::capability::isHnswRetrieval(
             envelope.retrieval_source().invocation().capability()) ||
         pangrova::capability::isIvfPqRetrieval(
             envelope.retrieval_source().invocation().capability()) ||
         pangrova::capability::isBm25Retrieval(
             envelope.retrieval_source().invocation().capability()))) {
      output.push_back(envelope.retrieval_source());
    }
  }

  const auto* reflection = relation.GetReflection();
  const auto* descriptor = relation.GetDescriptor();
  for (int field_index = 0; field_index < descriptor->field_count(); ++field_index) {
    const auto* field = descriptor->field(field_index);
    if (field->cpp_type() !=
        google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      continue;
    }
    if (field->is_repeated()) {
      const int count = reflection->FieldSize(relation, field);
      for (int index = 0; index < count; ++index) {
        const auto& nested = reflection->GetRepeatedMessage(
            relation, field, index);
        if (nested.GetDescriptor()->full_name() == "substrait.Rel") {
          collectRetrievalDetails(
              static_cast<const substrait::Rel&>(nested), output);
        }
      }
    } else if (reflection->HasField(relation, field)) {
      const auto& nested = reflection->GetMessage(relation, field);
      if (nested.GetDescriptor()->full_name() == "substrait.Rel") {
        collectRetrievalDetails(
            static_cast<const substrait::Rel&>(nested), output);
      } else {
        // A Rel owns an operator message (e.g. FilterRel), which in turn owns
        // its input Rel. Walk that one level using reflection without treating
        // arbitrary protobuf messages as wire contracts.
        const auto* nested_descriptor = nested.GetDescriptor();
        const auto* nested_reflection = nested.GetReflection();
        for (int child_index = 0;
             child_index < nested_descriptor->field_count();
             ++child_index) {
          const auto* child = nested_descriptor->field(child_index);
          if (child->cpp_type() !=
                  google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE ||
              child->message_type()->full_name() != "substrait.Rel") {
            continue;
          }
          if (child->is_repeated()) {
            const int count = nested_reflection->FieldSize(nested, child);
            for (int index = 0; index < count; ++index) {
              collectRetrievalDetails(
                  static_cast<const substrait::Rel&>(
                      nested_reflection->GetRepeatedMessage(
                          nested, child, index)),
                  output);
            }
          } else if (nested_reflection->HasField(nested, child)) {
            collectRetrievalDetails(
                static_cast<const substrait::Rel&>(
                    nested_reflection->GetMessage(nested, child)),
                output);
          }
        }
      }
    }
  }
}

const proto::NamedValue& requireOption(
    const proto::CapabilityInvocation& invocation,
    const std::string& name) {
  const proto::NamedValue* result = nullptr;
  for (const auto& option : invocation.options()) {
    if (option.name() == name) {
      if (result != nullptr) {
        throw std::runtime_error("Duplicate retrieval option: " + name);
      }
      result = &option;
    }
  }
  if (result == nullptr) {
    throw std::runtime_error("Missing retrieval option: " + name);
  }
  return *result;
}

std::vector<float> parseQueryVector(const proto::CapabilityInvocation& invocation) {
  return ArtifactVectorSearch::boundQuery(invocation);
}

int64_t requireSearchListSize(
    const proto::CapabilityInvocation& invocation,
    uint64_t exact_rescore_limit) {
  const auto& metric = requireOption(invocation, "metric").value();
  if (!metric.has_string_value() || metric.string_value() != "L2") {
    throw std::runtime_error("Native DiskANN currently requires metric=L2");
  }
  const auto& search_list =
      requireOption(invocation, "search_list_size").value();
  if (!search_list.has_int64_value() || search_list.int64_value() <= 0 ||
      search_list.int64_value() > 100'000 ||
      static_cast<uint64_t>(search_list.int64_value()) < exact_rescore_limit) {
    throw std::runtime_error("DiskANN search_list_size is outside safe bounds");
  }
  return search_list.int64_value();
}

int64_t requireAnnSearchParameter(
    const proto::CapabilityInvocation& invocation,
    const std::string& name,
    int64_t minimum,
    int64_t maximum) {
  const auto& metric = requireOption(invocation, "metric").value();
  const auto& parameter = requireOption(invocation, name).value();
  if (!metric.has_string_value() ||
      !parameter.has_int64_value() || parameter.int64_value() < minimum ||
      parameter.int64_value() > maximum) {
    throw std::runtime_error("ANN retrieval options are outside safe bounds");
  }
  vector::distanceMetricFromName(metric.string_value());
  return parameter.int64_value();
}

vector::DistanceMetric requireArtifactMetric(
    const proto::ArtifactBinding& artifact,
    const proto::CapabilityInvocation& invocation) {
  const auto& requested = requireOption(invocation, "metric").value();
  if (!requested.has_string_value()) {
    throw std::runtime_error("ANN metric option must be VARCHAR");
  }
  const auto metric = vector::distanceMetricFromName(requested.string_value());
  bool found = false;
  for (const auto& property : artifact.properties()) {
    if (property.name() == "metric") {
      if (found || !property.value().has_string_value() ||
          property.value().string_value() != requested.string_value()) {
        throw std::runtime_error("ANN query metric differs from the immutable artifact");
      }
      found = true;
    }
  }
  if (!found) {
    throw std::runtime_error("ANN artifact has no immutable metric property");
  }
  return metric;
}

int64_t requireDimension(
    const proto::ArtifactBinding& artifact,
    size_t query_dimension) {
  int64_t dimension = 0;
  for (const auto& property : artifact.properties()) {
    if (property.name() == "dimension") {
      if (dimension != 0 || !property.value().has_int64_value()) {
        throw std::runtime_error("ANN dimension property is invalid");
      }
      dimension = property.value().int64_value();
    }
  }
  if (dimension <= 0 || dimension > 65'536 ||
      static_cast<size_t>(dimension) != query_dimension) {
    throw std::runtime_error(
        "ANN query dimension differs from the immutable artifact");
  }
  return dimension;
}

struct Bundle {
  const proto::ArtifactObject* graph{nullptr};
  const proto::ArtifactObject* vectors{nullptr};
  const proto::ArtifactObject* locators{nullptr};
};

struct Bm25Bundle {
  const proto::ArtifactObject* postings{nullptr};
  const proto::ArtifactObject* locators{nullptr};
};

struct Bm25Artifacts {
  std::unordered_map<std::string, Bm25Bundle> bundles;
  const proto::ArtifactObject* corpus_stats{nullptr};
};

struct Bm25Query {
  std::vector<std::string> tokens;
  double k1{0};
  double b{0};
};

Bm25Query parseBm25Query(
    const proto::CapabilityInvocation& invocation) {
  const auto& query = requireOption(invocation, "query_text").value();
  const auto& analyzer = requireOption(invocation, "analyzer").value();
  const auto& k1 = requireOption(invocation, "k1").value();
  const auto& b = requireOption(invocation, "b").value();
  if (!query.has_string_value() || query.string_value().empty() ||
      !analyzer.has_string_value() || analyzer.string_value() != "standard-v1" ||
      !k1.has_double_value() || !std::isfinite(k1.double_value()) ||
      k1.double_value() <= 0 || !b.has_double_value() ||
      !std::isfinite(b.double_value()) || b.double_value() < 0 ||
      b.double_value() > 1) {
    throw std::runtime_error("BM25 retrieval options violate the capability contract");
  }
  auto tokens = text::bm25::Bm25Analyzer::tokenize(query.string_value());
  if (tokens.empty()) {
    throw std::runtime_error("BM25 query contains no searchable tokens");
  }
  return {std::move(tokens), k1.double_value(), b.double_value()};
}

void requireBm25ArtifactProperties(const proto::ArtifactBinding& artifact) {
  bool analyzer_seen = false;
  for (const auto& property : artifact.properties()) {
    if (property.name() == "analyzer") {
      if (analyzer_seen || !property.value().has_string_value() ||
          property.value().string_value() != "standard-v1") {
        throw std::runtime_error("BM25 artifact analyzer property is invalid");
      }
      analyzer_seen = true;
    }
  }
  if (!analyzer_seen) {
    throw std::runtime_error("BM25 artifact has no analyzer identity");
  }
}

std::unordered_map<std::string, Bundle> buildVectorBundles(
    const proto::ArtifactBinding& artifact,
    const std::string& index_role) {
  std::unordered_map<std::string, Bundle> bundles;
  for (const auto& object : artifact.split_objects()) {
    if (object.scope() != proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT) {
      throw std::runtime_error("ANN artifact contains a non-source object");
    }
    auto& bundle = bundles[object.source_object_id()];
    const proto::ArtifactObject** target = nullptr;
    if (object.object_role() == index_role && object.ordinal() == 0) {
      target = &bundle.graph;
    } else if (object.object_role() == "RAW_VECTORS" && object.ordinal() == 1) {
      target = &bundle.vectors;
    } else if (object.object_role() == "ROW_LOCATORS" && object.ordinal() == 2) {
      target = &bundle.locators;
    } else {
      throw std::runtime_error("ANN artifact contains an unknown object role");
    }
    if (*target != nullptr) {
      throw std::runtime_error("ANN artifact contains a duplicate object role");
    }
    *target = &object;
  }
  for (const auto& [source, bundle] : bundles) {
    if (source.empty() || bundle.graph == nullptr || bundle.vectors == nullptr ||
        bundle.locators == nullptr ||
        bundle.graph->row_count() != bundle.vectors->row_count() ||
        bundle.graph->row_count() != bundle.locators->row_count()) {
      throw std::runtime_error("ANN artifact source bundle is incomplete");
    }
  }
  return bundles;
}

Bm25Artifacts buildBm25Artifacts(
    const proto::ArtifactBinding& artifact) {
  Bm25Artifacts result;
  for (const auto& object : artifact.split_objects()) {
    if (object.scope() == proto::ARTIFACT_OBJECT_SCOPE_GENERATION &&
        object.object_role() == "BM25_CORPUS_STATS" && object.ordinal() == 0) {
      if (result.corpus_stats != nullptr ||
          object.source_object_id() != artifact.generation()) {
        throw std::runtime_error(
            "BM25 artifact contains duplicate or mismatched corpus statistics");
      }
      result.corpus_stats = &object;
      continue;
    }
    if (object.scope() != proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT) {
      throw std::runtime_error("BM25 artifact contains an unknown object scope");
    }
    auto& bundle = result.bundles[object.source_object_id()];
    const proto::ArtifactObject** target = nullptr;
    if (object.object_role() == "BM25_POSTINGS" && object.ordinal() == 0) {
      target = &bundle.postings;
    } else if (object.object_role() == "ROW_LOCATORS" && object.ordinal() == 1) {
      target = &bundle.locators;
    } else {
      throw std::runtime_error("BM25 artifact contains an unknown object role");
    }
    if (*target != nullptr) {
      throw std::runtime_error("BM25 artifact contains a duplicate object role");
    }
    *target = &object;
  }
  for (const auto& [source, bundle] : result.bundles) {
    if (source.empty() || bundle.postings == nullptr ||
        bundle.locators == nullptr ||
        bundle.postings->row_count() != bundle.locators->row_count()) {
      throw std::runtime_error("BM25 artifact source bundle is incomplete");
    }
  }
  if (result.corpus_stats == nullptr) {
    throw std::runtime_error("BM25 artifact has no generation-wide corpus statistics");
  }
  return result;
}

std::vector<float> readRawVector(
    std::ifstream& input,
    int64_t node_id,
    int64_t dimension) {
  if (node_id < 0 || dimension <= 0 ||
      static_cast<uint64_t>(node_id) >
          std::numeric_limits<uint64_t>::max() /
              static_cast<uint64_t>(dimension) / sizeof(float)) {
    return {};
  }
  const uint64_t offset = static_cast<uint64_t>(node_id) *
      static_cast<uint64_t>(dimension) * sizeof(float);
  input.clear();
  input.seekg(static_cast<std::streamoff>(offset));
  if (!input) {
    return {};
  }
  std::vector<float> result(static_cast<size_t>(dimension));
  std::array<unsigned char, sizeof(uint32_t)> bytes{};
  for (float& value : result) {
    input.read(reinterpret_cast<char*>(bytes.data()), bytes.size());
    if (!input) {
      return {};
    }
    uint32_t bits = 0;
    for (size_t byte = 0; byte < bytes.size(); ++byte) {
      bits |= static_cast<uint32_t>(bytes[byte]) << (byte * 8);
    }
    value = std::bit_cast<float>(bits);
    if (!std::isfinite(value)) {
      return {};
    }
  }
  return result;
}

}  // namespace

std::shared_ptr<ArtifactRetrievalPreparation>
ArtifactRetrievalPlanner::prepare(
    const std::vector<uint8_t>& substrait_plan,
    const std::unordered_map<std::string, proto::OperatorBinding>&
        operator_bindings,
    const std::vector<split::SplitConfig>& splits,
    uint64_t max_object_store_read_bytes,
    std::shared_ptr<resource::TaskObjectReadBudget> task_budget) {
  auto preparation = std::make_shared<ArtifactRetrievalPreparation>();
  preparation->splits = splits;

  substrait::Plan plan;
  if (substrait_plan.empty() ||
      !plan.ParseFromArray(
          substrait_plan.data(), static_cast<int>(substrait_plan.size()))) {
    throw std::runtime_error(
        "Artifact retrieval preparation received an invalid Substrait plan");
  }
  std::vector<proto::RetrievalSourceDetail> details;
  for (const auto& relation : plan.relations()) {
    if (relation.has_root()) {
      collectRetrievalDetails(relation.root().input(), details);
    } else if (relation.has_rel()) {
      collectRetrievalDetails(relation.rel(), details);
    }
  }
  if (details.empty()) {
    return preparation;
  }

  std::unordered_set<std::string> prepared_nodes;
  for (const auto& detail : details) {
    if (!prepared_nodes.emplace(detail.plan_node_id()).second) {
      throw std::runtime_error("Artifact retrieval plan_node_id is duplicated");
    }
    const bool filtered = pangrova::capability::isFilteredDiskAnnRetrieval(
        detail.invocation().capability());
    const bool diskann = pangrova::capability::isDiskAnnRetrieval(
        detail.invocation().capability());
    const bool hnsw = pangrova::capability::isHnswRetrieval(
        detail.invocation().capability());
    const bool ivf_pq = pangrova::capability::isIvfPqRetrieval(
        detail.invocation().capability());
    const bool bm25 = pangrova::capability::isBm25Retrieval(
        detail.invocation().capability());
    const bool exact = pangrova::capability::isExactTopKRetrieval(
        detail.invocation().capability());
    auto binding_it = operator_bindings.find(detail.plan_node_id());
    if (binding_it == operator_bindings.end() ||
        !(exact
              ? pangrova::capability::isExactTopKRetrieval(
                    binding_it->second.capability())
              : filtered
              ? pangrova::capability::isFilteredDiskAnnRetrieval(binding_it->second.capability())
              : diskann
              ? pangrova::capability::isDiskAnnRetrieval(
                    binding_it->second.capability())
              : hnsw
              ? pangrova::capability::isHnswRetrieval(
                    binding_it->second.capability())
              : ivf_pq
              ? pangrova::capability::isIvfPqRetrieval(
                    binding_it->second.capability())
              : bm25 && pangrova::capability::isBm25Retrieval(
                    binding_it->second.capability()))) {
      throw std::runtime_error(
          "Artifact retrieval has no exact immutable OperatorBinding");
    }
    const auto& binding = binding_it->second;
    const auto& policy = binding.execution_policy();
    if (!policy.has_approximation() || detail.output_limit() == 0 ||
        detail.output_limit() > detail.candidate_limit() ||
        policy.approximation().recall_k() != detail.output_limit() ||
        !std::isfinite(policy.approximation().required_recall_at_k()) ||
        detail.candidate_limit() == 0 ||
        (exact && detail.exact_rescore_limit() != 0) ||
        (!exact && (detail.exact_rescore_limit() == 0 ||
                    detail.exact_rescore_limit() > detail.candidate_limit())) ||
        policy.approximation().candidate_limit() != detail.candidate_limit() ||
        policy.approximation().exact_rescore_limit() !=
            (exact ? detail.candidate_limit()
                   : detail.exact_rescore_limit())) {
      throw std::runtime_error("Artifact retrieval execution contract drift");
    }
    if (exact) {
      if (binding.artifacts_size() != 0 || detail.exact_rescore_limit() != 0) {
        throw std::runtime_error(
            "Exact Top-K retrieval must not bind immutable artifacts");
      }
      continue;
    }
    if (binding.artifacts_size() != 1) {
      throw std::runtime_error(
          "Artifact retrieval requires one immutable artifact binding");
    }
    const auto& artifact = binding.artifacts(0);
    if (artifact.source_id() != detail.source_id() ||
        artifact.row_locator().SerializeAsString() !=
            pangrova::capability::physicalRowLocatorContract().SerializeAsString()) {
      throw std::runtime_error(
          "Artifact retrieval format or source identity differs from the plan");
    }
    if (filtered) {
      // The actual active-Task operator owns predicate eligibility, graph work
      // and hydration. Early unfiltered pruning would change SQL semantics.
      continue;
    }
    auto node_splits = splits;

    if (bm25) {
      if (detail.invocation().options_size() != 4 ||
          artifact.artifact_type() != "TEXT_INDEX" ||
          artifact.format().SerializeAsString() !=
              pangrova::capability::bm25ArtifactFormat().SerializeAsString()) {
        throw std::runtime_error("BM25 retrieval binding is incomplete");
      }
      Bm25Query query = parseBm25Query(detail.invocation());
      requireBm25ArtifactProperties(artifact);
      auto bm25_artifacts = buildBm25Artifacts(artifact);
      uint64_t remaining = max_object_store_read_bytes -
          std::min(max_object_store_read_bytes, preparation->downloaded_bytes);
      auto corpus_file = ImmutableArtifactCache::instance().materialize(
          *bm25_artifacts.corpus_stats, remaining, task_budget);
      preparation->downloaded_bytes += corpus_file.downloaded_bytes;
      auto corpus_stats = text::bm25::Bm25CorpusStatsCache::instance().getOrLoad(
          corpus_file.local_path);
      if (!corpus_stats || corpus_stats->documentCount() !=
              bm25_artifacts.corpus_stats->row_count()) {
        throw std::runtime_error(
            "BM25 corpus statistics differ from their immutable binding");
      }
      auto& bundles = bm25_artifacts.bundles;
      for (auto& split : node_splits) {
        if (split.table_name != artifact.source_id() ||
            split.source_snapshot != artifact.source_snapshot()) {
          continue;
        }
        auto bundle_it = bundles.find(split.data_object_id);
        if (bundle_it == bundles.end()) {
          throw std::runtime_error(
              "BM25 artifact does not cover a task source object");
        }
        const auto& bundle = bundle_it->second;
        remaining = max_object_store_read_bytes -
            std::min(max_object_store_read_bytes, preparation->downloaded_bytes);
        auto postings = ImmutableArtifactCache::instance().materialize(
            *bundle.postings, remaining, task_budget);
        preparation->downloaded_bytes += postings.downloaded_bytes;
        remaining = max_object_store_read_bytes -
            std::min(max_object_store_read_bytes, preparation->downloaded_bytes);
        auto locators = ImmutableArtifactCache::instance().materialize(
            *bundle.locators, remaining, task_budget);
        preparation->downloaded_bytes += locators.downloaded_bytes;

        auto segment = text::bm25::Bm25SegmentCache::instance().getOrLoad(
            postings.local_path);
        if (!segment || segment->documentCount() != bundle.postings->row_count()) {
          throw std::runtime_error(
              "BM25 segment metadata differs from its binding");
        }
        auto row_positions = RowLocatorSidecar::readPhysicalRowPositions(
            locators.local_path, bundle.locators->row_count());
        if (row_positions.size() != segment->documentCount()) {
          throw std::runtime_error(
              "BM25 row locator count differs from document count");
        }
        std::unordered_set<uint64_t> unique_positions;
        for (uint64_t position : row_positions) {
          if (!unique_positions.emplace(position).second) {
            throw std::runtime_error("BM25 row locators are not unique");
          }
        }

        auto candidates = segment->search(
            query.tokens, detail.exact_rescore_limit(), query.k1, query.b,
            *corpus_stats);
        split.row_position_filter_enabled = true;
        split.materialized_retrieval_score_enabled = true;
        split.row_position_allowlist.clear();
        split.materialized_retrieval_scores.clear();
        split.row_position_allowlist.reserve(candidates.size());
        split.materialized_retrieval_scores.reserve(candidates.size());
        for (const auto& candidate : candidates) {
          if (candidate.document >= row_positions.size()) {
            throw std::runtime_error("BM25 candidate has no row locator");
          }
          uint64_t position = row_positions[candidate.document];
          split.row_position_allowlist.push_back(position);
          if (!split.materialized_retrieval_scores.emplace(
                  position, candidate.score).second) {
            throw std::runtime_error("BM25 candidate row locator is duplicated");
          }
        }
        std::sort(
            split.row_position_allowlist.begin(),
            split.row_position_allowlist.end());
        preparation->leases.push_back(std::move(postings));
        preparation->leases.push_back(std::move(locators));
        bundles.erase(bundle_it);
      }
      if (!bundles.empty()) {
        throw std::runtime_error(
            "BM25 binding contains objects outside this task partition");
      }
      preparation->leases.push_back(std::move(corpus_file));
      preparation->split_overrides.emplace(
          detail.plan_node_id(), std::move(node_splits));
      continue;
    }

    if (detail.invocation().options_size() != 3) {
      throw std::runtime_error("ANN retrieval requires exactly three options");
    }
    auto query = parseQueryVector(detail.invocation());
    const int64_t search_parameter = diskann
        ? requireSearchListSize(
            detail.invocation(), detail.exact_rescore_limit())
        : hnsw
        ? requireAnnSearchParameter(
            detail.invocation(), "ef_search",
            detail.candidate_limit(), 100'000)
        : requireAnnSearchParameter(
            detail.invocation(), "nprobes", 1, 4'096);
    const auto expected_format = diskann
        ? pangrova::capability::diskAnnArtifactFormat()
        : hnsw
        ? pangrova::capability::hnswArtifactFormat()
        : pangrova::capability::ivfPqArtifactFormat();
    const std::string index_role = diskann
        ? "DISKANN_GRAPH" : hnsw ? "HNSW_GRAPH" : "IVF_PQ_INDEX";
    if (artifact.format().SerializeAsString() !=
            expected_format.SerializeAsString() ||
        artifact.artifact_type() != "VECTOR_INDEX") {
      throw std::runtime_error(
          "ANN artifact format or source identity differs from the plan");
    }
    const int64_t dimension = requireDimension(artifact, query.size());
    const auto metric = diskann ? vector::DistanceMetric::L2
        : requireArtifactMetric(artifact, detail.invocation());
    auto bundles = buildVectorBundles(artifact, index_role);

    std::unordered_map<std::string, std::vector<uint64_t>> diskann_positions;
    for (auto& split : node_splits) {
      if (split.table_name != artifact.source_id() ||
          split.source_snapshot != artifact.source_snapshot()) {
        continue;
      }
      if (diskann) {
        auto previous = diskann_positions.find(split.data_object_id);
        if (previous != diskann_positions.end()) {
          // Multiple non-overlapping source ranges share one physical graph,
          // raw object and locator domain; do not materialize/search it twice.
          split.row_position_filter_enabled = true;
          split.row_position_allowlist = previous->second;
          continue;
        }
      }
      auto bundle_it = bundles.find(split.data_object_id);
      if (bundle_it == bundles.end()) {
        throw std::runtime_error(
            "ANN artifact does not cover a task source object");
      }
      const auto& bundle = bundle_it->second;
      if (diskann) {
        const std::array<proto::ArtifactObject, 3> objects{
            *bundle.graph, *bundle.vectors, *bundle.locators};
        auto owner = std::make_shared<ArtifactVectorSearch>(objects, dimension,
            max_object_store_read_bytes - preparation->downloaded_bytes,
            pangrova::resource::standaloneHeapResource(), [] {}, task_budget);
        auto positions = owner->searchUnfiltered(query, detail.candidate_limit(),
            detail.exact_rescore_limit(), search_parameter, [] {});
        split.row_position_filter_enabled = true;
        split.row_position_allowlist.assign(positions.begin(), positions.end());
        std::sort(split.row_position_allowlist.begin(), split.row_position_allowlist.end());
        preparation->downloaded_bytes += owner->downloadedBytes();
        for (const auto& lease : owner->materializedObjects()) {
          preparation->leases.push_back(lease);  // The same real cache pin, not another download.
        }
        diskann_positions.emplace(split.data_object_id, split.row_position_allowlist);
        preparation->vector_search_owners.push_back(std::move(owner));
        bundles.erase(bundle_it);
        continue;
      }
      uint64_t remaining = max_object_store_read_bytes -
          std::min(max_object_store_read_bytes, preparation->downloaded_bytes);
      auto graph = ImmutableArtifactCache::instance().materialize(
          *bundle.graph, remaining, task_budget);
      preparation->downloaded_bytes += graph.downloaded_bytes;
      remaining = max_object_store_read_bytes -
          std::min(max_object_store_read_bytes, preparation->downloaded_bytes);
      auto vectors = ImmutableArtifactCache::instance().materialize(
          *bundle.vectors, remaining, task_budget);
      preparation->downloaded_bytes += vectors.downloaded_bytes;
      remaining = max_object_store_read_bytes -
          std::min(max_object_store_read_bytes, preparation->downloaded_bytes);
      auto locators = ImmutableArtifactCache::instance().materialize(
          *bundle.locators, remaining, task_budget);
      preparation->downloaded_bytes += locators.downloaded_bytes;

      int64_t node_count = 0;
      std::vector<int64_t> candidate_nodes;
      if (hnsw) {
        auto index = vector::hnsw::HnswIndexCache::instance().getOrLoad(
            graph.local_path, dimension);
        if (!index || index->dimension() != dimension || index->metric() != metric ||
            index->size() <= 0 ||
            static_cast<uint64_t>(index->size()) != bundle.graph->row_count()) {
          throw std::runtime_error(
              "HNSW graph metadata differs from its binding");
        }
        node_count = index->size();
        auto approximate = index->search(
            query.data(), detail.candidate_limit(), search_parameter);
        candidate_nodes.reserve(approximate.size());
        for (const auto& candidate : approximate) {
          candidate_nodes.push_back(candidate.node_id);
        }
      } else {
        auto index = vector::ivfpq::IvfPqIndexCache::instance().getOrLoad(
            graph.local_path, dimension);
        if (!index || index->dimension() != dimension || index->metric() != metric ||
            index->size() <= 0 ||
            static_cast<uint64_t>(index->size()) != bundle.graph->row_count() ||
            search_parameter > index->coarseClusters()) {
          throw std::runtime_error(
              "IVF-PQ metadata or nprobes differs from its binding");
        }
        node_count = index->size();
        auto approximate = index->search(
            query.data(), detail.candidate_limit(), search_parameter);
        candidate_nodes.reserve(approximate.size());
        for (const auto& candidate : approximate) {
          candidate_nodes.push_back(candidate.node_id);
        }
      }
      const uint64_t expected_vector_bytes =
          static_cast<uint64_t>(node_count) *
          static_cast<uint64_t>(dimension) * sizeof(float);
      if (vectors.size_bytes != expected_vector_bytes) {
        throw std::runtime_error("ANN raw vector object has an invalid size");
      }
      auto row_positions = RowLocatorSidecar::readPhysicalRowPositions(
          locators.local_path, bundle.locators->row_count());
      if (row_positions.size() != static_cast<size_t>(node_count)) {
        throw std::runtime_error("ANN row locator count differs from index nodes");
      }
      std::unordered_set<uint64_t> unique_positions;
      for (uint64_t position : row_positions) {
        if (!unique_positions.emplace(position).second) {
          throw std::runtime_error("ANN row locators are not unique");
        }
      }

      auto raw_input = std::make_shared<std::ifstream>(
          vectors.local_path, std::ios::binary);
      if (!*raw_input) {
        throw std::runtime_error("Cannot open cached ANN raw vectors");
      }
      vector::VectorTopKHeap exact(detail.exact_rescore_limit());
      for (int64_t node : candidate_nodes) {
        auto value = readRawVector(*raw_input, node, dimension);
        if (value.empty()) {
          throw std::runtime_error(
              "ANN full-precision rescore could not read a candidate vector");
        }
        exact.push(
            node, vector::metricDistance(
                metric, query.data(), value.data(), static_cast<size_t>(dimension)));
      }
      auto candidates = exact.drainSorted();
      split.row_position_filter_enabled = true;
      split.row_position_allowlist.clear();
      split.row_position_allowlist.reserve(candidates.size());
      for (const auto& candidate : candidates) {
        if (candidate.rowId < 0 ||
            static_cast<size_t>(candidate.rowId) >= row_positions.size()) {
          throw std::runtime_error("ANN candidate node has no row locator");
        }
        split.row_position_allowlist.push_back(
            row_positions[static_cast<size_t>(candidate.rowId)]);
      }
      std::sort(
          split.row_position_allowlist.begin(),
          split.row_position_allowlist.end());
      preparation->leases.push_back(std::move(graph));
      preparation->leases.push_back(std::move(vectors));
      preparation->leases.push_back(std::move(locators));
      bundles.erase(bundle_it);
    }
    if (!bundles.empty()) {
      throw std::runtime_error(
          "ANN binding contains objects outside this task partition");
    }
    preparation->split_overrides.emplace(
        detail.plan_node_id(), std::move(node_splits));
  }
  return preparation;
}

}  // namespace pangrova::artifact
