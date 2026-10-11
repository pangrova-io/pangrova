// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "velox/plan/ArtifactBuildOperator.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>

#include "artifact/GeneratedValueSegment.h"
#include "artifact/ImmutableArtifactCache.h"
#include "artifact/ImmutableArtifactWriter.h"
#include "artifact/RowLocatorSidecar.h"
#include "artifact/TabularExportWriter.h"
#include "text/bm25/Bm25CorpusStats.h"
#include "vector/diskann/DiskAnnBuilder.h"
#include "vector/hnsw/HnswIndex.h"
#include "vector/ivfpq/IvfPqIndex.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SelectivityVector.h"

namespace pangrova::velox {
namespace {

std::string sha256(std::string_view input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  if (EVP_Digest(input.data(), input.size(), digest.data(), &size, EVP_sha256(),
                 nullptr) != 1 ||
      size != 32) {
    throw std::runtime_error("Failed to compute artifact report SHA-256");
  }
  return {reinterpret_cast<const char*>(digest.data()), size};
}

std::string toHex(std::string_view bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (unsigned char value : bytes) {
    result.push_back(digits[value >> 4]);
    result.push_back(digits[value & 0x0f]);
  }
  return result;
}

class ScratchDirectory final {
 public:
  explicit ScratchDirectory(std::filesystem::path path)
      : path_(std::move(path)) {
    std::filesystem::create_directories(path_);
  }
  ~ScratchDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

void populateObject(protocol::v2::ArtifactObject* output,
                    const protocol::v2::ArtifactWriteBinding& binding,
                    const artifact::ImmutableArtifactObject& stored,
                    const std::string& role, const std::string& scope_id,
                    protocol::v2::ArtifactObjectScope scope, uint32_t ordinal,
                    uint64_t rows) {
  std::string identity = "artifact-object-v1:";
  const auto append_identity_field = [&identity](std::string_view field) {
    identity.append(std::to_string(field.size()));
    identity.push_back(':');
    identity.append(field);
  };
  append_identity_field(binding.artifact_id());
  append_identity_field(binding.generation());
  append_identity_field(scope_id);
  append_identity_field(role);
  append_identity_field(stored.sha256);
  output->set_object_id("artifact-object-sha256:" + toHex(sha256(identity)));
  output->set_opaque_uri(stored.uri);
  output->set_size_bytes(stored.size_bytes);
  output->mutable_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  output->mutable_checksum()->set_digest(stored.sha256);
  output->set_encryption_key_ref(binding.encryption_key_ref());
  output->set_object_role(role);
  output->set_source_object_id(scope_id);
  output->set_row_count(rows);
  output->set_ordinal(ordinal);
  output->set_scope(scope);
}

void writeRawVectors(const std::string& path,
                     const std::vector<float>& vectors) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("Failed to create ANN raw-vector sidecar");
  }
  for (float value : vectors) {
    const uint32_t bits = std::bit_cast<uint32_t>(value);
    std::array<char, sizeof(uint32_t)> bytes{};
    for (size_t byte = 0; byte < bytes.size(); ++byte) {
      bytes[byte] = static_cast<char>((bits >> (byte * 8)) & 0xffU);
    }
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  output.flush();
  if (!output.good()) {
    throw std::runtime_error("Failed to write ANN raw-vector sidecar");
  }
}

}  // namespace

ArtifactBuildOperator::ArtifactBuildOperator(
    int32_t operator_id, facebook::velox::exec::DriverCtx* driver_ctx,
    const std::shared_ptr<const ArtifactBuildNode>& node)
    : Operator(driver_ctx, node->outputType(), operator_id, node->id(),
               "ArtifactBuild"),
      config_(node->config()),
      export_input_type_(node->sources().at(0)->outputType()) {}

ArtifactBuildOperator::~ArtifactBuildOperator() {
  export_writer_.reset();
  if (!export_scratch_path_.empty()) {
    std::error_code ignored;
    std::filesystem::remove_all(export_scratch_path_, ignored);
  }
}

bool ArtifactBuildOperator::needsInput() const { return !noMoreInput_; }

void ArtifactBuildOperator::addInput(facebook::velox::RowVectorPtr input) {
  VELOX_USER_CHECK(!noMoreInput_, "ArtifactBuild received input after EOS");
  if (!input || input->size() == 0) {
    return;
  }
  VELOX_USER_CHECK(
      config_.kind != ArtifactBuildKind::BM25_REDUCE &&
          config_.kind != ArtifactBuildKind::BM25_FINALIZE,
      "Artifact statistics reduction must not receive source rows");
  VELOX_USER_CHECK_LE(
      static_cast<uint64_t>(input->size()),
      config_.max_rows -
          std::min<uint64_t>(config_.max_rows, row_positions_.size()),
      "ArtifactBuild row limit exceeded");
  const uint64_t batch_bytes = input->estimateFlatSize();
  VELOX_USER_CHECK_LE(batch_bytes,
                      config_.task.max_input_bytes -
                          std::min(config_.task.max_input_bytes, input_bytes_),
                      "ArtifactBuild input byte grant exceeded");
  if (config_.kind == ArtifactBuildKind::HNSW) {
    const uint64_t next_rows =
        row_positions_.size() + static_cast<uint64_t>(input->size());
    if (vector::hnsw::HnswIndex::estimatePeakBuildBytes(
            static_cast<int64_t>(next_rows), config_.hnsw.dimension,
            config_.hnsw.max_connections) > config_.task.max_input_bytes) {
      throw std::runtime_error(
          "RESOURCE_EXHAUSTED: HNSW estimated peak exceeds memory grant");
    }
  }

  facebook::velox::LazyVector::ensureLoadedRows(
      input, facebook::velox::SelectivityVector(input->size()));
  if (config_.kind == ArtifactBuildKind::TABULAR_EXPORT) {
    VELOX_USER_CHECK_LE(
        static_cast<uint64_t>(input->size()),
        config_.max_rows - std::min(config_.max_rows, export_rows_),
        "Tabular export row limit exceeded");
    openExportWriter();
    export_writer_->write(input);
    export_rows_ += static_cast<uint64_t>(input->size());
    input_bytes_ += batch_bytes;
    export_peak_batch_bytes_ = std::max(export_peak_batch_bytes_, batch_bytes);
    VELOX_USER_CHECK_LE(input_bytes_, config_.task.max_input_bytes,
                        "Tabular export input byte grant exceeded");
    return;
  }
  facebook::velox::DecodedVector row_positions(
      *input->childAt(config_.row_position_channel));
  if (config_.kind == ArtifactBuildKind::BM25) {
    facebook::velox::DecodedVector texts(*input->childAt(config_.text_channel));
    for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
      VELOX_USER_CHECK(!row_positions.isNullAt(row) && !texts.isNullAt(row),
                       "ArtifactBuild row position and text must be non-null");
      const int64_t signed_position = row_positions.valueAt<int64_t>(row);
      VELOX_USER_CHECK(
          signed_position >= 0,
          "ArtifactBuild physical row position must not be negative");
      const uint64_t row_position = static_cast<uint64_t>(signed_position);
      VELOX_USER_CHECK(unique_row_positions_.emplace(row_position).second,
                       "ArtifactBuild physical row position must be unique "
                       "within a source object");
      const auto value = texts.valueAt<facebook::velox::StringView>(row);
      bm25_.addDocument(std::string_view(value.data(), value.size()));
      row_positions_.push_back(row_position);
    }
    input_bytes_ += batch_bytes;
    VELOX_USER_CHECK_LE(
        bm25_.estimatedMemoryBytes() + row_positions_.size() * sizeof(uint64_t),
        config_.task.max_input_bytes,
        "ArtifactBuild BM25 materialized input exceeds memory grant");
    return;
  }
  appendVectors(input);
  input_bytes_ += batch_bytes;
  const uint64_t materialized_bytes = vectors_.size() * sizeof(float) +
                                      generated_validity_.size() +
                                      row_positions_.size() * sizeof(uint64_t);
  VELOX_USER_CHECK_LE(materialized_bytes, config_.task.max_input_bytes,
                      "ArtifactBuild materialized input exceeds memory grant");
}

void ArtifactBuildOperator::appendVectors(
    const facebook::velox::RowVectorPtr& input) {
  facebook::velox::DecodedVector row_positions(
      *input->childAt(config_.row_position_channel));
  const bool generated = config_.kind == ArtifactBuildKind::GENERATED_VALUES;
  if (generated) {
    VELOX_USER_CHECK_GT(config_.generated_value_dimension, 0);
    VELOX_USER_CHECK_LE(config_.generated_value_dimension,
                        artifact::GeneratedValueSegment::kMaxDimension);
    dimension_ = config_.generated_value_dimension;
    const uint64_t row_bytes =
        static_cast<uint64_t>(dimension_) * sizeof(float) + sizeof(uint64_t) +
        1;
    VELOX_USER_CHECK_LE(
        static_cast<uint64_t>(input->size()) + row_positions_.size(),
        config_.task.max_input_bytes / row_bytes,
        "Generated-value dense materialization exceeds memory grant");
  }
  const size_t value_channel =
      config_.kind == ArtifactBuildKind::GENERATED_VALUES
          ? config_.generated_value_channel
          : config_.vector_channel;
  facebook::velox::DecodedVector arrays(*input->childAt(value_channel));
  auto* array_base = arrays.base()->as<facebook::velox::ArrayVector>();
  VELOX_USER_CHECK_NOT_NULL(
      array_base, "ArtifactBuild vector input must decode to ARRAY(REAL)");
  facebook::velox::DecodedVector elements(*array_base->elements());

  for (facebook::velox::vector_size_t row = 0; row < input->size(); ++row) {
    VELOX_USER_CHECK(!row_positions.isNullAt(row),
                     "ArtifactBuild row position must be non-null");
    VELOX_USER_CHECK(generated || !arrays.isNullAt(row),
                     "ArtifactBuild index vector must be non-null");
    const int64_t signed_position = row_positions.valueAt<int64_t>(row);
    VELOX_USER_CHECK(
        signed_position >= 0,
        "ArtifactBuild physical row position must not be negative");
    const uint64_t row_position = static_cast<uint64_t>(signed_position);
    VELOX_USER_CHECK(unique_row_positions_.emplace(row_position).second,
                     "ArtifactBuild physical row position must be unique "
                     "within a source object");

    if (generated) {
      generated_validity_.push_back(arrays.isNullAt(row) ? 0 : 1);
      if (arrays.isNullAt(row)) {
        row_positions_.push_back(row_position);
        vectors_.resize(vectors_.size() + static_cast<size_t>(dimension_),
                        0.0F);
        continue;
      }
    }

    const auto base_row = arrays.index(row);
    const auto offset = array_base->offsetAt(base_row);
    const auto size = array_base->sizeAt(base_row);
    VELOX_USER_CHECK_GT(size, 0, "ArtifactBuild vector must not be empty");
    if (dimension_ == 0) {
      dimension_ = size;
      const int64_t expected_dimension =
          config_.kind == ArtifactBuildKind::DISKANN ? config_.diskann.dimension
          : config_.kind == ArtifactBuildKind::HNSW  ? config_.hnsw.dimension
          : config_.kind == ArtifactBuildKind::IVF_PQ
              ? config_.ivf_pq.dimension
              : config_.generated_value_dimension;
      VELOX_USER_CHECK_EQ(
          dimension_, expected_dimension,
          "ArtifactBuild vector dimension differs from binding");
    } else {
      VELOX_USER_CHECK_EQ(size, dimension_,
                          "ArtifactBuild vectors must have one dimension");
    }
    row_positions_.push_back(row_position);
    vectors_.reserve(vectors_.size() + static_cast<size_t>(size));
    for (facebook::velox::vector_size_t index = 0; index < size; ++index) {
      const auto element = offset + index;
      VELOX_USER_CHECK(!elements.isNullAt(element),
                       "ArtifactBuild vector elements must be non-null");
      const float value = elements.valueAt<float>(element);
      VELOX_USER_CHECK(std::isfinite(value),
                       "ArtifactBuild vector elements must be finite");
      vectors_.push_back(value);
    }
  }
}

void ArtifactBuildOperator::openExportWriter() {
  if (export_writer_) {
    return;
  }
  VELOX_USER_CHECK(
      config_.kind == ArtifactBuildKind::TABULAR_EXPORT && export_input_type_,
      "Tabular export requires an exact Native input schema");
  export_started_ = std::chrono::steady_clock::now();
  const auto scratch_name =
      "artifact-" + toHex(sha256(config_.output.idempotency_key()));
  export_scratch_path_ =
      std::filesystem::path(config_.task.scratch_directory) / scratch_name;
  std::filesystem::create_directories(export_scratch_path_);
  export_writer_ = std::make_unique<artifact::TabularExportWriter>(
      (export_scratch_path_ / "segment.arrow").string(), operatorCtx_->pool(),
      export_input_type_);
}

facebook::velox::RowVectorPtr ArtifactBuildOperator::getOutput() {
  return nullptr;
}

void ArtifactBuildOperator::noMoreInput() {
  Operator::noMoreInput();
  buildAndPublish();
}

void ArtifactBuildOperator::buildAndPublish() {
  if (published_) {
    return;
  }
  VELOX_USER_CHECK_NOT_NULL(config_.task.report_state,
                            "ArtifactBuild report state is unavailable");
  if (config_.kind == ArtifactBuildKind::BM25_REDUCE ||
      config_.kind == ArtifactBuildKind::BM25_FINALIZE) {
    mergeBm25Statistics();
    return;
  }
  if (config_.kind == ArtifactBuildKind::TABULAR_EXPORT) {
    VELOX_USER_CHECK(
        config_.output.work_kind() ==
                protocol::v2::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION &&
            !config_.output.work_unit_id().empty() &&
            config_.output.output_scope_id() == config_.output.work_unit_id() &&
            config_.output.expected_source_object_ids_size() == 0 &&
            !config_.output.has_row_locator(),
        "Tabular export requires an exact relational work-unit scope");
    openExportWriter();
    export_writer_->finish();
    const auto output_path = export_writer_->path();
    const uint64_t output_bytes = std::filesystem::file_size(output_path);
    VELOX_USER_CHECK_LE(output_bytes, config_.task.max_spill_bytes,
                        "Tabular export exceeds spill grant");
    VELOX_USER_CHECK_LE(output_bytes, config_.task.max_object_store_write_bytes,
                        "Tabular export exceeds object-store write grant");
    const auto stored = artifact::ImmutableArtifactWriter::putFile(
        output_path, config_.output.staging_uri(), "TABULAR_DATA");
    protocol::v2::ArtifactWorkReport report;
    report.set_job_id(config_.task.job_id);
    report.set_page_id(config_.task.page_id);
    report.set_fencing_token(config_.task.fencing_token);
    report.set_source_id(config_.output.source_id());
    report.set_source_snapshot(config_.output.source_snapshot());
    report.set_staging_generation(config_.output.generation());
    populateObject(report.add_outputs(), config_.output, stored, "TABULAR_DATA",
                   config_.output.output_scope_id(),
                   protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT, 0,
                   export_rows_);
    const auto elapsed = std::chrono::steady_clock::now() - export_started_;
    report.mutable_usage()->set_cpu_nanos(
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
    report.mutable_usage()->set_peak_native_memory_bytes(
        export_peak_batch_bytes_);
    report.mutable_usage()->set_spill_bytes(output_bytes);
    report.mutable_usage()->set_object_store_write_bytes(output_bytes);
    auto checksum_input = report.SerializeAsString();
    report.mutable_report_checksum()->set_algorithm(
        protocol::v2::Checksum::ALGORITHM_SHA256);
    report.mutable_report_checksum()->set_digest(sha256(checksum_input));
    config_.task.report_state->publish(report);
    published_ = true;
    export_writer_.reset();
    std::error_code ignored;
    std::filesystem::remove_all(export_scratch_path_, ignored);
    export_scratch_path_.clear();
    return;
  }
  VELOX_USER_CHECK(!row_positions_.empty(),
                   "ArtifactBuild cannot publish an empty artifact segment");

  const auto started = std::chrono::steady_clock::now();
  const auto scratch_name =
      "artifact-" + toHex(sha256(config_.output.idempotency_key()));
  ScratchDirectory scratch(
      std::filesystem::path(config_.task.scratch_directory) / scratch_name);
  if (config_.kind == ArtifactBuildKind::BM25) {
    const auto postings_path = (scratch.path() / "segment.bm25").string();
    const auto locators_path = (scratch.path() / "segment.locators").string();
    const auto statistics_path = (scratch.path() / "segment.stats").string();
    bm25_.write(postings_path);
    bm25_.writeStatistics(statistics_path);
    artifact::RowLocatorSidecar::writePhysicalRowPositions(locators_path,
                                                           row_positions_);
    const uint64_t local_output_bytes =
        std::filesystem::file_size(postings_path) +
        std::filesystem::file_size(locators_path) +
        std::filesystem::file_size(statistics_path);
    VELOX_USER_CHECK_LE(local_output_bytes, config_.task.max_spill_bytes,
                        "ArtifactBuild local output exceeds spill grant");
    VELOX_USER_CHECK_LE(
        local_output_bytes, config_.task.max_object_store_write_bytes,
        "ArtifactBuild output exceeds object-store write grant");
    const auto postings = artifact::ImmutableArtifactWriter::putFile(
        postings_path, config_.output.staging_uri(), "BM25_POSTINGS");
    const auto row_locators = artifact::ImmutableArtifactWriter::putFile(
        locators_path, config_.output.staging_uri(), "ROW_LOCATORS");
    const auto statistics = artifact::ImmutableArtifactWriter::putFile(
        statistics_path, config_.output.staging_uri(), "BM25_STATS_SHARD");
    protocol::v2::ArtifactWorkReport report;
    report.set_job_id(config_.task.job_id);
    report.set_page_id(config_.task.page_id);
    report.set_fencing_token(config_.task.fencing_token);
    report.set_source_id(config_.output.source_id());
    report.set_source_snapshot(config_.output.source_snapshot());
    report.set_staging_generation(config_.output.generation());
    populateObject(report.add_outputs(), config_.output, postings,
                   "BM25_POSTINGS",
                   config_.output.expected_source_object_ids(0),
                   protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 0,
                   row_positions_.size());
    populateObject(report.add_outputs(), config_.output, row_locators,
                   "ROW_LOCATORS", config_.output.expected_source_object_ids(0),
                   protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 1,
                   row_positions_.size());
    populateObject(report.add_outputs(), config_.output, statistics,
                   "BM25_STATS_SHARD", config_.task.page_id,
                   protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT, 2,
                   row_positions_.size());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    report.mutable_usage()->set_cpu_nanos(
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
    report.mutable_usage()->set_peak_native_memory_bytes(
        bm25_.estimatedMemoryBytes() +
        row_positions_.size() * sizeof(uint64_t));
    report.mutable_usage()->set_spill_bytes(local_output_bytes);
    report.mutable_usage()->set_object_store_write_bytes(local_output_bytes);
    auto checksum_input = report.SerializeAsString();
    report.mutable_report_checksum()->set_algorithm(
        protocol::v2::Checksum::ALGORITHM_SHA256);
    report.mutable_report_checksum()->set_digest(sha256(checksum_input));
    config_.task.report_state->publish(report);
    published_ = true;
    row_positions_.clear();
    unique_row_positions_.clear();
    return;
  }
  if (config_.kind == ArtifactBuildKind::GENERATED_VALUES) {
    VELOX_USER_CHECK_GT(dimension_, 0,
                        "Generated-value dimension is unavailable");
    const auto values_path =
        (scratch.path() / "segment.generated-values").string();
    const auto locators_path = (scratch.path() / "segment.locators").string();
    artifact::GeneratedValueSegment::write(
        values_path, vectors_, generated_validity_, row_positions_.size(),
        static_cast<uint32_t>(dimension_));
    artifact::RowLocatorSidecar::writePhysicalRowPositions(locators_path,
                                                           row_positions_);
    const uint64_t local_output_bytes =
        std::filesystem::file_size(values_path) +
        std::filesystem::file_size(locators_path);
    VELOX_USER_CHECK_LE(local_output_bytes, config_.task.max_spill_bytes,
                        "Generated-value output exceeds spill grant");
    VELOX_USER_CHECK_LE(
        local_output_bytes, config_.task.max_object_store_write_bytes,
        "Generated-value output exceeds object-store write grant");
    const auto generated_values = artifact::ImmutableArtifactWriter::putFile(
        values_path, config_.output.staging_uri(), "GENERATED_VALUES");
    const auto row_locators = artifact::ImmutableArtifactWriter::putFile(
        locators_path, config_.output.staging_uri(), "ROW_LOCATORS");

    protocol::v2::ArtifactWorkReport report;
    report.set_job_id(config_.task.job_id);
    report.set_page_id(config_.task.page_id);
    report.set_fencing_token(config_.task.fencing_token);
    report.set_source_id(config_.output.source_id());
    report.set_source_snapshot(config_.output.source_snapshot());
    report.set_staging_generation(config_.output.generation());
    const auto& source_object = config_.output.expected_source_object_ids(0);
    populateObject(report.add_outputs(), config_.output, generated_values,
                   "GENERATED_VALUES", source_object,
                   protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 0,
                   row_positions_.size());
    populateObject(report.add_outputs(), config_.output, row_locators,
                   "ROW_LOCATORS", source_object,
                   protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 1,
                   row_positions_.size());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    report.mutable_usage()->set_cpu_nanos(
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
    report.mutable_usage()->set_peak_native_memory_bytes(
        vectors_.size() * sizeof(float) + generated_validity_.size() +
        row_positions_.size() * sizeof(uint64_t));
    report.mutable_usage()->set_spill_bytes(local_output_bytes);
    report.mutable_usage()->set_object_store_write_bytes(local_output_bytes);
    const auto checksum_input = report.SerializeAsString();
    report.mutable_report_checksum()->set_algorithm(
        protocol::v2::Checksum::ALGORITHM_SHA256);
    report.mutable_report_checksum()->set_digest(sha256(checksum_input));
    config_.task.report_state->publish(report);
    published_ = true;
    row_positions_.clear();
    vectors_.clear();
    generated_validity_.clear();
    unique_row_positions_.clear();
    return;
  }
  VELOX_USER_CHECK_GT(dimension_, 0,
                      "ArtifactBuild ANN dimension is unavailable");
  const std::string algorithm =
      config_.kind == ArtifactBuildKind::DISKANN ? "diskann"
      : config_.kind == ArtifactBuildKind::HNSW  ? "hnsw"
                                                 : "ivfpq";
  const std::string index_role =
      config_.kind == ArtifactBuildKind::DISKANN ? "DISKANN_GRAPH"
      : config_.kind == ArtifactBuildKind::HNSW  ? "HNSW_GRAPH"
                                                 : "IVF_PQ_INDEX";
  const auto graph_path = (scratch.path() / ("segment." + algorithm)).string();
  const auto vectors_path = graph_path + ".vecs";
  const auto locators_path = (scratch.path() / "segment.locators").string();

  if (config_.kind == ArtifactBuildKind::DISKANN) {
    vector::diskann::DiskAnnBuilder(config_.diskann)
        .buildFromVectors(vectors_.data(),
                          static_cast<int64_t>(row_positions_.size()),
                          dimension_, graph_path);
  } else if (config_.kind == ArtifactBuildKind::HNSW) {
    vector::hnsw::HnswIndex index(config_.hnsw);
    const uint64_t retained_input_bytes =
        vectors_.capacity() * sizeof(float) +
        row_positions_.capacity() * sizeof(uint64_t) +
        unique_row_positions_.size() * 64;
    index.build(vectors_.data(), static_cast<int64_t>(row_positions_.size()),
                dimension_, config_.task.max_input_bytes, retained_input_bytes);
    VELOX_USER_CHECK(index.save(graph_path),
                     "Failed to save Native HNSW index");
    writeRawVectors(vectors_path, vectors_);
  } else {
    vector::ivfpq::IvfPqIndex index(config_.ivf_pq);
    index.build(vectors_.data(), static_cast<int64_t>(row_positions_.size()),
                dimension_);
    VELOX_USER_CHECK(index.save(graph_path),
                     "Failed to save Native IVF-PQ index");
    writeRawVectors(vectors_path, vectors_);
  }
  artifact::RowLocatorSidecar::writePhysicalRowPositions(locators_path,
                                                         row_positions_);

  const uint64_t local_output_bytes = std::filesystem::file_size(graph_path) +
                                      std::filesystem::file_size(vectors_path) +
                                      std::filesystem::file_size(locators_path);
  VELOX_USER_CHECK_LE(local_output_bytes, config_.task.max_spill_bytes,
                      "ArtifactBuild local output exceeds spill grant");
  VELOX_USER_CHECK_LE(local_output_bytes,
                      config_.task.max_object_store_write_bytes,
                      "ArtifactBuild output exceeds object-store write grant");

  const auto graph = artifact::ImmutableArtifactWriter::putFile(
      graph_path, config_.output.staging_uri(), index_role);
  const auto raw_vectors = artifact::ImmutableArtifactWriter::putFile(
      vectors_path, config_.output.staging_uri(), "RAW_VECTORS");
  const auto row_locators = artifact::ImmutableArtifactWriter::putFile(
      locators_path, config_.output.staging_uri(), "ROW_LOCATORS");

  protocol::v2::ArtifactWorkReport report;
  report.set_job_id(config_.task.job_id);
  report.set_page_id(config_.task.page_id);
  report.set_fencing_token(config_.task.fencing_token);
  report.set_source_id(config_.output.source_id());
  report.set_source_snapshot(config_.output.source_snapshot());
  report.set_staging_generation(config_.output.generation());
  populateObject(report.add_outputs(), config_.output, graph, index_role,
                 config_.output.expected_source_object_ids(0),
                 protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 0,
                 row_positions_.size());
  populateObject(report.add_outputs(), config_.output, raw_vectors,
                 "RAW_VECTORS", config_.output.expected_source_object_ids(0),
                 protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 1,
                 row_positions_.size());
  populateObject(report.add_outputs(), config_.output, row_locators,
                 "ROW_LOCATORS", config_.output.expected_source_object_ids(0),
                 protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT, 2,
                 row_positions_.size());
  const auto elapsed = std::chrono::steady_clock::now() - started;
  report.mutable_usage()->set_cpu_nanos(
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
  report.mutable_usage()->set_peak_native_memory_bytes(
      vectors_.size() * sizeof(float) +
      row_positions_.size() * sizeof(uint64_t));
  report.mutable_usage()->set_spill_bytes(local_output_bytes);
  report.mutable_usage()->set_object_store_write_bytes(local_output_bytes);
  auto checksum_input = report.SerializeAsString();
  report.mutable_report_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  report.mutable_report_checksum()->set_digest(sha256(checksum_input));
  config_.task.report_state->publish(report);
  published_ = true;
  vectors_.clear();
  vectors_.shrink_to_fit();
  row_positions_.clear();
  unique_row_positions_.clear();
}

void ArtifactBuildOperator::mergeBm25Statistics() {
  VELOX_USER_CHECK(
      !config_.input_objects.empty() && config_.input_objects.size() <= 64,
      "Artifact statistics reduction requires between 1 and 64 inputs");
  const auto started = std::chrono::steady_clock::now();
  const auto scratch_name =
      "artifact-" + toHex(sha256(config_.output.idempotency_key()));
  ScratchDirectory scratch(
      std::filesystem::path(config_.task.scratch_directory) / scratch_name);
  std::vector<artifact::MaterializedArtifact> leases;
  std::vector<std::string> input_paths;
  leases.reserve(config_.input_objects.size());
  input_paths.reserve(config_.input_objects.size());
  uint64_t downloaded_bytes = 0;
  for (const auto& input : config_.input_objects) {
    VELOX_USER_CHECK(
        input.object_role() == "BM25_STATS_SHARD" &&
            input.scope() == protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT,
        "BM25 reduction accepts only work-unit statistics shards");
    const uint64_t remaining =
        config_.task.max_object_store_read_bytes -
        std::min(config_.task.max_object_store_read_bytes, downloaded_bytes);
    auto materialized =
        artifact::ImmutableArtifactCache::instance().materialize(input,
                                                                 remaining, config_.task.read_budget);
    downloaded_bytes += materialized.downloaded_bytes;
    input_paths.push_back(materialized.local_path);
    leases.push_back(std::move(materialized));
  }

  const auto output_path = (scratch.path() / "merged.stats").string();
  text::bm25::Bm25CorpusStats::mergeSortedShards(input_paths, output_path);
  const uint64_t output_bytes = std::filesystem::file_size(output_path);
  VELOX_USER_CHECK_LE(output_bytes, config_.task.max_spill_bytes,
                      "Artifact statistics output exceeds spill grant");
  VELOX_USER_CHECK_LE(
      output_bytes, config_.task.max_object_store_write_bytes,
      "Artifact statistics output exceeds object-store write grant");
  const bool final = config_.kind == ArtifactBuildKind::BM25_FINALIZE;
  const std::string role = final ? "BM25_CORPUS_STATS" : "BM25_STATS_SHARD";
  const auto stored = artifact::ImmutableArtifactWriter::putFile(
      output_path, config_.output.staging_uri(), role);
  const uint64_t documents =
      text::bm25::Bm25CorpusStats::documentCountFromDisk(output_path);

  protocol::v2::ArtifactWorkReport report;
  report.set_job_id(config_.task.job_id);
  report.set_page_id(config_.task.page_id);
  report.set_fencing_token(config_.task.fencing_token);
  report.set_source_id(config_.output.source_id());
  report.set_source_snapshot(config_.output.source_snapshot());
  report.set_staging_generation(config_.output.generation());
  populateObject(report.add_outputs(), config_.output, stored, role,
                 config_.output.output_scope_id(),
                 final ? protocol::v2::ARTIFACT_OBJECT_SCOPE_GENERATION
                       : protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT,
                 0, documents);
  const auto elapsed = std::chrono::steady_clock::now() - started;
  report.mutable_usage()->set_cpu_nanos(
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
  report.mutable_usage()->set_peak_native_memory_bytes(
      config_.input_objects.size() * sizeof(protocol::v2::ArtifactObject));
  report.mutable_usage()->set_spill_bytes(output_bytes);
  report.mutable_usage()->set_object_store_read_bytes(downloaded_bytes);
  report.mutable_usage()->set_object_store_write_bytes(output_bytes);
  const auto checksum_input = report.SerializeAsString();
  report.mutable_report_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  report.mutable_report_checksum()->set_digest(sha256(checksum_input));
  config_.task.report_state->publish(report);
  published_ = true;
}

bool ArtifactBuildOperator::isFinished() { return noMoreInput_ && published_; }

facebook::velox::exec::BlockingReason ArtifactBuildOperator::isBlocked(
    facebook::velox::ContinueFuture* /*future*/) {
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

std::unique_ptr<facebook::velox::exec::Operator>
ArtifactBuildTranslator::toOperator(
    facebook::velox::exec::DriverCtx* ctx, int32_t id,
    const facebook::velox::core::PlanNodePtr& node) {
  auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(node);
  if (!artifact) {
    return nullptr;
  }
  return std::make_unique<ArtifactBuildOperator>(id, ctx, artifact);
}

void registerArtifactBuildOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<ArtifactBuildTranslator>());
}

}  // namespace pangrova::velox
