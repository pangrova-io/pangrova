// PangrovaVeloxConnector: Pangrova's Velox Connector and DataSource implementation.
// Uses Velox native readers (ParquetReader/DwrfReader) for zero-conversion
// data reading. Supports predicate pushdown via RowReaderOptions for row
// group statistics-based pruning. Iceberg delete filter is applied as
// post-processing on RowVector + SelectivityVector level.
#include "velox/connector/PangrovaVeloxConnector.h"
#include "resource/TaskBudgetReadFile.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <unordered_map>

// Structured logging for predicate pushdown observability.
#include "observability/PangrovaLog.h"

// Centralized feature detection — single source of truth for optional
// Velox component availability. Do NOT redefine these macros locally.
#include "compat/NativeFeatureDetection.h"

#include "split/PathUtils.h"
#include "delete/io/DeleteFileReader.h"
#include "delete/RowDeleteFilterFactory.h"
#include "delete/DeletionVectorRowFilter.h"
#include "delete/PaimonSnapshotDeletionFilter.h"
#include "delete/io/DeletionVectorReader.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/ColumnAssignmentDataSource.h"
#include "velox/connector/PartitionColumnInjector.h"
#include "velox/connector/SchemaEvolution.h"
#include "velox/connector/PhysicalFileSchema.h"
#include "velox/connector/SourceObjectIdentityInjector.h"
#include "velox/connector/DynamicFilterEvaluator.h"
#include "velox/filter/PendingFilterRegistry.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

#if PANGROVA_HAS_VELOX_DWIO
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#if PANGROVA_HAS_CACHED_BUFFERED_INPUT && PANGROVA_HAS_VELOX_ASYNC_CACHE
#include "velox/common/caching/FileIds.h"
#endif
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"
#if __has_include("velox/dwio/parquet/reader/ParquetReader.h") && __has_include(<thrift/TApplicationException.h>)
#include "velox/dwio/parquet/reader/ParquetReader.h"
#define PANGROVA_HAS_VELOX_PARQUET_READER 1
#else
#define PANGROVA_HAS_VELOX_PARQUET_READER 0
#endif
#if __has_include("velox/dwio/dwrf/reader/DwrfReader.h") && __has_include("velox/dwio/dwrf/proto/dwrf_proto.pb.h")
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#define PANGROVA_HAS_VELOX_DWRF_READER 1
#else
#define PANGROVA_HAS_VELOX_DWRF_READER 0
#endif
#endif

// Velox filter types for SubfieldFilter decomposition.
#include "velox/type/Filter.h"
#include "velox/core/Expressions.h"
#include "velox/connector/FilterDecomposer.h"

namespace pangrova::velox {

namespace {

class LanceMergeBatchReader final : public merge::RowReaderWrapper {
 public:
  LanceMergeBatchReader(std::unique_ptr<pangrova::lance::LanceBatchReader> reader,
      std::unique_ptr<delete_filter::RowDeleteFilter> deletion)
      : reader_(std::move(reader)), deletion_(std::move(deletion)) {}
  facebook::velox::RowVectorPtr next(uint64_t size,
      facebook::velox::memory::MemoryPool* pool) override {
    while (reader_->hasNext()) {
      auto batch = reader_->next(size);
      if (!batch || !deletion_) return batch;
      facebook::velox::SelectivityVector rows(batch->size());
      deletion_->apply(*batch, rows, static_cast<int64_t>(reader_->lastBatchRowStart()));
      const auto count = rows.countSelected();
      if (count == 0) continue;
      if (count == batch->size()) return batch;
      auto indices = facebook::velox::allocateIndices(count, pool);
      auto* raw = indices->asMutable<facebook::velox::vector_size_t>();
      facebook::velox::vector_size_t next = 0;
      rows.applyToSelected([&](auto row) { raw[next++] = row; });
      auto children = batch->children();
      for (auto& child : children) child = facebook::velox::BaseVector::wrapInDictionary(nullptr, indices, count, child);
      return std::make_shared<facebook::velox::RowVector>(pool, batch->type(), nullptr, count, std::move(children));
    }
    return nullptr;
  }
  bool hasNext() const override { return reader_->hasNext(); }
 private:
  std::unique_ptr<pangrova::lance::LanceBatchReader> reader_;
  std::unique_ptr<delete_filter::RowDeleteFilter> deletion_;
};

// Determine Velox file format from split config enum.
#if PANGROVA_HAS_VELOX_DWIO
facebook::velox::dwio::common::FileFormat resolveFileFormat(
    split::FileFormatType format) {
  switch (format) {
    case split::FileFormatType::PARQUET:
      return facebook::velox::dwio::common::FileFormat::PARQUET;
    case split::FileFormatType::ORC:
      return facebook::velox::dwio::common::FileFormat::DWRF;
    case split::FileFormatType::LANCE:
      throw pangrova::executor::NativeUnsupportedFeatureException(
          "file_format:lance_via_dwio", "lance",
          "Lance files use LanceBatchReader, not Velox DWIO");
    case split::FileFormatType::METADATA_JSON:
      throw pangrova::executor::NativeUnsupportedFeatureException(
          "file_format:metadata_json_via_dwio", "metadata_json",
          "Metadata tables use the Native metadata reader, not Velox DWIO");
    case split::FileFormatType::METADATA_DIRECTORY:
      throw pangrova::executor::NativeUnsupportedFeatureException(
          "file_format:metadata_directory_via_dwio", "metadata_directory",
          "Directory metadata uses the Native branch reader, not Velox DWIO");
    case split::FileFormatType::METADATA_AVRO:
      throw pangrova::executor::NativeUnsupportedFeatureException(
          "file_format:metadata_avro_via_dwio", "metadata_avro",
          "Iceberg manifest metadata uses the Native Avro reader, not Velox DWIO");
  }
  throw pangrova::executor::NativeUnsupportedFeatureException(
      "file_format:unknown", "unknown", "");
}
#endif

}  // namespace

// --- VeloxBatchReader ---

#if PANGROVA_HAS_VELOX_DWIO
VeloxBatchReader::VeloxBatchReader(
    std::unique_ptr<facebook::velox::dwio::common::Reader> reader,
    std::unique_ptr<facebook::velox::dwio::common::RowReader> rowReader,
    facebook::velox::RowTypePtr outputType)
    : reader_(std::move(reader)),
      row_reader_(std::move(rowReader)),
      output_type_(std::move(outputType)) {}
#endif

facebook::velox::RowVectorPtr VeloxBatchReader::next(
    uint64_t size,
    facebook::velox::memory::MemoryPool* pool) {
#if PANGROVA_HAS_VELOX_DWIO
  if (exhausted_ || !row_reader_) {
    return nullptr;
  }
  auto result =
      facebook::velox::BaseVector::create(output_type_, 0, pool);
  uint64_t rowsRead = row_reader_->next(size, result);
  if (rowsRead == 0) {
    exhausted_ = true;
    return nullptr;
  }
  return std::dynamic_pointer_cast<facebook::velox::RowVector>(result);
#else
  return nullptr;
#endif
}

bool VeloxBatchReader::hasNext() const {
  return !exhausted_;
}

// --- PangrovaVeloxDataSource ---

PangrovaVeloxDataSource::PangrovaVeloxDataSource(
    facebook::velox::memory::MemoryPool* pool,
    const facebook::velox::RowTypePtr& outputType,
    const facebook::velox::RowTypePtr& scanSchema,
    const std::string& taskId,
    const std::string& planNodeId,
    std::shared_ptr<const facebook::velox::core::ITypedExpr> remainingFilter,
    int64_t preferredOutputBatchBytes,
    int64_t eagerFirstStripeThresholdBytes)
    : pool_(pool),
      output_type_(outputType),
      scan_schema_(scanSchema),
      eager_first_stripe_threshold_bytes_(eagerFirstStripeThresholdBytes),
      task_id_(taskId),
      plan_node_id_(planNodeId),
      remaining_filter_(std::move(remainingFilter)) {
  if (!scan_schema_) throw std::invalid_argument("Native scan requires its canonical logical base schema");
  // Compute initial adaptive batch size from output schema column widths,
  // using the operator-level preferred output batch bytes as the target.
  batch_sizer_.computeOptimalBatchSize(output_type_, preferredOutputBatchBytes);
}

void PangrovaVeloxDataSource::addSplit(
    std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) {
  auto pangrovaSplit =
      std::dynamic_pointer_cast<PangrovaConnectorSplit>(split);
  if (!pangrovaSplit) {
    throw std::invalid_argument("Native source requires its bound connector split");
  }

  const auto& splitConfig = pangrovaSplit->splitConfig;

  const bool physical_identity_requested =
      output_type_->containsChild(kPhysicalRowPositionColumnName) ||
      output_type_->containsChild(kSourceObjectIdColumnName);

  if ((splitConfig.row_position_filter_enabled || physical_identity_requested) &&
      splitConfig.paimon.has_value() && splitConfig.paimon->needs_merge) {
    throw std::runtime_error(
        "Physical source identity cannot follow a merge of distinct physical objects");
  }

  // Store file path for structured logging.
  current_file_path_ = splitConfig.file_path;

  // Populate partition values, schema evolution, delete filters,
  // changelog mode flag, and Paimon deletion vectors.
  prepareSplitState(splitConfig);

  // Reset merge reader from any previous split.
  merge_reader_.reset();
  metadata_reader_.reset();
  iceberg_manifest_reader_.reset();
  paimon_snapshot_reader_.reset();
  paimon_branch_reader_.reset();
  lance_reader_.reset();
#if PANGROVA_HAS_VELOX_DWIO
  velox_row_reader_.reset();
  velox_reader_.reset();
#endif

  // Reset batch calibration for the new split — different files may have
  // different actual row widths (e.g., different VARCHAR lengths).
  batch_calibrated_ = false;

  if (splitConfig.system_table.has_value()) {
    if (*splitConfig.system_table == split::SystemTableKind::ICEBERG_MANIFESTS) {
      if (splitConfig.file_format != split::FileFormatType::METADATA_AVRO) {
        throw std::runtime_error("Iceberg manifests split requires metadata Avro format");
      }
      iceberg_manifest_reader_ = std::make_unique<IcebergManifestListReader>(
          splitConfig.file_path, splitConfig.length,
          splitConfig.system_table_snapshot_id,
          splitConfig.system_table_iceberg_snapshot_sequence_number,
          splitConfig.system_table_iceberg_format_version,
          splitConfig.system_table_object_sha256,
          splitConfig.system_table_iceberg_partition_specs,
          output_type_, pool_);
    } else if (*splitConfig.system_table == split::SystemTableKind::PAIMON_BRANCHES) {
      if (splitConfig.file_format != split::FileFormatType::METADATA_DIRECTORY) {
        throw std::runtime_error("Paimon branches split requires directory format");
      }
      paimon_branch_reader_ = std::make_unique<PaimonBranchDirectoryReader>(
          splitConfig.file_path, splitConfig.system_table_branch_name,
          splitConfig.system_table_directory_mtime_ms,
          splitConfig.system_table_directory_device,
          splitConfig.system_table_directory_inode,
          splitConfig.system_table_directory_zone_offset_seconds,
          output_type_, pool_);
    } else if (splitConfig.file_format != split::FileFormatType::METADATA_JSON) {
      throw std::runtime_error("System table split requires metadata JSON format");
    } else if (*splitConfig.system_table == split::SystemTableKind::PAIMON_SNAPSHOTS ||
        *splitConfig.system_table == split::SystemTableKind::PAIMON_TAGS ||
        *splitConfig.system_table == split::SystemTableKind::PAIMON_OPTIONS ||
        *splitConfig.system_table == split::SystemTableKind::PAIMON_SCHEMAS) {
      paimon_snapshot_reader_ = std::make_unique<PaimonSnapshotJsonReader>(
          splitConfig.file_path, splitConfig.length,
          splitConfig.system_table_snapshot_id,
          splitConfig.system_table_object_sha256, output_type_, pool_,
          splitConfig.system_table_tag_name,
          splitConfig.system_table_schema_id,
          *splitConfig.system_table == split::SystemTableKind::PAIMON_SCHEMAS);
    } else {
      metadata_reader_ = std::make_unique<IcebergMetadataJsonReader>(
          splitConfig.file_path, splitConfig.length, *splitConfig.system_table,
          output_type_, pool_);
    }
    return;
  }

  // Paimon MOR merge: when needs_merge=true, create per-level readers
  // and feed them into a MergeReader. The single Velox reader is
  // not opened — next() pulls from merge_reader_ instead.
  if (splitConfig.paimon.has_value() &&
      splitConfig.paimon->needs_merge) {
    initializeMergeReader(splitConfig);
    return;
  }

  // Lance file format: dispatch to LanceBatchReader instead of Velox DWIO.
  // Supports mixed-format scans where some splits are Parquet/ORC and
  // others are Lance within the same table.
  if (splitConfig.file_format == split::FileFormatType::LANCE) {
    openLanceReader(splitConfig);
    return;
  }

  // COW / read-optimized path: when needs_merge=false the split is
  // fully compacted — read as a plain scan via openVeloxReader().
  openVeloxReader(splitConfig);
}

void PangrovaVeloxDataSource::prepareSplitState(
    const split::SplitConfig& config) {
  checkCancelled();
  file_evolution_output_type_.reset();
  physical_lance_file_ = config.physical_lance_file;
  current_data_object_id_ = config.data_object_id;
  current_source_id_ = config.table_name;
  current_source_snapshot_ = config.source_snapshot;
  source_object_identity_requested_ = false;
  for (size_t channel = 0; channel < output_type_->size(); ++channel) {
    if (!isSourceIdentityConstant(output_type_->nameOf(channel))) {
      continue;
    }
    if (output_type_->childAt(channel)->kind() !=
            facebook::velox::TypeKind::VARCHAR) {
      throw std::runtime_error(
          "Native source-object identity column must occur once and be VARCHAR");
    }
    source_object_identity_requested_ = true;
  }
  if (source_object_identity_requested_ && current_data_object_id_.empty()) {
    throw std::runtime_error(
        "Native source-object identity column requires split data_object_id");
  }

  // Store partition values for injection in next().
  // For Iceberg: uses SplitConfig.partition_values (identity + hidden
  // transforms pre-evaluated on Java side).
  // For Paimon: uses PaimonConfig.partition_values.
  if (config.paimon.has_value() &&
      !config.paimon->partition_values.empty()) {
    current_partition_values_ = config.paimon->partition_values;
  } else {
    current_partition_values_ = config.partition_values;
  }

  // Store schema evolution config for column mapping and type promotion
  // in next(). Shared by Iceberg and Paimon — both populate
  // SplitConfig.column_mappings and SplitConfig.type_promotions from
  // the SplitInfo proto SchemaEvolution message on the Java side.
  current_column_mappings_ = config.column_mappings;
  current_type_promotions_ = config.type_promotions;
  artifact_candidate_filter_enabled_ = config.row_position_filter_enabled;
  artifact_candidate_positions_.clear();
  artifact_candidate_positions_.insert(
      config.row_position_allowlist.begin(),
      config.row_position_allowlist.end());
  if (artifact_candidate_positions_.size() !=
      config.row_position_allowlist.size()) {
    throw std::runtime_error(
        "Artifact retrieval row-position allowlist contains duplicates");
  }
  materialized_retrieval_score_enabled_ =
      config.materialized_retrieval_score_enabled;
  materialized_retrieval_scores_ = config.materialized_retrieval_scores;
  if (materialized_retrieval_score_enabled_) {
    if (!artifact_candidate_filter_enabled_ ||
        materialized_retrieval_scores_.size() !=
            artifact_candidate_positions_.size()) {
      throw std::runtime_error(
          "Materialized retrieval scores differ from the candidate allowlist");
    }
    for (const auto& [position, score] : materialized_retrieval_scores_) {
      if (!artifact_candidate_positions_.contains(position) ||
          !std::isfinite(score)) {
        throw std::runtime_error(
            "Materialized retrieval score is non-finite or has no candidate");
      }
    }
  } else if (!materialized_retrieval_scores_.empty()) {
    throw std::runtime_error(
        "Materialized retrieval scores require an enabled score contract");
  }

  // Build Iceberg delete filter if applicable.
  // Clear any filters from a previous split first.
  delete_filters_.clear();
  if (config.iceberg.has_value()) {
    delete_filters_ = delete_filter::RowDeleteFilterFactory::create(
        config.iceberg.value(), config.file_path);
  }
  row_index_offset_ = 0;

  // Paimon changelog mode: when changelog_mode=true, the split's
  // Parquet file contains a _row_kind column (TINYINT: 0=+I, 1=-U,
  // 2=+U, 3=-D). The Velox reader reads _row_kind as a regular column
  // if the planner includes it in output_type_.
  changelog_mode_ = config.paimon.has_value() &&
                    config.paimon->changelog_mode;

  // Store required subfields for subfield pruning (Task 2.20).
  // When non-empty, createReaderPair() adds only the listed subfields
  // to the ScanSpec for nested columns, reducing I/O for wide structs.
  current_required_subfields_ = config.required_subfields;

  if (changelog_mode_) {
    auto rowKindIdx = output_type_->getChildIdxIfExists("_row_kind");
    if (!rowKindIdx.has_value()) {
      PANGROVA_LOG_WARN << "paimon_changelog_mode=misconfigured"
                      << " file_path=" << current_file_path_
                      << " output_type does not contain _row_kind column"
                      << " changelog semantics will be incorrect";
    } else {
      PANGROVA_LOG_INFO << "paimon_changelog_mode=active"
                      << " file_path=" << current_file_path_
                      << " _row_kind_column_index=" << rowKindIdx.value();
    }
  }

  // Paimon MOW deletion vector: when DV is enabled, read the DV file
  // and create a DeletionVectorRowFilter. The existing applyDeleteFilter()
  // in next() applies it automatically.
  if (config.paimon.has_value()) {
    loadPaimonDeletionVector(config);
  }
}

void PangrovaVeloxDataSource::loadPaimonDeletionVector(
    const split::SplitConfig& split) {
  protocol::v2::FileSplitDetail detail;
  if (!detail.ParseFromString(split.file_scan_detail)) {
    throw std::invalid_argument("Paimon split requires canonical physical file metadata");
  }
  if (detail.physical_file_scan().has_deletion_vector()) {
    delete_filters_.push_back(std::make_unique<delete_filter::PaimonSnapshotDeletionFilter>(
        detail.physical_file_scan().deletion_vector(), split.storage_binding));
  }
}

void PangrovaVeloxDataSource::createReaderPair(
    const std::string& filePath,
    split::FileFormatType fileFormat,
    std::unique_ptr<facebook::velox::dwio::common::Reader>& outReader,
    std::unique_ptr<facebook::velox::dwio::common::RowReader>& outRowReader,
    std::shared_ptr<facebook::velox::common::ScanSpec>* outScanSpec,
    facebook::velox::RowTypePtr requestedType) {
#if PANGROVA_HAS_VELOX_DWIO
  const auto& target_type = requestedType ? requestedType : output_type_;
  auto cleanPath = split::normalizeToS3Uri(
      split::stripFilePrefix(filePath));
  auto format = resolveFileFormat(fileFormat);

  facebook::velox::dwio::common::ReaderOptions readerOpts(pool_);
  readerOpts.setFileFormat(format);

  // IO executor for async prefetch. Velox uses this to overlap IO with
  // computation: while the current batch is being processed, the next row
  // group is prefetched in the background.
  auto ioExecutor =
      pangrova::jni::NativeEngineLifecycle::instance().ioExecutor();
  if (ioExecutor) {
    readerOpts.setIOExecutor(std::move(ioExecutor));
  }

  // Task 2.18 (CoalesceIO): Configure reader-level I/O coalescing for
  // small files. The Velox reader merges adjacent column chunk reads into
  // fewer, larger I/O requests when the gap between them is smaller than
  // maxCoalesceDistance. This reduces I/O round-trips by 50-70% for small
  // Parquet/ORC files with many columns.
  // - maxCoalesceDistance: merge reads separated by up to 512KB of gap
  // - maxCoalesceBytes: single coalesced read up to 8MB
  static constexpr int64_t kMaxCoalesceDistance = 512L * 1024;   // 512KB
  static constexpr int64_t kMaxCoalesceBytes = 8L * 1024 * 1024; // 8MB
  readerOpts.setMaxCoalesceDistance(kMaxCoalesceDistance);
  readerOpts.setMaxCoalesceBytes(kMaxCoalesceBytes);

  auto fileSystem = facebook::velox::filesystems::getFileSystem(cleanPath, nullptr);
  if (!fileSystem) {
    throw std::runtime_error(
        "No registered Velox filesystem for data object: " + cleanPath);
  }
  std::shared_ptr<facebook::velox::ReadFile> readFile(
      fileSystem->openFileForRead(cleanPath));
  if (read_budget_) {
    readFile = std::make_shared<resource::TaskBudgetReadFile>(
        std::move(readFile), read_budget_, cancellation_token_);
  }
  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> input;
#if PANGROVA_HAS_CACHED_BUFFERED_INPUT && PANGROVA_HAS_VELOX_ASYNC_CACHE
  auto* cache = facebook::velox::cache::AsyncDataCache::getInstance();
  if (cache) {
    // CachedBufferedInput uses these IDs as actual cache keys. Empty leases
    // carry kNoId and fail when a coalesced load retains the file reference.
    // Match Velox FileHandle's path and directory identity for the full reader
    // lifetime; scan-level tracking remains optional.
    const auto separator = cleanPath.rfind('/');
    const auto group = separator == std::string::npos
        ? cleanPath : cleanPath.substr(0, separator);
    auto ioStats = std::make_shared<facebook::velox::dwio::common::IoStatistics>();
    input = std::make_unique<facebook::velox::dwio::common::CachedBufferedInput>(
        readFile,
        facebook::velox::dwio::common::MetricsLog::voidLog(),
        facebook::velox::StringIdLease(facebook::velox::fileIds(), cleanPath),
        cache,
        std::shared_ptr<facebook::velox::cache::ScanTracker>(),
        facebook::velox::StringIdLease(facebook::velox::fileIds(), group),
        ioStats,
        std::shared_ptr<facebook::velox::IoStats>(),
        nullptr,
        readerOpts);
  }
#endif
  if (!input) {
    input = std::make_unique<facebook::velox::dwio::common::BufferedInput>(
        readFile, *pool_);
  }
  outReader =
      facebook::velox::dwio::common::getReaderFactory(format)
          ->createReader(std::move(input), readerOpts);

  if (source_object_identity_requested_) {
    for (const auto& name : outReader->rowType()->names()) {
      if (isSourceIdentityConstant(name) || name == kPhysicalRowPositionColumnName) {
        throw std::runtime_error("Physical source file shadows a reserved identity column");
      }
    }
  }
  facebook::velox::dwio::common::RowReaderOptions rowOpts;
  // Velox defaults Parquet timestamps to millisecond output precision. Keep
  // the file's microsecond and nanosecond fractions through scans and sorts.
  rowOpts.setTimestampPrecision(
      facebook::velox::TimestampPrecision::kNanoseconds);

  // Schema evolution: when column mappings are present, build a "reader type"
  // that uses source (physical) column names from the file schema. The Parquet
  // reader matches columns by name, so renamed columns would fail without this.
  // Post-processing (applyColumnMapping) renames back to target names.
  std::unordered_map<std::string, std::string> target_to_source;
  if (!requestedType) {
    for (const auto& cm : current_column_mappings_) {
      auto lower_target = cm.target_name;
      std::transform(lower_target.begin(), lower_target.end(),
                     lower_target.begin(), ::tolower);
      target_to_source[lower_target] = cm.source_name;
    }
  }
  std::vector<std::string> reader_names;
  std::vector<facebook::velox::TypePtr> reader_types;
  auto evolved_types = target_type->children();
  reader_names.reserve(target_type->size() + 1);
  reader_types.reserve(target_type->size() + 1);
  bool score_column_requested = false;
  for (size_t i = 0; i < target_type->size(); ++i) {
    auto col_name = target_type->nameOf(i);
    if (isSourceIdentityConstant(col_name)) {
      continue;
    }
    if (col_name == kMaterializedRetrievalScoreColumnName) {
      if (score_column_requested || target_type->childAt(i)->kind() !=
              facebook::velox::TypeKind::DOUBLE) {
        throw std::runtime_error(
            "Materialized retrieval score column is duplicated or not DOUBLE");
      }
      score_column_requested = true;
      continue;
    }
    auto lower_col = col_name;
    std::transform(lower_col.begin(), lower_col.end(),
                   lower_col.begin(), ::tolower);
    auto it = target_to_source.find(lower_col);
    auto physical_type = target_type->childAt(i);
    if (!requestedType) {
      for (const auto& mapping : current_column_mappings_) {
        if (mapping.target_name == col_name && mapping.source_id < 0) {
          col_name.clear();
          break;
        }
      }
      for (const auto& promotion : current_type_promotions_) {
        if (promotion.column_name == target_type->nameOf(i)) {
          physical_type = resolveSchemaEvolutionType(promotion.source_type);
          evolved_types[i] = physical_type;
          break;
        }
      }
    }
    if (col_name.empty()) continue;
    reader_names.push_back(
        it == target_to_source.end() ? col_name : it->second);
    reader_types.push_back(physical_type);
  }
  if (!requestedType) {
    file_evolution_output_type_ = facebook::velox::ROW(target_type->names(), std::move(evolved_types));
  }
  if (score_column_requested != materialized_retrieval_score_enabled_) {
    throw std::runtime_error(
        "Query plan and split disagree on materialized retrieval scoring");
  }
  auto readerType = facebook::velox::ROW(
      std::move(reader_names), std::move(reader_types));

  if (artifact_candidate_filter_enabled_ &&
      !readerType->containsChild(kPhysicalRowPositionColumnName)) {
    auto reader_names = readerType->names();
    auto reader_types = readerType->children();
    reader_names.emplace_back(kPhysicalRowPositionColumnName);
    reader_types.push_back(facebook::velox::BIGINT());
    readerType = facebook::velox::ROW(
        std::move(reader_names), std::move(reader_types));
  }
  current_reader_output_type_ = readerType;

  rowOpts.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          readerType));

  // Task 2.22 (Eager First Stripe): For small files, eagerly load the first
  // stripe/row-group metadata to avoid a second I/O round-trip. Large files
  // keep lazy loading to avoid blocking on metadata of stripes that may be
  // pruned by predicate pushdown.
  // L2-6: Threshold is now configurable (was hardcoded 1MB).
  auto fileSize = readFile->size();
  bool isSmallFile =
      fileSize > 0 && eager_first_stripe_threshold_bytes_ > 0 &&
      fileSize < static_cast<uint64_t>(eager_first_stripe_threshold_bytes_);
  rowOpts.setEagerFirstStripeLoad(isSmallFile);

  // Task 2.16 (Parquet Page Index): Enable column index filtering for
  // page-level data skipping. When SubfieldFilters are present on the
  // ScanSpec, the Velox Parquet reader uses ColumnIndex (page-level
  // min/max statistics) to skip non-matching pages (8KB-1MB granularity),
  // which is 100-1000x finer than row-group level (128MB). This flag
  // ensures the reader loads and evaluates page index metadata.
  // L3-1: Guarded by PANGROVA_VELOX_HAS_PAGE_INDEX. Enable via cmake:
  //   -DPANGROVA_VELOX_HAS_PAGE_INDEX=ON (when Velox version exposes API).
#ifdef PANGROVA_VELOX_HAS_PAGE_INDEX
  rowOpts.setColumnReaderStatisticsBasedFiltering(true);
#endif

  // Task 2.17 (ORC Bloom Filter): For ORC/DWRF format, enable bloom
  // filter reading so equality predicates can skip non-matching stripes.
  // ORC bloom filters provide 50-80% I/O reduction for point lookups.
  // L3-2: Guarded by PANGROVA_VELOX_HAS_BLOOM_FILTER. Enable via cmake:
  //   -DPANGROVA_VELOX_HAS_BLOOM_FILTER=ON (when Velox version exposes API).
#ifdef PANGROVA_VELOX_HAS_BLOOM_FILTER
  rowOpts.setUseBloomFilter(true);
#endif

  auto scanSpec =
      std::make_shared<facebook::velox::common::ScanSpec>("root");
  // Task 2.20 (Subfield Pruning): When required_subfields is populated,
  // only add the specified subfields for nested columns (STRUCT/MAP/ARRAY).
  // This avoids reading unnecessary subfields from wide nested types,
  // reducing I/O proportionally to the pruned subfield ratio.
  // When required_subfields is empty, add all fields recursively (default).
  for (facebook::velox::column_index_t i = 0; i < readerType->size(); ++i) {
    const auto& colName = readerType->nameOf(i);
    if (colName == kPhysicalRowPositionColumnName) {
      VELOX_USER_CHECK(
          readerType->childAt(i)->kind() ==
              facebook::velox::TypeKind::BIGINT,
          "Native physical row-position column must be BIGINT");
      auto* row_position = scanSpec->addField(colName, i);
      row_position->setColumnType(
          facebook::velox::common::ScanSpec::ColumnType::kRowIndex);
      continue;
    }
    auto it = current_required_subfields_.find(colName);
    if (it != current_required_subfields_.end() && !it->second.empty()) {
      // Pruned path: add only the top-level field without recursing into
      // all children. Velox ScanSpec with a non-recursive field entry
      // causes the reader to load only the column's metadata, and the
      // listed subfields are added individually below.
      auto* fieldSpec = scanSpec->addField(colName, i);
      for (const auto& subfield : it->second) {
        fieldSpec->addField(subfield, 0);
      }
    } else {
      // Full path: add all subfields recursively (no pruning).
      // For ARRAY/MAP types, we cannot call addFieldRecursively on the
      // root scanSpec with the ARRAY/MAP type directly — that crashes
      // with SIGSEGV in ListColumnReader::ListColumnReader. Instead, we
      // add the top-level field via addField, then use Velox's dedicated
      // addArrayElementFieldRecursively / addMapKey/ValueFieldRecursively
      // APIs on the child spec. These set isArrayElementOrMapEntry_ and
      // use kNoChannel, which is what the ListColumnReader expects.
      auto childType = readerType->childAt(i);
      if (childType->isArray()) {
        auto* arraySpec = scanSpec->addField(colName, i);
        arraySpec->addArrayElementFieldRecursively(*childType->childAt(0));
      } else if (childType->isMap()) {
        auto* mapSpec = scanSpec->addField(colName, i);
        mapSpec->addMapKeyFieldRecursively(*childType->childAt(0));
        mapSpec->addMapValueFieldRecursively(*childType->childAt(1));
      } else {
        scanSpec->addFieldRecursively(colName, *childType, i);
      }
    }
  }
  rowOpts.setScanSpec(scanSpec);

  if (outScanSpec) {
    *outScanSpec = scanSpec;
  }

  checkCancelled();
  outRowReader = outReader->createRowReader(rowOpts);
  checkCancelled();
#else
  throw std::runtime_error(
      "Velox DWIO readers not available. Cannot open: " + filePath);
#endif
}

void PangrovaVeloxDataSource::openVeloxReader(
    const split::SplitConfig& split) {
#if PANGROVA_HAS_VELOX_DWIO
  reader_exhausted_ = false;
  velox_reader_.reset();
  velox_row_reader_.reset();

  auto filePath = split::normalizeToS3Uri(
      split::stripFilePrefix(split.file_path));

  std::shared_ptr<facebook::velox::common::ScanSpec> scanSpec;
  try {
    createReaderPair(
        split.file_path, split.file_format,
        velox_reader_, velox_row_reader_, &scanSpec);
  } catch (const std::exception& e) {
    PANGROVA_LOG_ERROR << "velox_reader_open_failed"
                     << " file_path=" << filePath
                     << " file_format=" << split::fileFormatName(split.file_format)
                     << " error_detail=" << e.what();
    throw std::runtime_error(
        "Failed to open file with Velox native reader: path=" + filePath +
        ", format=" + split::fileFormatName(split.file_format) +
        ", error=" + e.what());
  }

  // Predicate pushdown to DWIO reader level.
  // Decompose simple comparison predicates from the remaining filter into
  // per-column SubfieldFilters on the ScanSpec. This enables the Parquet/DWRF
  // reader to skip entire row groups whose column statistics prove no
  // matching rows. Unsupported predicates are left to the FilterNode above.
  if (remaining_filter_ && scanSpec) {
    int pushedFilters = decomposeFilterToScanSpec(
        remaining_filter_, scanSpec, output_type_);
    PANGROVA_LOG_INFO << "predicate_pushdown=active"
                    << " file_path=" << filePath
                    << " filter_type=" << remaining_filter_->type()->toString()
                    << " pushed_subfield_filters=" << pushedFilters
                    << " filter_node=active";
  }

  // L2-1: Store the ScanSpec for dynamic filter pushdown later.
  current_scan_spec_ = scanSpec;
  dynamic_filters_pushed_to_scan_ = false;
#else
  reader_exhausted_ = true;
  throw std::runtime_error(
      "Velox DWIO readers not available. Cannot read file: path=" +
      split.file_path + ", format=" +
      split::fileFormatName(split.file_format));
#endif
}

void PangrovaVeloxDataSource::openLanceReader(
    const split::SplitConfig& split) {
  reader_exhausted_ = false;
#if PANGROVA_HAS_VELOX_DWIO
  velox_reader_.reset();
  velox_row_reader_.reset();
#endif
  lance_reader_.reset();

  const auto& filePath = split.file_path;
  uint64_t version = 0;
  const auto& snapshot = split.source_snapshot;
  const auto [end, error] = std::from_chars(
      snapshot.data(), snapshot.data() + snapshot.size(), version);
  if (error != std::errc{} || end != snapshot.data() + snapshot.size() ||
      version == 0) {
    throw std::invalid_argument(
        "Lance split requires a non-zero decimal source_snapshot version");
  }

  std::unordered_map<std::string, const split::ColumnMappingConfig*>
      mappingsByTarget;
  for (const auto& mapping : current_column_mappings_) {
    auto target = mapping.target_name;
    std::transform(target.begin(), target.end(), target.begin(), ::tolower);
    mappingsByTarget[target] = &mapping;
  }
  std::unordered_map<std::string, const split::TypePromotionConfig*>
      promotionsByColumn;
  for (const auto& promotion : current_type_promotions_) {
    auto column = promotion.column_name;
    std::transform(column.begin(), column.end(), column.begin(), ::tolower);
    promotionsByColumn[column] = &promotion;
  }

  std::vector<std::string> physicalNames;
  std::vector<facebook::velox::TypePtr> physicalTypes;
  std::vector<std::string> dataNames;
  std::vector<facebook::velox::TypePtr> dataTypes;
  for (uint32_t index = 0; index < output_type_->size(); ++index) {
    const auto& targetName = output_type_->nameOf(index);
    if (targetName == kPhysicalRowPositionColumnName || isSourceIdentityConstant(targetName)) {
      continue;
    }
    if (current_partition_values_.contains(targetName)) {
      continue;
    }

    auto normalizedTarget = targetName;
    std::transform(
        normalizedTarget.begin(), normalizedTarget.end(),
        normalizedTarget.begin(), ::tolower);
    auto sourceName = targetName;
    bool present = true;
    if (auto mapping = mappingsByTarget.find(normalizedTarget);
        mapping != mappingsByTarget.end()) {
      sourceName = mapping->second->source_name;
      present = mapping->second->source_id >= 0;
    }

    auto sourceType = output_type_->childAt(index);
    if (auto promotion = promotionsByColumn.find(normalizedTarget);
        promotion != promotionsByColumn.end()) {
      sourceType = resolveSchemaEvolutionType(promotion->second->source_type);
    }
    dataNames.push_back(targetName);
    dataTypes.push_back(sourceType);
    if (!present) continue;
    if (std::find(physicalNames.begin(), physicalNames.end(), sourceName) !=
        physicalNames.end()) {
      throw std::runtime_error(
          "Lance physical projection contains duplicate column '" +
          sourceName + "'");
    }
    physicalNames.push_back(sourceName);
    physicalTypes.push_back(sourceType);
  }
  if (!split.physical_lance_file &&
      (output_type_->containsChild(kPhysicalRowPositionColumnName) || artifact_candidate_filter_enabled_)) {
    physicalNames.emplace_back(kPhysicalRowPositionColumnName);
    physicalTypes.push_back(facebook::velox::BIGINT());
  }
  auto physicalType = facebook::velox::ROW(physicalNames, physicalTypes);
  lance_data_output_type_ =
      facebook::velox::ROW(std::move(dataNames), std::move(dataTypes));

  try {
    lance_reader_ = std::make_unique<pangrova::lance::LanceBatchReader>(
        pool_, physicalType, filePath, version, physicalNames,
        split.storage_binding, split.file_scan_detail, split.physical_lance_file ? split.length : 0);
    current_reader_output_type_ = lance_reader_->outputType();
  } catch (const std::exception& e) {
    PANGROVA_LOG_ERROR << "lance_reader_open_failed"
                     << " file_path=" << filePath
                     << " error_detail=" << e.what();
    throw std::runtime_error(
        "Failed to open Lance file: path=" + filePath +
        ", error=" + e.what());
  }

  // remaining_filter_ is intentionally not serialized to Lance SQL. Velox
  // expression toString() has no syntax contract with Lance/DataFusion. The
  // canonical relational FilterNode remains responsible for exact filtering.
}

void PangrovaVeloxDataSource::initializeMergeReader(
    const split::SplitConfig& split) {
#if PANGROVA_HAS_VELOX_DWIO
  const auto& paimon = split.paimon.value();
  auto merge_config = paimon;
  merge_reader_output_type_ = output_type_;
  {
    auto names = output_type_->names();
    auto types = output_type_->children();
    for (const auto& key : paimon.primary_keys) {
      if (output_type_->containsChild(key)) continue;
      if (!scan_schema_->containsChild(key)) throw std::invalid_argument("Paimon primary key absent from canonical base schema");
      names.push_back(key);
      types.push_back(scan_schema_->childAt(scan_schema_->getChildIdx(key)));
    }
    for (auto iterator = merge_config.aggregation_specs.begin(); iterator != merge_config.aggregation_specs.end();) {
      if (!scan_schema_->containsChild(iterator->first)) throw std::invalid_argument("Paimon aggregation field absent from canonical base schema");
      if (!output_type_->containsChild(iterator->first)) iterator = merge_config.aggregation_specs.erase(iterator);
      else ++iterator;
    }
    names.emplace_back("_SEQUENCE_NUMBER");
    types.push_back(facebook::velox::BIGINT());
    names.emplace_back("_VALUE_KIND");
    types.push_back(facebook::velox::TINYINT());
    merge_reader_output_type_ = facebook::velox::ROW(std::move(names), std::move(types));
    merge_config.sequence_column = "_SEQUENCE_NUMBER";
  }

  // Create the merge reader from factory (resolves engine type).
  try {
    merge_reader_ =
        merge::MergeReaderFactory::create(merge_config, merge_reader_output_type_);
  } catch (const pangrova::executor::NativeUnsupportedFeatureException&) {
    // Re-throw NativeUnsupportedFeatureException as-is for fallback reporting.
    throw;
  } catch (const std::exception& e) {
    PANGROVA_LOG_ERROR << "merge_reader_init_failed"
                     << " file_path=" << current_file_path_
                     << " merge_engine=" << paimon.merge_engine
                     << " error_detail=" << e.what();
    throw std::runtime_error(
        "Failed to create merge reader: engine=" + paimon.merge_engine +
        ", error=" + e.what());
  }

  if (!merge_reader_) {
    throw std::logic_error("Paimon MOR admission must produce exactly one merge reader");
  }

  // Create one RowReaderWrapper per LSM level file.
  // Apply predicate pushdown to each per-level reader's ScanSpec when a
  // remaining filter is available. This enables row group pruning during
  // MOR merge reads, avoiding full-file scans when WHERE conditions can
  // eliminate row groups via column statistics.
  std::vector<std::unique_ptr<merge::RowReaderWrapper>> readers;
  for (const auto& level : paimon.lsm_levels) {
    for (const auto& file : level.files) {
      const auto& path = file.file_path;
      try {
        PhysicalFileSchema schema(merge_reader_output_type_, file.column_mappings, file.type_promotions);
        const auto& physical_type = schema.physicalType();
        if (file.file_format == split::FileFormatType::LANCE) {
          protocol::v2::FileSplitDetail read;
          if (!read.ParseFromString(file.file_scan_detail)) throw std::invalid_argument("Malformed physical file read unit");
          std::unique_ptr<delete_filter::RowDeleteFilter> deletion;
          if (read.physical_file_scan().has_deletion_vector()) {
            deletion = std::make_unique<delete_filter::PaimonSnapshotDeletionFilter>(
                read.physical_file_scan().deletion_vector(), split.storage_binding);
          }
          readers.push_back(std::make_unique<EvolvedMergeBatchReader>(
              std::make_unique<LanceMergeBatchReader>(
                  std::make_unique<pangrova::lance::LanceBatchReader>(pool_, physical_type, path,
                      paimon.snapshot_id, physical_type->names(), split.storage_binding,
                      file.file_scan_detail, file.file_size), std::move(deletion)), std::move(schema)));
          continue;
        }
        std::unique_ptr<facebook::velox::dwio::common::Reader> reader;
        std::unique_ptr<facebook::velox::dwio::common::RowReader> rowReader;
        std::shared_ptr<facebook::velox::common::ScanSpec> scanSpec;
        createReaderPair(path, file.file_format, reader, rowReader, &scanSpec, physical_type);
        // Filtering an update before primary-key merge can resurrect older rows.
        // Keep non-key predicates in the relational FilterNode after the merge.
        readers.push_back(std::make_unique<EvolvedMergeBatchReader>(
            std::make_unique<VeloxBatchReader>(std::move(reader), std::move(rowReader), physical_type),
            std::move(schema)));
      } catch (const std::exception& e) {
        PANGROVA_LOG_ERROR << "merge_level_file_open_failed"
                         << " file_path=" << path
                         << " merge_engine=" << paimon.merge_engine
                         << " error_detail=" << e.what();
        throw std::runtime_error(
            "Failed to open LSM level file for merge: path=" + path +
            ", error=" + e.what());
      }
    }
  }

  if (readers.empty()) {
    merge_reader_.reset();
    reader_exhausted_ = true;
    return;
  }

  merge_reader_->initialize(std::move(readers));
#else
  throw std::runtime_error(
      "Velox DWIO readers not available for Paimon MOR merge.");
#endif
}

std::optional<facebook::velox::RowVectorPtr>
PangrovaVeloxDataSource::nextFromMergeReader(uint64_t batchSize) {
  if (!merge_reader_ || !merge_reader_->hasNext()) {
    merge_reader_.reset();
    // Return nullptr (not nullopt!) to signal split completion.
    // Velox DataSource protocol: nullopt = blocked waiting for connector,
    // nullptr (optional with null value) = split finished / EOF.
    return facebook::velox::RowVectorPtr(nullptr);
  }

  auto rowVector = merge_reader_->next(batchSize, pool_);
  if (!rowVector || rowVector->size() == 0) {
    merge_reader_.reset();
    return facebook::velox::RowVectorPtr(nullptr);
  }

  completed_rows_ += rowVector->size();
  completed_bytes_ += rowVector->estimateFlatSize();
  if (rowVector->childrenSize() > output_type_->size()) {
    auto children = rowVector->children();
    children.resize(output_type_->size());
    rowVector = std::make_shared<facebook::velox::RowVector>(pool_, output_type_, nullptr,
        rowVector->size(), std::move(children));
  }
  return rowVector;
}

std::optional<facebook::velox::RowVectorPtr> PangrovaVeloxDataSource::next(
    uint64_t size,
    facebook::velox::ContinueFuture& future) {
  checkCancelled();
#if PANGROVA_HAS_VELOX_DWIO
  // Use adaptive batch size, respecting Velox's requested upper bound.
  auto effectiveSize =
      static_cast<uint64_t>(batch_sizer_.getCurrentBatchSize());
  auto batchSize = std::min(size, effectiveSize);

  if (metadata_reader_) {
    auto rowVector = metadata_reader_->next(batchSize);
    checkCancelled();
    if (!rowVector) {
      metadata_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    completed_rows_ += rowVector->size();
    completed_bytes_ += rowVector->estimateFlatSize();
    return rowVector;
  }
  if (iceberg_manifest_reader_) {
    auto rowVector = iceberg_manifest_reader_->next(batchSize);
    checkCancelled();
    if (!rowVector) {
      iceberg_manifest_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    completed_rows_ += rowVector->size();
    completed_bytes_ += rowVector->estimateFlatSize();
    return rowVector;
  }
  if (paimon_snapshot_reader_) {
    auto rowVector = paimon_snapshot_reader_->next(batchSize);
    checkCancelled();
    if (!rowVector) {
      paimon_snapshot_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    completed_rows_ += rowVector->size();
    completed_bytes_ += rowVector->estimateFlatSize();
    return rowVector;
  }
  if (paimon_branch_reader_) {
    auto rowVector = paimon_branch_reader_->next(batchSize);
    checkCancelled();
    if (!rowVector) {
      paimon_branch_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    completed_rows_ += rowVector->size();
    completed_bytes_ += rowVector->estimateFlatSize();
    return rowVector;
  }

  // Paimon MOR merge path: pull from merge reader when present.
  // Each immutable file was evolved before primary-key merge. Reapplying one
  // primary file's mapping here could misbind a newly-added reused column name.
  if (merge_reader_) {
    auto merged = nextFromMergeReader(batchSize);
    checkCancelled();
    if (!merged.value()) {
      // Merge reader exhausted — propagate split completion signal (nullptr).
      return merged;
    }
    auto rowVector = merged.value();
    return injectPartitionColumns(rowVector, current_partition_values_, output_type_, pool_);
  }

  // Lance file format path: pull from Lance reader when present.
  // After read, apply partition injection and schema evolution (same as
  // Parquet/ORC path). This enables mixed-format scans where some splits
  // are Parquet and others are Lance within the same table.
  if (lance_reader_) {
    if (!lance_reader_->hasNext()) {
      lance_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    auto rowVector = lance_reader_->next(batchSize);
    checkCancelled();
    if (!rowVector || rowVector->size() == 0) {
      lance_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    completed_rows_ += rowVector->size();
    completed_bytes_ += rowVector->estimateFlatSize();
    facebook::velox::VectorPtr datasetPositions;
    if (!physical_lance_file_) {
      auto channel = rowVector->type()->asRow().getChildIdxIfExists(kPhysicalRowPositionColumnName);
      if (channel) datasetPositions = rowVector->childAt(*channel);
    }
    rowVector = applyLancePostProcessing(std::move(rowVector));
    rowVector = applyLanceIdentity(std::move(rowVector), std::move(datasetPositions));
    if (!task_id_.empty() && !plan_node_id_.empty()) {
      auto pending = PendingFilterRegistry::instance().drain(task_id_, plan_node_id_);
      std::lock_guard<std::mutex> lock(dynamic_filter_mutex_);
      for (auto& entry : pending) dynamic_filters_.emplace_back(entry.column_index, std::move(entry.filter));
    }
    if (!rowVector) return std::dynamic_pointer_cast<facebook::velox::RowVector>(
        facebook::velox::BaseVector::create(output_type_, 0, pool_));
    return applyDynamicFilters(std::move(rowVector));
  }

  if (reader_exhausted_ || !velox_row_reader_) {
    // Reader already exhausted or not initialized — signal split completion.
    // Velox DataSource protocol: nullptr (optional with null value) = split
    // finished / EOF; nullopt (empty optional) = blocked waiting for connector.
    return facebook::velox::RowVectorPtr(nullptr);
  }

  // Read directly from Velox native reader — produces RowVector with
  // zero Arrow conversion overhead.
  // L2-7: Measure IO latency to feed AdaptiveBatchSizer.
  auto io_start = std::chrono::steady_clock::now();
  auto result = facebook::velox::BaseVector::create(
      current_reader_output_type_ ? current_reader_output_type_ : output_type_,
      0,
      pool_);
  uint64_t rowsRead = velox_row_reader_->next(batchSize, result);
  checkCancelled();
  auto io_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - io_start).count();

  if (rowsRead == 0) {
    reader_exhausted_ = true;
    // Reader produced zero rows — split is done. Signal split completion.
    // Velox DataSource protocol: nullptr (optional with null value) = split
    // finished / EOF; nullopt (empty optional) = blocked waiting for connector.
    return facebook::velox::RowVectorPtr(nullptr);
  }

  auto rowVector =
      std::dynamic_pointer_cast<facebook::velox::RowVector>(result);
  if (!rowVector) {
    throw std::runtime_error("Native source reader returned a non-row vector");
  }

  if (source_object_identity_requested_) {
    auto requested_reader_names = output_type_->names();
    auto requested_reader_types = output_type_->children();
    if (materialized_retrieval_score_enabled_) {
      const auto score_channel = output_type_->getChildIdx(
          kMaterializedRetrievalScoreColumnName);
      requested_reader_names.erase(requested_reader_names.begin() + score_channel);
      requested_reader_types.erase(requested_reader_types.begin() + score_channel);
    }
    if (artifact_candidate_filter_enabled_ &&
        !output_type_->containsChild(kPhysicalRowPositionColumnName)) {
      requested_reader_names.emplace_back(kPhysicalRowPositionColumnName);
      requested_reader_types.push_back(facebook::velox::BIGINT());
    }
    rowVector = injectPhysicalSourceIdentity(
        rowVector, {current_source_id_, current_source_snapshot_, current_data_object_id_},
        facebook::velox::ROW(
            std::move(requested_reader_names),
            std::move(requested_reader_types)),
        pool_);
  }

  completed_rows_ += rowVector->size();
  // Estimate bytes from the RowVector.
  auto flatSize = rowVector->estimateFlatSize();
  completed_bytes_ += flatSize;

  // Calibrate batch sizer with actual row width from the first batch.
  // Replaces the schema-based estimate (which uses fixed 32 bytes for
  // VARCHAR) with the real average row width observed in data.
  if (!batch_calibrated_) {
    batch_sizer_.calibrate(
        static_cast<int64_t>(flatSize),
        static_cast<int64_t>(rowVector->size()));
    batch_calibrated_ = true;
  }

  // Heuristic memory pressure check with hysteresis: halve batch size
  // above high watermark, recover below low watermark, hold steady in
  // the dead zone to prevent oscillation.
  auto* parent = pool_->parent();
  if (parent && parent->capacity() > 0) {
    double usage = static_cast<double>(parent->reservedBytes()) /
                   static_cast<double>(parent->capacity());
    batch_sizer_.adjustForMemoryPressure(usage);
  }

  // L2-7: IO latency-aware batch size scaling. High-latency storage (S3/GCS)
  // benefits from larger batches to amortize per-request overhead.
  batch_sizer_.adjustForIOLatency(io_elapsed_ms);

  // Apply dynamic filters (runtime bloom/range filters from hash join build).
  // First drain any externally injected filters from the PendingFilterRegistry
  // (Java → JNI → VeloxTaskRunner path). Then apply all accumulated filters.
  //
  // IMPORTANT: Delete filters (position delete, equality delete, deletion
  // vectors) MUST be applied BEFORE dynamic filters. Position delete filters
  // use row_index_offset_ to map file-level row numbers to local vector
  // indices — this mapping requires the vector to be the raw reader output
  // with rows in file order. Dynamic filters produce dictionary-wrapped
  // vectors where local indices no longer correspond to file positions.
  //
  // Ordering: read → delete filter → dynamic filter → post-processing.

  // Apply Iceberg/Paimon delete filter on the raw reader output first.
  // row_index_offset_ tracks the file-level position and must advance
  // by the original batch size (before any filtering).
  auto originalBatchSize = rowVector->size();
  if (artifact_candidate_filter_enabled_) {
    // The immutable index was built from the exact serialized split (including
    // delete metadata) and contains only live row locators. Reapplying a
    // position-delete filter after candidate compaction would use local batch
    // offsets and corrupt physical identity, so candidate filtering subsumes
    // delete filtering for this exact source snapshot.
    rowVector = applyArtifactCandidateFilter(std::move(rowVector));
    row_index_offset_ += originalBatchSize;
    if (!rowVector || rowVector->size() == 0) {
      auto emptyVector = facebook::velox::BaseVector::create(
          output_type_, 0, pool_);
      return std::dynamic_pointer_cast<facebook::velox::RowVector>(emptyVector);
    }
  } else if (!delete_filters_.empty()) {
    rowVector = applyDeleteFilter(rowVector, originalBatchSize);
    if (!rowVector || rowVector->size() == 0) {
      // All rows deleted — return empty vector (not nullopt, which
      // signals EOF to Velox).
      auto emptyVector = facebook::velox::BaseVector::create(
          output_type_, 0, pool_);
      return std::dynamic_pointer_cast<facebook::velox::RowVector>(
          emptyVector);
    }
  } else {
    // No delete filters — still advance the offset for consistency.
    row_index_offset_ += originalBatchSize;
  }

  // Now apply dynamic filters on the (possibly delete-filtered) vector.
  // Dynamic filter column indices use output_type_ which matches the raw
  // reader schema. After delete filter compaction (dictionary wrapping),
  // column structure is preserved — only row count changes.
  if (!task_id_.empty() && !plan_node_id_.empty()) {
    auto pending = PendingFilterRegistry::instance().drain(
        task_id_, plan_node_id_);
    if (!pending.empty()) {
      std::lock_guard<std::mutex> lock(dynamic_filter_mutex_);
      for (auto& entry : pending) {
        dynamic_filters_.emplace_back(entry.column_index, std::move(entry.filter));
      }

      // L2-1: Push dynamic filters to ScanSpec for reader-level row-group
      // skipping. This is in addition to the row-level evaluation below.
      // The reader uses column statistics to skip entire row groups where
      // the filter cannot match, reducing IO by 50-90%.
      if (current_scan_spec_ && velox_row_reader_ &&
          !dynamic_filters_pushed_to_scan_) {
        for (const auto& [colIdx, filter] : dynamic_filters_) {
          if (colIdx < output_type_->size()) {
            auto colName = output_type_->nameOf(colIdx);
            auto* fieldSpec = current_scan_spec_->getOrCreateChild(
                facebook::velox::common::Subfield(colName));
            // Only push if the column doesn't already have a static filter
            // (from predicate pushdown). setFilter() replaces, so we avoid
            // clobbering the existing filter.
            if (fieldSpec && !fieldSpec->filter()) {
              fieldSpec->setFilter(filter);
            }
          }
        }
        velox_row_reader_->resetFilterCaches();
        dynamic_filters_pushed_to_scan_ = true;
      }
    }
  }
  rowVector = applyDynamicFilters(rowVector);
  if (!rowVector || rowVector->size() == 0) {
    auto emptyVector = facebook::velox::BaseVector::create(
        output_type_, 0, pool_);
    return std::dynamic_pointer_cast<facebook::velox::RowVector>(
        emptyVector);
  }

  return applyPostProcessing(std::move(rowVector));
#else
  // No DWIO support — signal split completion immediately.
  return facebook::velox::RowVectorPtr(nullptr);
#endif
}

facebook::velox::RowVectorPtr
PangrovaVeloxDataSource::applyArtifactCandidateFilter(
    facebook::velox::RowVectorPtr rowVector) {
  if (!artifact_candidate_filter_enabled_ || !rowVector) {
    return rowVector;
  }
  auto reader_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      rowVector->type());
  const bool output_has_position =
      output_type_->containsChild(kPhysicalRowPositionColumnName);
  // The score is synthesized here. The position is supplied by DWIO and may
  // already be a requested output field; only an internal-only position adds
  // a trailing reader column.
  const size_t expected_reader_size = output_type_->size() -
      (materialized_retrieval_score_enabled_ ? 1 : 0) +
      (output_has_position ? 0 : 1);
  if (!reader_type || reader_type->size() != expected_reader_size ||
      std::count(reader_type->names().begin(), reader_type->names().end(),
          kPhysicalRowPositionColumnName) != 1) {
    throw std::runtime_error(
        "Artifact retrieval reader did not produce the physical row-position column");
  }

  const auto position_channel = reader_type->getChildIdx(kPhysicalRowPositionColumnName);
  if (reader_type->childAt(position_channel)->kind() != facebook::velox::TypeKind::BIGINT ||
      (!output_has_position && position_channel != reader_type->size() - 1)) {
    throw std::runtime_error(
        "Artifact retrieval physical row-position column has an invalid type or layout");
  }
  const size_t output_reader_size = reader_type->size() - (output_has_position ? 0 : 1);
  facebook::velox::DecodedVector positions(*rowVector->childAt(position_channel));
  facebook::velox::SelectivityVector selected(rowVector->size(), false);
  for (facebook::velox::vector_size_t row = 0; row < rowVector->size(); ++row) {
    if (positions.isNullAt(row)) {
      throw std::runtime_error(
          "Artifact retrieval physical row position must not be null");
    }
    const int64_t value = positions.valueAt<int64_t>(row);
    if (value < 0) {
      throw std::runtime_error(
          "Artifact retrieval physical row position must not be negative");
    }
    if (artifact_candidate_positions_.contains(static_cast<uint64_t>(value))) {
      selected.setValid(row, true);
    }
  }
  selected.updateBounds();
  const auto count = selected.countSelected();
  if (count == 0) {
    return std::dynamic_pointer_cast<facebook::velox::RowVector>(
        facebook::velox::BaseVector::create(output_type_, 0, pool_));
  }
  auto indices = facebook::velox::allocateIndices(count, pool_);
  auto* raw_indices = indices->asMutable<facebook::velox::vector_size_t>();
  facebook::velox::vector_size_t next = 0;
  selected.applyToSelected([&](auto row) { raw_indices[next++] = row; });
  std::vector<facebook::velox::VectorPtr> children(output_type_->size());
  size_t reader_channel = 0;
  for (size_t index = 0; index < output_type_->size(); ++index) {
    if (output_type_->nameOf(index) ==
        kMaterializedRetrievalScoreColumnName) {
      auto scores = facebook::velox::BaseVector::create(
          facebook::velox::DOUBLE(), count, pool_);
      auto* flat_scores = scores->asFlatVector<double>();
      for (facebook::velox::vector_size_t output_row = 0;
           output_row < count; ++output_row) {
        auto input_row = raw_indices[output_row];
        const auto position = static_cast<uint64_t>(
            positions.valueAt<int64_t>(input_row));
        auto score = materialized_retrieval_scores_.find(position);
        if (score == materialized_retrieval_scores_.end()) {
          throw std::runtime_error(
              "Selected artifact candidate has no materialized score");
        }
        flat_scores->set(output_row, score->second);
      }
      children[index] = std::move(scores);
      continue;
    }
    if (reader_channel >= output_reader_size ||
        (output_type_->nameOf(index) == kPhysicalRowPositionColumnName &&
            reader_channel != position_channel)) {
      throw std::runtime_error(
          "Artifact retrieval output column is absent from the reader schema");
    }
    children[index] = facebook::velox::BaseVector::wrapInDictionary(
        nullptr, indices, count, rowVector->childAt(reader_channel));
    ++reader_channel;
  }
  return std::make_shared<facebook::velox::RowVector>(
      pool_, output_type_, nullptr, count, std::move(children));
}

facebook::velox::RowVectorPtr PangrovaVeloxDataSource::applyPostProcessing(
    facebook::velox::RowVectorPtr rowVector) {
  // Inject partition column values for columns present in the output
  // schema but absent from the data file.
  if (!current_partition_values_.empty()) {
    rowVector = injectPartitionColumns(
        rowVector, current_partition_values_, output_type_, pool_);
  }

  // Apply Iceberg/Paimon schema evolution: column mapping and type promotion.
  // Both connectors share the same SchemaEvolution utility (R6.6, R13.4).
  // Java side populates SplitConfig.column_mappings and type_promotions from
  // the SchemaEvolution proto message — Iceberg via IcebergSplitManager field
  // ID comparison, Paimon via PaimonSplitManager DataFileMeta.schemaId() check.
  // The native side is connector-agnostic: it applies column reordering and
  // null-fill by field ID, then type promotion casts, regardless of source.
  if (!current_column_mappings_.empty()) {
    rowVector = applyColumnMapping(
        rowVector, current_column_mappings_,
        file_evolution_output_type_ ? file_evolution_output_type_ : output_type_, pool_);
  }
  if (!current_type_promotions_.empty()) {
    rowVector = applyTypePromotion(
        rowVector, current_type_promotions_, pool_);
  }

  return rowVector;
}

facebook::velox::RowVectorPtr PangrovaVeloxDataSource::applyLanceIdentity(
    facebook::velox::RowVectorPtr rows, facebook::velox::VectorPtr positions) {
  if (!physical_lance_file_ && !source_object_identity_requested_ &&
      !output_type_->containsChild(kPhysicalRowPositionColumnName) && !artifact_candidate_filter_enabled_) {
    return rows;
  }
  const auto size = rows->size();
  if (physical_lance_file_) {
    const auto start = lance_reader_->lastBatchRowStart();
    if (size <= 0 ||
        start > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - static_cast<uint64_t>(size - 1)) {
      throw std::runtime_error("Physical Lance row position exceeds the signed locator bounds");
    }
    row_index_offset_ = static_cast<int64_t>(start);
    positions = facebook::velox::BaseVector::create(facebook::velox::BIGINT(), size, pool_);
    for (facebook::velox::vector_size_t row = 0; row < size; ++row) {
      positions->asFlatVector<int64_t>()->set(row, static_cast<int64_t>(row_index_offset_ + row));
    }
  }
  auto names = output_type_->names();
  auto types = output_type_->children();
  const auto& inputType = rows->type()->asRow();
  if (rows->childrenSize() != inputType.size()) {
    throw std::runtime_error("Lance logical row vector does not match its declared input schema");
  }
  std::vector<facebook::velox::VectorPtr> children(output_type_->size());
  for (size_t channel = 0; channel < output_type_->size(); ++channel) {
    const auto& name = names[channel];
    if (name == kPhysicalRowPositionColumnName || isSourceIdentityConstant(name)) continue;
    const auto inputChannel = inputType.getChildIdxIfExists(name);
    if (!inputChannel || !rows->childAt(*inputChannel) ||
        !rows->childAt(*inputChannel)->type()->equivalent(*types[channel])) {
      throw std::runtime_error("Lance logical output column is absent or differs from its declared type: " + name);
    }
    children[channel] = rows->childAt(*inputChannel);
  }
  auto positionChannel = output_type_->getChildIdxIfExists(kPhysicalRowPositionColumnName);
  if (positionChannel || artifact_candidate_filter_enabled_) {
    if (!positions || positions->size() != size ||
        positions->typeKind() != facebook::velox::TypeKind::BIGINT ||
        (positionChannel && types[*positionChannel]->kind() != facebook::velox::TypeKind::BIGINT)) {
      throw std::runtime_error("Lance identity read did not produce exact BIGINT physical positions");
    }
    if (positionChannel) children[*positionChannel] = positions;
    else {
      names.emplace_back(kPhysicalRowPositionColumnName);
      types.push_back(facebook::velox::BIGINT());
      children.push_back(positions);
    }
  }
  for (size_t channel = 0; channel < names.size(); ++channel) {
    if (names[channel] == kSourceObjectIdColumnName) {
      children[channel] = facebook::velox::BaseVector::createConstant(
          facebook::velox::VARCHAR(), facebook::velox::variant(current_data_object_id_), size, pool_);
    } else if (names[channel] == kSourceIdColumnName || names[channel] == kSourceSnapshotColumnName) {
      const auto& value = names[channel] == kSourceIdColumnName ? current_source_id_ : current_source_snapshot_;
      if (value.empty()) throw std::runtime_error("Lance identity has no signed source authority");
      children[channel] = facebook::velox::BaseVector::createConstant(
          facebook::velox::VARCHAR(), facebook::velox::variant(value), size, pool_);
    }
  }
  rows = std::make_shared<facebook::velox::RowVector>(pool_, facebook::velox::ROW(names, types),
      nullptr, size, std::move(children));
  if (artifact_candidate_filter_enabled_) return applyArtifactCandidateFilter(std::move(rows));
  return physical_lance_file_ ? applyDeleteFilter(std::move(rows), size) : rows;
}

facebook::velox::RowVectorPtr
PangrovaVeloxDataSource::applyLancePostProcessing(
    facebook::velox::RowVectorPtr rowVector) {
  if (!lance_data_output_type_) {
    throw std::runtime_error("Lance logical data schema is not initialized");
  }
  rowVector = applyColumnMapping(
      rowVector, current_column_mappings_, lance_data_output_type_, pool_);
  if (!current_type_promotions_.empty()) {
    rowVector = applyTypePromotion(
        rowVector, current_type_promotions_, pool_);
  }
  return injectPartitionColumns(
      rowVector, current_partition_values_, output_type_, pool_);
}

facebook::velox::RowVectorPtr PangrovaVeloxDataSource::applyDeleteFilter(
    facebook::velox::RowVectorPtr rowVector,
    facebook::velox::vector_size_t originalBatchSize) {
  if (delete_filters_.empty() || !rowVector || rowVector->size() == 0) {
    row_index_offset_ += originalBatchSize;
    return rowVector;
  }

  facebook::velox::SelectivityVector rows(rowVector->size());
  for (const auto& filter : delete_filters_) {
    filter->apply(*rowVector, rows, row_index_offset_);
  }
  row_index_offset_ += originalBatchSize;

  auto selected = rows.countSelected();
  if (selected == rowVector->size()) {
    return rowVector;  // No rows deleted.
  }
  if (selected == 0) {
    return nullptr;
  }

  // Compact the RowVector by selecting only the surviving rows.
  // BaseVector::wrapInDictionary on a RowVector creates a
  // DictionaryVector<ComplexType> which is NOT a RowVector subclass —
  // dynamic_pointer_cast<RowVector> would return nullptr, silently
  // dropping the entire batch. Instead, construct a new RowVector
  // with dictionary-wrapped children that preserves the RowVector type.
  auto indices = facebook::velox::allocateIndices(selected, pool_);
  auto* rawIndices = indices->asMutable<facebook::velox::vector_size_t>();
  facebook::velox::vector_size_t cnt = 0;
  rows.applyToSelected([&](auto r) { rawIndices[cnt++] = r; });

  std::vector<facebook::velox::VectorPtr> wrappedChildren(
      rowVector->childrenSize());
  for (size_t i = 0; i < rowVector->childrenSize(); ++i) {
    wrappedChildren[i] = facebook::velox::BaseVector::wrapInDictionary(
        nullptr, indices, cnt, rowVector->childAt(i));
  }
  return std::make_shared<facebook::velox::RowVector>(
      pool_, rowVector->type(), nullptr, cnt, std::move(wrappedChildren));
}

void PangrovaVeloxDataSource::addDynamicFilter(
    facebook::velox::column_index_t outputChannel,
    const std::shared_ptr<facebook::velox::common::Filter>& filter) {
  if (!filter) {
    return;
  }
  if (outputChannel >= output_type_->size()) {
    PANGROVA_LOG_ERROR << "dynamic_filter_column_out_of_bounds"
                     << " output_channel=" << outputChannel
                     << " output_type_size=" << output_type_->size()
                     << " file_path=" << current_file_path_;
    return;
  }
  std::lock_guard<std::mutex> lock(dynamic_filter_mutex_);
  dynamic_filters_.emplace_back(outputChannel, filter);
  PANGROVA_LOG_INFO << "dynamic_filter_added"
                  << " output_channel=" << outputChannel
                  << " column_name=" << output_type_->nameOf(outputChannel)
                  << " file_path=" << current_file_path_;
}

facebook::velox::RowVectorPtr PangrovaVeloxDataSource::applyDynamicFilters(
    facebook::velox::RowVectorPtr rowVector) {
  std::vector<std::pair<facebook::velox::column_index_t,
                        std::shared_ptr<facebook::velox::common::Filter>>>
      filters;
  {
    std::lock_guard<std::mutex> lock(dynamic_filter_mutex_);
    if (dynamic_filters_.empty()) {
      return rowVector;
    }
    filters = dynamic_filters_;
  }
  return evaluateDynamicFilters(
      std::move(rowVector), filters, output_type_, pool_);
}

void PangrovaVeloxDataSource::setCancellationToken(folly::CancellationToken token) {
  cancellation_token_ = std::move(token);
}

void PangrovaVeloxDataSource::setReadBudget(std::shared_ptr<resource::TaskObjectReadBudget> budget) {
  read_budget_ = std::move(budget);
}

void PangrovaVeloxDataSource::cancel() {
  cancellation_requested_.store(true, std::memory_order_release);
}

void PangrovaVeloxDataSource::checkCancelled() const {
  if (cancellation_requested_.load(std::memory_order_acquire) ||
      cancellation_token_.isCancellationRequested()) {
    throw std::runtime_error("Native source read cancelled");
  }
}

uint64_t PangrovaVeloxDataSource::getCompletedRows() {
  return completed_rows_;
}

uint64_t PangrovaVeloxDataSource::getCompletedBytes() {
  return completed_bytes_;
}

// --- PangrovaVeloxConnector ---

PangrovaVeloxConnector::PangrovaVeloxConnector(
    const std::string& id,
    std::shared_ptr<const facebook::velox::config::ConfigBase> config)
    : Connector(id, std::move(config)) {}

std::unique_ptr<facebook::velox::connector::DataSource>
PangrovaVeloxConnector::createDataSource(
    const facebook::velox::RowTypePtr& outputType,
    const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
    const facebook::velox::connector::ColumnHandleMap& columnHandles,
    facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  // ConnectorQueryCtx compatibility note (Velox v2026.04.17):
  // ConnectorQueryCtx gained new constructor parameters (PrefixSortConfig,
  // TokenProvider, CancellationToken) in recent Velox versions. This method
  // only consumes the pointer via stable accessors (memoryPool(), taskId()),
  // so no adaptation is needed here — the Velox Task framework constructs
  // ConnectorQueryCtx with the new parameters before passing it to us.

  // Extract remaining filter from PangrovaTableHandle for reader-level
  // predicate pushdown. When present, the filter is forwarded to
  // RowReaderOptions so the DWIO reader can skip row groups whose
  // statistics prove no matching rows.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> remainingFilter;
  facebook::velox::RowTypePtr scanSchema;
  std::string planNodeId;
  std::shared_ptr<resource::TaskObjectReadBudget> readBudget;
  if (tableHandle) {
    auto pangrovaHandle =
        std::dynamic_pointer_cast<const PangrovaTableHandle>(tableHandle);
    if (pangrovaHandle) {
      remainingFilter = pangrovaHandle->remainingFilter();
      planNodeId = pangrovaHandle->planNodeId();
      scanSchema = pangrovaHandle->scanSchema();
      readBudget = pangrovaHandle->readBudget();
    }
  }

  // Read preferred output batch bytes. ConnectorQueryCtx does not expose
  // QueryCtx directly in this Velox version, so use the default target.
  // VeloxTaskRunner sets preferredOutputBatchBytes via QueryConfig at the
  // Task level, which propagates to the DataSource via Velox internals.
  int64_t preferredBatchBytes = kDefaultBatchTargetBytes;

  // L2-6: Read eager first stripe threshold from session properties
  // (overridable via connector.eager_first_stripe_threshold_bytes).
  int64_t eagerFirstStripeThreshold = 1L * 1024 * 1024;  // default 1MB
  auto* sessProps = connectorQueryCtx->sessionProperties();
  if (sessProps) {
    auto thresholdStr = sessProps->get<std::string>(
        "connector.eager_first_stripe_threshold_bytes", "");
    if (!thresholdStr.empty()) {
      try { eagerFirstStripeThreshold = std::stol(thresholdStr); }
      catch (...) { /* use default */ }
    }
  }

  const auto sourceType = assignedSourceType(outputType, columnHandles);
  auto source = std::make_unique<PangrovaVeloxDataSource>(
      connectorQueryCtx->memoryPool(), sourceType,
      scanSchema,
      connectorQueryCtx->taskId(),
      planNodeId,
      assignedSourceFilter(remainingFilter, outputType, sourceType),
      preferredBatchBytes,
      eagerFirstStripeThreshold);
  source->setCancellationToken(connectorQueryCtx->cancellationToken());
  source->setReadBudget(std::move(readBudget));
  return std::make_unique<ColumnAssignmentDataSource>(
      std::move(source), outputType, sourceType);
}

std::unique_ptr<facebook::velox::connector::DataSink>
PangrovaVeloxConnector::createDataSink(
    facebook::velox::RowTypePtr inputType,
    facebook::velox::connector::ConnectorInsertTableHandlePtr
        connectorInsertTableHandle,
    facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    facebook::velox::connector::CommitStrategy commitStrategy) {
  // Task-owned write sinks consume the same Velox output queue concurrently
  // with the producer. Native format writers publish typed commit metadata;
  // connector-owned sessions receive the canonical stream through JNI. This
  // connector does not register a second Velox DataSink execution path.
  throw pangrova::executor::NativeUnsupportedFeatureException(
      "write_path",
      /*connectorType=*/"pangrova",
      /*splitId=*/"");
}

}  // namespace pangrova::velox
