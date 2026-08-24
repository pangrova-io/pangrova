// PangrovaVeloxConnector: Pangrova's Velox Connector and DataSource implementation.
// Uses Velox native readers (ParquetReader/DwrfReader) for zero-conversion
// data reading. Supports predicate pushdown via RowReaderOptions for row
// group statistics-based pruning. Iceberg delete filter is applied as
// post-processing on RowVector + SelectivityVector level.
#include "velox/connector/PangrovaVeloxConnector.h"

#include <algorithm>
#include <chrono>
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
#include "delete/io/DeletionVectorReader.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/PartitionColumnInjector.h"
#include "velox/connector/SchemaEvolution.h"
#include "velox/connector/DynamicFilterEvaluator.h"
#include "velox/filter/PendingFilterRegistry.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

#if PANGROVA_HAS_VELOX_DWIO
#include "velox/common/file/File.h"
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
    const std::string& taskId,
    std::shared_ptr<const facebook::velox::core::ITypedExpr> remainingFilter,
    int64_t preferredOutputBatchBytes,
    int64_t eagerFirstStripeThresholdBytes)
    : pool_(pool),
      output_type_(outputType),
      eager_first_stripe_threshold_bytes_(eagerFirstStripeThresholdBytes),
      task_id_(taskId),
      remaining_filter_(std::move(remainingFilter)) {
  // Compute initial adaptive batch size from output schema column widths,
  // using the operator-level preferred output batch bytes as the target.
  batch_sizer_.computeOptimalBatchSize(output_type_, preferredOutputBatchBytes);
}

void PangrovaVeloxDataSource::addSplit(
    std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) {
  auto pangrovaSplit =
      std::dynamic_pointer_cast<PangrovaConnectorSplit>(split);
  if (!pangrovaSplit) return;

  const auto& splitConfig = pangrovaSplit->splitConfig;

  // Store file path for structured logging.
  current_file_path_ = splitConfig.file_path;

  // Populate partition values, schema evolution, delete filters,
  // changelog mode flag, and Paimon deletion vectors.
  prepareSplitState(splitConfig);

  // Reset merge reader from any previous split.
  merge_reader_.reset();

  // Reset batch calibration for the new split — different files may have
  // different actual row widths (e.g., different VARCHAR lengths).
  batch_calibrated_ = false;

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
#if PANGROVA_LANCE_AVAILABLE
  if (splitConfig.file_format == split::FileFormatType::LANCE) {
    openLanceReader(splitConfig);
    return;
  }
#else
  if (splitConfig.file_format == split::FileFormatType::LANCE) {
    throw std::runtime_error(
        "Lance file format requested but Lance C FFI is not linked. "
        "Rebuild with LANCE_C_FFI to enable Lance support. "
        "file_path=" + splitConfig.file_path);
  }
#endif

  // COW / read-optimized path: when needs_merge=false the split is
  // fully compacted — read as a plain scan via openVeloxReader().
  openVeloxReader(splitConfig);
}

void PangrovaVeloxDataSource::prepareSplitState(
    const split::SplitConfig& config) {
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
    loadPaimonDeletionVector(config.paimon.value());
  }
}

void PangrovaVeloxDataSource::loadPaimonDeletionVector(
    const split::PaimonConfig& paimon) {
  if (!paimon.deletion_vectors_enabled ||
      paimon.deletion_vector_file_path.empty()) {
    return;
  }
  try {
    auto bitmap = delete_filter::io::DeletionVectorReader::readPaimonDV(
        paimon.deletion_vector_file_path);
    delete_filters_.push_back(
        std::make_unique<delete_filter::DeletionVectorRowFilter>(
            std::move(bitmap)));
  } catch (const std::exception& e) {
    PANGROVA_LOG_ERROR << "dv_parse_failed"
                     << " file_path=" << current_file_path_
                     << " dv_path=" << paimon.deletion_vector_file_path
                     << " error_detail=" << e.what();
    throw std::runtime_error(
        "Failed to load Paimon deletion vector: dv_path=" +
        paimon.deletion_vector_file_path +
        ", error=" + e.what());
  }
}

void PangrovaVeloxDataSource::createReaderPair(
    const std::string& filePath,
    split::FileFormatType fileFormat,
    std::unique_ptr<facebook::velox::dwio::common::Reader>& outReader,
    std::unique_ptr<facebook::velox::dwio::common::RowReader>& outRowReader,
    std::shared_ptr<facebook::velox::common::ScanSpec>* outScanSpec) {
#if PANGROVA_HAS_VELOX_DWIO
  auto cleanPath = split::normalizeToS3Uri(
      split::stripFilePrefix(filePath));
  auto format = resolveFileFormat(fileFormat);

  facebook::velox::dwio::common::ReaderOptions readerOpts(pool_);
  readerOpts.setFileFormat(format);

  // IO executor for async prefetch. Velox uses this to overlap IO with
  // computation: while the current batch is being processed, the next row
  // group is prefetched in the background.
  auto ioExecutor =
      pangrova::jni::NativeEngineLifecycle::instance().executor();
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

  auto readFile = std::make_shared<facebook::velox::LocalReadFile>(cleanPath);
  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> input;
#if PANGROVA_HAS_CACHED_BUFFERED_INPUT && PANGROVA_HAS_VELOX_ASYNC_CACHE
  auto* cache = facebook::velox::cache::AsyncDataCache::getInstance();
  if (cache) {
    // Pass a shared IoStatistics instance for cache hit/miss observability.
    // ScanTracker and StringIdLease remain empty — scan-level tracking is
    // not yet wired to NativeMetricsCollector.
    auto ioStats = std::make_shared<facebook::velox::dwio::common::IoStatistics>();
    input = std::make_unique<facebook::velox::dwio::common::CachedBufferedInput>(
        readFile,
        facebook::velox::dwio::common::MetricsLog::voidLog(),
        facebook::velox::StringIdLease(),
        cache,
        std::shared_ptr<facebook::velox::cache::ScanTracker>(),
        facebook::velox::StringIdLease(),
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

  facebook::velox::dwio::common::RowReaderOptions rowOpts;

  // Schema evolution: when column mappings are present, build a "reader type"
  // that uses source (physical) column names from the file schema. The Parquet
  // reader matches columns by name, so renamed columns would fail without this.
  // Post-processing (applyColumnMapping) renames back to target names.
  auto readerType = output_type_;
  if (!current_column_mappings_.empty()) {
    std::unordered_map<std::string, std::string> target_to_source;
    for (const auto& cm : current_column_mappings_) {
      auto lower_target = cm.target_name;
      std::transform(lower_target.begin(), lower_target.end(),
                     lower_target.begin(), ::tolower);
      target_to_source[lower_target] = cm.source_name;
    }
    std::vector<std::string> reader_names;
    std::vector<facebook::velox::TypePtr> reader_types;
    reader_names.reserve(output_type_->size());
    reader_types.reserve(output_type_->size());
    for (size_t i = 0; i < output_type_->size(); ++i) {
      auto col_name = output_type_->nameOf(i);
      auto lower_col = col_name;
      std::transform(lower_col.begin(), lower_col.end(),
                     lower_col.begin(), ::tolower);
      auto it = target_to_source.find(lower_col);
      if (it != target_to_source.end()) {
        reader_names.push_back(it->second);
      } else {
        reader_names.push_back(col_name);
      }
      reader_types.push_back(output_type_->childAt(i));
    }
    readerType = facebook::velox::ROW(
        std::move(reader_names), std::move(reader_types));
  }

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

  outRowReader = outReader->createRowReader(rowOpts);
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

#if PANGROVA_LANCE_AVAILABLE
void PangrovaVeloxDataSource::openLanceReader(
    const split::SplitConfig& split) {
  reader_exhausted_ = false;
#if PANGROVA_HAS_VELOX_DWIO
  velox_reader_.reset();
  velox_row_reader_.reset();
#endif
  lance_reader_.reset();

  auto filePath = split::normalizeToS3Uri(
      split::stripFilePrefix(split.file_path));

  // Build projected column list from output type.
  std::vector<std::string> projectedColumns;
  projectedColumns.reserve(output_type_->size());
  for (auto i = 0; i < output_type_->size(); ++i) {
    projectedColumns.push_back(output_type_->nameOf(i));
  }

  try {
    lance_reader_ = std::make_unique<pangrova::lance::LanceBatchReader>(
        pool_, output_type_, filePath, projectedColumns);
  } catch (const std::exception& e) {
    PANGROVA_LOG_ERROR << "lance_reader_open_failed"
                     << " file_path=" << filePath
                     << " error_detail=" << e.what();
    throw std::runtime_error(
        "Failed to open Lance file: path=" + filePath +
        ", error=" + e.what());
  }

  // Pass remaining filter to Lance reader for predicate pushdown.
  // Lance C FFI accepts SQL-like filter expressions.
  if (remaining_filter_) {
    auto filterStr = remaining_filter_->toString();
    lance_reader_->setFilter(filterStr);
    PANGROVA_LOG_INFO << "lance_predicate_pushdown=active"
                    << " file_path=" << filePath
                    << " filter=" << filterStr;
  }
}
#endif

void PangrovaVeloxDataSource::initializeMergeReader(
    const split::SplitConfig& split) {
#if PANGROVA_HAS_VELOX_DWIO
  const auto& paimon = split.paimon.value();

  // Create the merge reader from factory (resolves engine type).
  try {
    merge_reader_ =
        merge::MergeReaderFactory::create(paimon, output_type_);
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
    // Factory returned nullptr — fallback to single-file read.
    openVeloxReader(split);
    return;
  }

  // Create one RowReaderWrapper per LSM level file.
  // Apply predicate pushdown to each per-level reader's ScanSpec when a
  // remaining filter is available. This enables row group pruning during
  // MOR merge reads, avoiding full-file scans when WHERE conditions can
  // eliminate row groups via column statistics.
  std::vector<std::unique_ptr<merge::RowReaderWrapper>> readers;
  for (const auto& level : paimon.lsm_levels) {
    for (const auto& path : level.file_paths) {
      try {
        std::unique_ptr<facebook::velox::dwio::common::Reader> reader;
        std::unique_ptr<facebook::velox::dwio::common::RowReader> rowReader;
        std::shared_ptr<facebook::velox::common::ScanSpec> scanSpec;
        createReaderPair(path, split.file_format, reader, rowReader, &scanSpec);
        if (remaining_filter_ && scanSpec) {
          decomposeFilterToScanSpec(remaining_filter_, scanSpec, output_type_);
        }
        readers.push_back(std::make_unique<VeloxBatchReader>(
            std::move(reader), std::move(rowReader), output_type_));
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
  return rowVector;
}

std::optional<facebook::velox::RowVectorPtr> PangrovaVeloxDataSource::next(
    uint64_t size,
    facebook::velox::ContinueFuture& future) {
#if PANGROVA_HAS_VELOX_DWIO
  // Use adaptive batch size, respecting Velox's requested upper bound.
  auto effectiveSize =
      static_cast<uint64_t>(batch_sizer_.getCurrentBatchSize());
  auto batchSize = std::min(size, effectiveSize);

  // Paimon MOR merge path: pull from merge reader when present.
  // After merge, still apply partition injection and schema evolution.
  if (merge_reader_) {
    auto merged = nextFromMergeReader(batchSize);
    if (!merged.value()) {
      // Merge reader exhausted — propagate split completion signal (nullptr).
      return merged;
    }
    auto rowVector = merged.value();
    return applyPostProcessing(std::move(rowVector));
  }

  // Lance file format path: pull from Lance reader when present.
  // After read, apply partition injection and schema evolution (same as
  // Parquet/ORC path). This enables mixed-format scans where some splits
  // are Parquet and others are Lance within the same table.
#if PANGROVA_LANCE_AVAILABLE
  if (lance_reader_) {
    if (!lance_reader_->hasNext()) {
      lance_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    auto rowVector = lance_reader_->next(batchSize);
    if (!rowVector || rowVector->size() == 0) {
      lance_reader_.reset();
      return facebook::velox::RowVectorPtr(nullptr);
    }
    completed_rows_ += rowVector->size();
    completed_bytes_ += rowVector->estimateFlatSize();
    return applyPostProcessing(std::move(rowVector));
  }
#endif

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
  auto result = facebook::velox::BaseVector::create(output_type_, 0, pool_);
  uint64_t rowsRead = velox_row_reader_->next(batchSize, result);
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
    reader_exhausted_ = true;
    // Cast failed — treat as exhausted, signal split completion.
    return facebook::velox::RowVectorPtr(nullptr);
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
  if (!delete_filters_.empty()) {
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
  if (!task_id_.empty()) {
    auto pending = PendingFilterRegistry::instance().drain(task_id_);
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
        rowVector, current_column_mappings_, output_type_, pool_);
  }
  if (!current_type_promotions_.empty()) {
    rowVector = applyTypePromotion(
        rowVector, current_type_promotions_, pool_);
  }

  return rowVector;
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
  if (tableHandle) {
    auto pangrovaHandle =
        std::dynamic_pointer_cast<const PangrovaTableHandle>(tableHandle);
    if (pangrovaHandle) {
      remainingFilter = pangrovaHandle->remainingFilter();
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

  return std::make_unique<PangrovaVeloxDataSource>(
      connectorQueryCtx->memoryPool(), outputType,
      connectorQueryCtx->taskId(),
      std::move(remainingFilter),
      preferredBatchBytes,
      eagerFirstStripeThreshold);
}

std::unique_ptr<facebook::velox::connector::DataSink>
PangrovaVeloxConnector::createDataSink(
    facebook::velox::RowTypePtr inputType,
    facebook::velox::connector::ConnectorInsertTableHandlePtr
        connectorInsertTableHandle,
    facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    facebook::velox::connector::CommitStrategy commitStrategy) {
  // ARCHITECTURE DECISION: Write path bypasses Velox Connector DataSink.
  //
  // The read path uses PangrovaVeloxDataSource (Connector DataSource interface)
  // because Velox's TableScan operator natively integrates with DataSource.
  // The write path, however, uses a custom WriteOperator (src/write/) that
  // calls Java ConnectorPageSink via JNI (NativeWriteExecutor). This is
  // because:
  //   1. Velox's DataSink interface assumes the sink owns the file writer,
  //      but Pangrova delegates writing to Java-side connectors which manage
  //      transactions, metadata commits, and manifest updates.
  //   2. The JNI callback model (C++ → Java) for write is inherently
  //      synchronous and pull-based, matching WriteOperator's addInput()
  //      contract but not DataSink's async model.
  //   3. PlanExecutionDriver::drainOutputToWriter() bridges the gap by
  //      draining the OutputQueue after Velox execution completes.
  //
  // See: src/write/WriteOperator.h, src/write/NativeWriteExecutor.h,
  //      PlanExecutionDriver::drainOutputToWriter().
  throw pangrova::executor::NativeUnsupportedFeatureException(
      "write_path",
      /*connectorType=*/"pangrova",
      /*splitId=*/"");
}

}  // namespace pangrova::velox
