// PangrovaVeloxConnector: Pangrova's Velox Connector and DataSource implementation.
// Uses Velox native readers (ParquetReader/DwrfReader) for zero-conversion
// data reading. Supports predicate pushdown via RowReaderOptions for row
// group statistics-based pruning. Iceberg delete filter is applied as
// post-processing on RowVector + SelectivityVector level.
//
// Placement: velox/ — Velox integration layer.
#pragma once

#include <memory>
#include <atomic>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "resource/TaskObjectReadBudget.h"
#include "split/SplitConfig.h"
#include "delete/RowDeleteFilter.h"
#include "merge/MergeReader.h"
#include "merge/MergeReaderFactory.h"
#include "velox/connector/AdaptiveBatchSizer.h"
#include "velox/connector/IcebergMetadataJsonReader.h"
#include "velox/connector/PaimonSnapshotJsonReader.h"
#include "velox/connector/PartitionColumnInjector.h"
#include "velox/connector/SchemaEvolution.h"
#include "velox/connector/SourceObjectIdentityInjector.h"

#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"
#include "folly/CancellationToken.h"

#include "lance/LanceBatchReader.h"

#if __has_include("velox/dwio/common/ReaderFactory.h")
#include "velox/dwio/common/ReaderFactory.h"
#define PANGROVA_HAS_VELOX_DWIO 1
#else
#define PANGROVA_HAS_VELOX_DWIO 0
#endif

namespace pangrova::velox {

// Custom ConnectorSplit that wraps Pangrova's SplitConfig.
// Passed through Task::addSplit() → DataSource::addSplit().
struct PangrovaConnectorSplit : public facebook::velox::connector::ConnectorSplit {
  split::SplitConfig splitConfig;

  PangrovaConnectorSplit(
      const std::string& connectorId,
      split::SplitConfig config)
      : ConnectorSplit(connectorId),
        splitConfig(std::move(config)) {}
};

// Adapter wrapping a Velox dwio::common::RowReader into the
// VeloxRowReader interface for consumption by VeloxMergeReader.
// Each adapter owns one Velox Reader + RowReader pair representing
// a single LSM level file in a Paimon MOR split.
class VeloxBatchReader : public merge::RowReaderWrapper {
 public:
#if PANGROVA_HAS_VELOX_DWIO
  VeloxBatchReader(
      std::unique_ptr<facebook::velox::dwio::common::Reader> reader,
      std::unique_ptr<facebook::velox::dwio::common::RowReader> rowReader,
      facebook::velox::RowTypePtr outputType);
#endif

  facebook::velox::RowVectorPtr next(
      uint64_t size,
      facebook::velox::memory::MemoryPool* pool) override;

  bool hasNext() const override;

 private:
#if PANGROVA_HAS_VELOX_DWIO
  std::unique_ptr<facebook::velox::dwio::common::Reader> reader_;
  std::unique_ptr<facebook::velox::dwio::common::RowReader> row_reader_;
#endif
  facebook::velox::RowTypePtr output_type_;
  bool exhausted_ = false;
};

// Velox DataSource implementation for Pangrova.
// Reads files using Velox native readers (ParquetReader/DwrfReader),
// producing RowVector directly without Arrow RecordBatch intermediate
// conversion.
//
// Supports predicate pushdown: when a remaining filter expression is
// provided, it is set on RowReaderOptions so the DWIO reader can skip
// row groups whose statistics prove no matching rows.
//
// For Iceberg splits with delete files, applies RowDeleteFilter as
// post-processing on RowVector + SelectivityVector.
class PangrovaVeloxDataSource : public facebook::velox::connector::DataSource {
 public:
  // Construct with optional remaining filter for reader-level pushdown.
  // When remainingFilter is non-null, openVeloxReader() passes it to
  // RowReaderOptions enabling row group statistics-based pruning.
  // task_id is used to drain externally injected dynamic filters from
  // the PendingFilterRegistry on each next() call.
  explicit PangrovaVeloxDataSource(
      facebook::velox::memory::MemoryPool* pool,
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::RowTypePtr& scanSchema,
      const std::string& taskId = "",
      const std::string& planNodeId = "",
      std::shared_ptr<const facebook::velox::core::ITypedExpr>
          remainingFilter = nullptr,
      int64_t preferredOutputBatchBytes = kDefaultBatchTargetBytes,
      int64_t eagerFirstStripeThresholdBytes = 1L * 1024 * 1024);

  void addSplit(
      std::shared_ptr<facebook::velox::connector::ConnectorSplit> split)
      override;

  std::optional<facebook::velox::RowVectorPtr> next(
      uint64_t size,
      facebook::velox::ContinueFuture& future) override;

  void addDynamicFilter(
      facebook::velox::column_index_t outputChannel,
      const std::shared_ptr<facebook::velox::common::Filter>& filter) override;

  uint64_t getCompletedRows() override;
  uint64_t getCompletedBytes() override;
  void setCancellationToken(folly::CancellationToken token);
  void setReadBudget(std::shared_ptr<resource::TaskObjectReadBudget> budget);
  void cancel() override;

 private:
  void checkCancelled() const;
  folly::CancellationToken cancellation_token_;
  std::shared_ptr<resource::TaskObjectReadBudget> read_budget_;
  std::atomic<bool> cancellation_requested_{false};
  std::string current_source_id_;
  std::string current_source_snapshot_;
  // Apply dynamic filters to a RowVector, returning a filtered vector.
  // Uses SelectivityVector to mark rows that pass all dynamic filters,
  // then compacts via dictionary wrapping (same pattern as applyDeleteFilter).
  facebook::velox::RowVectorPtr applyDynamicFilters(
      facebook::velox::RowVectorPtr rowVector);
  // Applies a Native retrieval allowlist using the DWIO physical row-index
  // column, then removes that hidden column before the relational pipeline.
  facebook::velox::RowVectorPtr applyArtifactCandidateFilter(
      facebook::velox::RowVectorPtr rowVector);
  // Prepare split-level state: partition values, schema evolution,
  // delete filters, changelog mode, and Paimon deletion vectors.
  // Called from addSplit() before reader dispatch.
  void prepareSplitState(const split::SplitConfig& config);

  // Open Velox native reader for the current split's file.
  void openVeloxReader(const split::SplitConfig& split);

  // Open Lance reader for the current split's file.
  // Dispatched when split format is LANCE. Uses LanceBatchReader via
  // Lance C FFI instead of Velox DWIO readers.
  void openLanceReader(const split::SplitConfig& split);

  // Adds signed source identity and exact physical offsets before policy filtering.
  facebook::velox::RowVectorPtr applyLanceIdentity(
      facebook::velox::RowVectorPtr rows, facebook::velox::VectorPtr datasetPositions);

  // Load Paimon MOW deletion vector when DV is enabled.
  // Reads the DV file via DeletionVectorReader and creates a
  // DeletionVectorRowFilter added to delete_filters_.
  void loadPaimonDeletionVector(const split::SplitConfig& split);

  // Create a Velox DWIO Reader + RowReader pair for a file path.
  // Shared by openVeloxReader() and initializeMergeReader() to eliminate
  // duplicated reader construction logic (ReaderOptions, BufferedInput,
  // CachedBufferedInput, ScanSpec). Returns the ScanSpec via outScanSpec
  // so that openVeloxReader() can attach predicate pushdown filters.
  void createReaderPair(
      const std::string& filePath,
      split::FileFormatType fileFormat,
      std::unique_ptr<facebook::velox::dwio::common::Reader>& outReader,
      std::unique_ptr<facebook::velox::dwio::common::RowReader>& outRowReader,
      std::shared_ptr<facebook::velox::common::ScanSpec>* outScanSpec = nullptr,
      facebook::velox::RowTypePtr requestedType = nullptr);

  // Initialize Paimon MOR merge reader from LSM level files.
  // Creates one VeloxBatchReader per level file, then delegates
  // to MergeReaderFactory::create() for the merge reader.
  void initializeMergeReader(const split::SplitConfig& split);

  // Read next batch from merge reader. Returns nullopt when exhausted.
  std::optional<facebook::velox::RowVectorPtr> nextFromMergeReader(
      uint64_t batchSize);

  // Apply Iceberg delete filter post-processing on RowVector.
  // originalBatchSize is the pre-dynamic-filter row count, used to advance
  // row_index_offset_ correctly for file-level position delete alignment.
  facebook::velox::RowVectorPtr applyDeleteFilter(
      facebook::velox::RowVectorPtr rowVector,
      facebook::velox::vector_size_t originalBatchSize);

  // Apply partition injection, column mapping, and type promotion.
  // Shared by both the normal read path and the merge reader path.
  facebook::velox::RowVectorPtr applyPostProcessing(
      facebook::velox::RowVectorPtr rowVector);
  facebook::velox::RowVectorPtr applyLancePostProcessing(
      facebook::velox::RowVectorPtr rowVector);

  facebook::velox::memory::MemoryPool* pool_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::RowTypePtr scan_schema_;
  facebook::velox::RowTypePtr current_reader_output_type_;
  facebook::velox::RowTypePtr lance_data_output_type_;
  facebook::velox::RowTypePtr file_evolution_output_type_;
  uint64_t completed_rows_ = 0;
  uint64_t completed_bytes_ = 0;

  // L2-6: Configurable eager first stripe threshold (bytes).
  // Files smaller than this threshold eagerly load the first stripe metadata
  // to avoid a second I/O round-trip. Default 1MB.
  int64_t eager_first_stripe_threshold_bytes_ = 1L * 1024 * 1024;

  // Task ID for draining externally injected dynamic filters from
  // PendingFilterRegistry. Empty when not associated with a task.
  std::string task_id_;

  // Exact scan-node ID. Together with task_id_ this prevents a filter for one
  // scan from being consumed by a sibling scan in the same task.
  std::string plan_node_id_;

  // Remaining filter expression for DWIO reader-level predicate pushdown.
  // Set on RowReaderOptions in openVeloxReader() to enable row group pruning.
  std::shared_ptr<const facebook::velox::core::ITypedExpr> remaining_filter_;

  // Current split file path for structured logging.
  std::string current_file_path_;

  // Canonical immutable object identity carried by SplitConfig. It is
  // materialized as a Native constant column only when the plan explicitly
  // requests $pangrova$source_object_id.
  std::string current_data_object_id_;
  bool source_object_identity_requested_ = false;

  // Velox native reader state.
#if PANGROVA_HAS_VELOX_DWIO
  std::unique_ptr<facebook::velox::dwio::common::Reader> velox_reader_;
  std::unique_ptr<facebook::velox::dwio::common::RowReader> velox_row_reader_;
#endif
  bool reader_exhausted_ = false;

  // Iceberg delete filter (operates directly on RowVector).
  std::vector<std::unique_ptr<delete_filter::RowDeleteFilter>> delete_filters_;
  int64_t row_index_offset_ = 0;

  bool artifact_candidate_filter_enabled_ = false;
  std::unordered_set<uint64_t> artifact_candidate_positions_;
  bool materialized_retrieval_score_enabled_ = false;
  std::unordered_map<uint64_t, double> materialized_retrieval_scores_;

  // Adaptive batch sizer — computes optimal row count from output schema
  // and adjusts under memory pressure. NOT thread-safe; single-threaded
  // use within this DataSource instance.
  AdaptiveBatchSizer batch_sizer_;
  // True after the first batch calibrates batch_sizer_ with actual row width.
  bool batch_calibrated_ = false;

  // Partition column values from the current split (Iceberg or Paimon).
  // Used by injectPartitionColumns() in next() to synthesize constant
  // columns for partition columns not present in the data file.
  // Identity and hidden partition transforms are evaluated on the Java
  // side — native side receives pre-computed string values.
  std::unordered_map<std::string, std::string> current_partition_values_;

  // Schema evolution config from the current split.
  // Populated in addSplit() from SplitConfig.column_mappings and
  // SplitConfig.type_promotions. Applied in next() after delete filter
  // and partition injection. Zero overhead when both are empty.
  std::vector<split::ColumnMappingConfig> current_column_mappings_;
  std::vector<split::TypePromotionConfig> current_type_promotions_;

  // Paimon MOR merge reader. Non-null when the current split requires
  // multi-way LSM merge (needs_merge=true). When present, next() pulls
  // from the merge reader instead of the single Velox row reader.
  // Set to nullptr when merge reader is exhausted or not needed.
  std::unique_ptr<merge::MergeReader> merge_reader_;
  facebook::velox::RowTypePtr merge_reader_output_type_;

  // Lance file format reader. Non-null when the current split uses
  // LANCE format. When present, next() pulls from the Lance reader
  // instead of the Velox DWIO row reader. Supports mixed-format scans
  // where some splits are Parquet/ORC and others are Lance.
  std::unique_ptr<pangrova::lance::LanceBatchReader> lance_reader_;
  bool physical_lance_file_ = false;

  // Pinned Iceberg metadata object; read on the worker through its registered
  // filesystem, never materialized in the coordinator or connector JVM.
  std::unique_ptr<IcebergMetadataJsonReader> metadata_reader_;
  std::unique_ptr<PaimonSnapshotJsonReader> paimon_snapshot_reader_;

  // Paimon changelog mode flag for the current split.
  // When true, the split's Parquet file contains a _row_kind column.
  bool changelog_mode_ = false;

  // Subfield pruning: required subfields per top-level column.
  // When non-empty, createReaderPair() adds only the listed subfields
  // to the ScanSpec instead of recursively adding all children.
  // Populated from SplitConfig.required_subfields in prepareSplitState().
  std::unordered_map<std::string, std::vector<std::string>>
      current_required_subfields_;

  // Dynamic filters injected at runtime via addDynamicFilter().
  // Each entry maps an output column index to a Velox Filter.
  // Applied in next() after delete filter but before post-processing.
  // Ordering: read → delete filter → dynamic filter → post-processing.
  // @GuardedBy: dynamic_filter_mutex_
  std::mutex dynamic_filter_mutex_;
  std::vector<std::pair<facebook::velox::column_index_t,
                        std::shared_ptr<facebook::velox::common::Filter>>>
      dynamic_filters_;

  // L2-1: Stored ScanSpec for dynamic filter pushdown to reader level.
  // Set by openVeloxReader() and used to push dynamic filters directly
  // to the DWIO reader's ScanSpec, enabling row-group/page skipping.
  std::shared_ptr<facebook::velox::common::ScanSpec> current_scan_spec_;
  bool dynamic_filters_pushed_to_scan_ = false;
};

// Pangrova's Velox Connector implementation.
// Registered with Velox connector registry so that Velox Task can
// create DataSource instances for table scan operators.
class PangrovaVeloxConnector : public facebook::velox::connector::Connector {
 public:
  static constexpr const char* kConnectorId = "pangrova-velox";

  explicit PangrovaVeloxConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config);

  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx)
      override;

  std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
      facebook::velox::RowTypePtr inputType,
      facebook::velox::connector::ConnectorInsertTableHandlePtr
          connectorInsertTableHandle,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      facebook::velox::connector::CommitStrategy commitStrategy) override;
};

}  // namespace pangrova::velox
