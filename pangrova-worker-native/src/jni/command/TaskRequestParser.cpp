// Deserializes TaskRequest protobuf bytes into TaskExecutorConfig.
#include "jni/command/TaskRequestParser.h"

#include <cstring>
#include <stdexcept>
#include <string>

#include "split/SplitConfig.h"

// Conditionally include generated proto header when available.
#if __has_include("pangrova/native/protocol/v1/task.pb.h")
#include "pangrova/native/protocol/v1/task.pb.h"
#include "pangrova/native/protocol/v1/split.pb.h"
#include "pangrova/native/protocol/v1/common.pb.h"
#include "pangrova/native/protocol/v1/shuffle.pb.h"
#define PANGROVA_HAS_PROTO 1
#else
#define PANGROVA_HAS_PROTO 0
#endif

namespace pangrova::jni {

#if PANGROVA_HAS_PROTO

namespace {

namespace proto = pangrova::native::protocol::v1;
using split::ColumnMappingConfig;
using split::DeleteFileConfig;
using split::IcebergConfig;
using split::PaimonConfig;
using split::SplitConfig;
using split::TypePromotionConfig;
using executor::ResourceLimitConfig;

split::FileFormatType parseFileFormat(proto::FileFormat fmt) {
  switch (fmt) {
    case proto::FILE_FORMAT_ORC: return split::FileFormatType::ORC;
    case proto::FILE_FORMAT_PARQUET: return split::FileFormatType::PARQUET;
    case proto::FILE_FORMAT_LANCE: return split::FileFormatType::LANCE;
    default: return split::FileFormatType::PARQUET;
  }
}

std::string deleteTypeToString(proto::DeleteFileType type) {
  switch (type) {
    case proto::DELETE_FILE_TYPE_POSITION: return "position";
    case proto::DELETE_FILE_TYPE_EQUALITY: return "equality";
    case proto::DELETE_FILE_TYPE_DELETION_VECTOR: return "deletion_vector";
    default: return "unknown";
  }
}

std::string icebergReadModeToString(proto::IcebergReadMode mode) {
  switch (mode) {
    case proto::ICEBERG_READ_TIME_TRAVEL: return "TIME_TRAVEL";
    case proto::ICEBERG_READ_INCREMENTAL: return "INCREMENTAL";
    default: return "FULL";
  }
}

std::string icebergContentToString(int32_t content) {
  switch (content) {
    case 1: return "POSITION_DELETES";
    case 2: return "EQUALITY_DELETES";
    default: return "DATA";
  }
}

std::string paimonReadModeToString(proto::PaimonReadMode mode) {
  switch (mode) {
    case proto::PAIMON_READ_MODE_APPEND: return "APPEND";
    case proto::PAIMON_READ_MODE_PRIMARY_KEY: return "PRIMARY_KEY";
    case proto::PAIMON_READ_MODE_CHANGELOG: return "CHANGELOG";
    case proto::PAIMON_READ_MODE_INCREMENTAL: return "INCREMENTAL";
    default: return "FULL";
  }
}

std::string changelogProducerToString(proto::PaimonChangelogProducer producer) {
  switch (producer) {
    case proto::PAIMON_CHANGELOG_INPUT: return "INPUT";
    case proto::PAIMON_CHANGELOG_LOOKUP: return "LOOKUP";
    case proto::PAIMON_CHANGELOG_FULL_COMPACTION: return "FULL_COMPACTION";
    default: return "NONE";
  }
}

std::string mergeEngineToString(proto::PaimonMergeEngine engine) {
  switch (engine) {
    case proto::PAIMON_MERGE_PARTIAL_UPDATE: return "partial_update";
    case proto::PAIMON_MERGE_AGGREGATION:    return "aggregation";
    case proto::PAIMON_MERGE_FIRST_ROW:      return "first_row";
    default: return "deduplicate";
  }
}

IcebergConfig parseIcebergInfo(const proto::IcebergSplitInfo& info) {
  IcebergConfig cfg;
  cfg.snapshot_id = info.snapshot_id();
  cfg.data_sequence_number = info.data_sequence_number();
  cfg.spec_id = info.spec_id();
  cfg.content = icebergContentToString(info.content());
  cfg.read_mode = icebergReadModeToString(info.read_mode());
  cfg.from_snapshot_id = info.from_snapshot_id();
  cfg.to_snapshot_id = info.to_snapshot_id();
  cfg.delete_files.reserve(info.delete_files_size());
  for (const auto& df : info.delete_files()) {
    DeleteFileConfig dfc;
    dfc.file_path = df.file_path();
    dfc.delete_type = deleteTypeToString(df.delete_type());
    dfc.equality_fields.assign(
        df.equality_fields().begin(), df.equality_fields().end());
    dfc.record_count = df.record_count();
    dfc.data_sequence_number = df.data_sequence_number();
    dfc.dv_file_path = df.dv_file_path();
    cfg.delete_files.push_back(std::move(dfc));
  }
  return cfg;
}

PaimonConfig parsePaimonInfo(const proto::PaimonSplitInfo& info) {
  PaimonConfig cfg;
  cfg.merge_engine = mergeEngineToString(info.merge_engine());
  cfg.changelog_mode = info.changelog_mode();
  // Backward compatibility: derive changelog_mode from read_mode if not explicitly set.
  if (!cfg.changelog_mode &&
      info.read_mode() == proto::PAIMON_READ_MODE_CHANGELOG) {
    cfg.changelog_mode = true;
  }
  cfg.primary_keys.assign(
      info.primary_keys().begin(), info.primary_keys().end());
  cfg.bucket_id = info.bucket_id();
  cfg.snapshot_id = info.snapshot_id();
  cfg.from_snapshot_id = info.from_snapshot_id();
  cfg.to_snapshot_id = info.to_snapshot_id();
  cfg.read_mode = paimonReadModeToString(info.read_mode());
  cfg.changelog_producer = changelogProducerToString(info.changelog_producer());
  // MOW deletion vector fields.
  cfg.deletion_vectors_enabled = info.deletion_vectors_enabled();
  cfg.deletion_vector_file_path = info.deletion_vector_file_path();
  // MOR merge control.
  cfg.needs_merge = info.needs_merge();
  // Sequence column for ordering during merge.
  cfg.sequence_column = info.sequence_column();
  // Aggregation specs: column → function mapping.
  cfg.aggregation_specs.insert(
      info.aggregation_specs().begin(), info.aggregation_specs().end());
  // Paimon partition values.
  cfg.partition_values.insert(
      info.partition_values().begin(), info.partition_values().end());
  // LSM levels.
  cfg.lsm_levels.reserve(info.lsm_levels_size());
  for (const auto& level : info.lsm_levels()) {
    split::LsmLevelConfig lsm;
    lsm.level = level.level();
    lsm.file_paths.assign(
        level.file_paths().begin(), level.file_paths().end());
    lsm.total_record_count = level.total_record_count();
    cfg.lsm_levels.push_back(std::move(lsm));
  }
  return cfg;
}

SplitConfig parseSplit(const proto::SplitInfo& split) {
  SplitConfig cfg;
  cfg.split_id = split.split_id();
  cfg.connector_id = split.connector_id();
  // P0-CHAIN-4: Parse table_name for multi-table join split routing.
  cfg.table_name = split.table_name();
  cfg.file_path = split.file_path();
  cfg.file_format = parseFileFormat(split.file_format());
  cfg.start_offset = split.start_offset();
  cfg.length = split.length();

  // Top-level partition values (used by Iceberg; Paimon uses PaimonConfig.partition_values).
  cfg.partition_values.insert(
      split.partition_values().begin(), split.partition_values().end());

  // Legacy column_mapping field (SplitInfo.column_mapping, field 7).
  for (const auto& cm : split.column_mapping()) {
    ColumnMappingConfig cmc;
    cmc.source_id = cm.source_field_id();
    cmc.target_id = cm.target_field_id();
    cmc.source_name = cm.source_name();
    cmc.target_name = cm.target_name();
    cmc.default_value = cm.default_value();
    cfg.column_mappings.push_back(std::move(cmc));
  }

  // Schema evolution (SplitInfo.schema_evolution, field 11).
  // Overrides legacy column_mapping if present.
  if (split.has_schema_evolution()) {
    const auto& se = split.schema_evolution();
    if (se.column_mappings_size() > 0) {
      cfg.column_mappings.clear();
      cfg.column_mappings.reserve(se.column_mappings_size());
      for (const auto& cm : se.column_mappings()) {
        ColumnMappingConfig cmc;
        cmc.source_id = cm.source_field_id();
        cmc.target_id = cm.target_field_id();
        cmc.source_name = cm.source_name();
        cmc.target_name = cm.target_name();
        cmc.default_value = cm.default_value();
        cfg.column_mappings.push_back(std::move(cmc));
      }
    }
    cfg.type_promotions.reserve(se.type_promotions_size());
    for (const auto& tp : se.type_promotions()) {
      TypePromotionConfig tpc;
      tpc.field_id = tp.field_id();
      tpc.source_type = tp.source_type();
      tpc.target_type = tp.target_type();
      tpc.column_name = tp.column_name();
      cfg.type_promotions.push_back(std::move(tpc));
    }
  }

  if (split.has_iceberg_info()) {
    cfg.iceberg = parseIcebergInfo(split.iceberg_info());
  }
  if (split.has_paimon_info()) {
    cfg.paimon = parsePaimonInfo(split.paimon_info());
  }
  return cfg;
}

ResourceLimitConfig parseResourceLimit(const proto::ResourceLimit& rl) {
  ResourceLimitConfig cfg;
  cfg.max_memory_bytes = rl.max_memory_bytes();
  cfg.max_threads = rl.max_threads();
  cfg.max_spill_bytes = rl.max_spill_bytes();
  cfg.spill_directory = rl.spill_directory();
  return cfg;
}

executor::WriteConfigInfo parseWriteConfig(const proto::WriteConfig& wc) {
  executor::WriteConfigInfo cfg;
  cfg.target_table = wc.target_table();
  cfg.write_mode = wc.write_mode();
  cfg.partition_spec.assign(
      wc.partition_spec().begin(), wc.partition_spec().end());
  cfg.transaction_id = wc.transaction_id();
  cfg.file_format = parseFileFormat(wc.file_format());
  cfg.target_file_size = wc.target_file_size();
  return cfg;
}

executor::TaskExecutorConfig parseFromProto(
    const std::vector<uint8_t>& data) {
  executor::TaskExecutorConfig config;
  proto::TaskRequest request;
  if (!request.ParseFromArray(
          data.data(), static_cast<int>(data.size()))) {
    throw std::runtime_error(
        "Failed to parse TaskRequest protobuf, data_size=" +
        std::to_string(data.size()));
  }

  config.task_id = request.task_id();
  config.query_id = request.query_id();
  // task_type: Mapped from Java-side TaskType enum via TaskTypeMapper.toProtoType().
  // Currently used for observability (structured logging) and future task-type-specific
  // resource allocation. The C++ side stores it as int32_t but does not branch on it —
  // all task types follow the same Velox execution path. If task-type-specific behavior
  // is needed in native (e.g., different memory limits for shuffle vs scan tasks),
  // add dispatch logic in VeloxTaskRunner::start() keyed on this field.
  config.task_type = static_cast<int32_t>(request.task_type());

  const auto& plan = request.substrait_plan();
  config.substrait_plan.assign(plan.begin(), plan.end());

  config.splits.reserve(request.splits_size());
  for (const auto& s : request.splits()) {
    config.splits.push_back(parseSplit(s));
  }

  config.session_properties.insert(
      request.session_properties().begin(),
      request.session_properties().end());

  if (request.has_resource_limit()) {
    config.resource_limit = parseResourceLimit(request.resource_limit());
  }
  if (request.has_write_config()) {
    config.write_config = parseWriteConfig(request.write_config());
  }

  // Parse shuffle configuration.
  if (request.has_shuffle_config()) {
    const auto& sc = request.shuffle_config();
    config.num_shuffle_partitions = sc.num_partitions();
    config.shuffle_partition_key_indices.assign(
        sc.partition_key_indices().begin(),
        sc.partition_key_indices().end());
  }

  return config;
}

}  // namespace

#endif  // PANGROVA_HAS_PROTO

namespace {

void applyDefaults(executor::TaskExecutorConfig& config) {
  if (config.task_id.empty()) config.task_id = "task-unknown";
  if (config.query_id.empty()) config.query_id = "query-unknown";
}

}  // namespace

executor::TaskExecutorConfig parseTaskConfig(
    const std::vector<uint8_t>& data) {
  if (data.empty()) {
    executor::TaskExecutorConfig config;
    applyDefaults(config);
    return config;
  }

#if PANGROVA_HAS_PROTO
  auto config = parseFromProto(data);
  applyDefaults(config);
  return config;
#else
  throw std::runtime_error(
      "Failed to parse TaskRequest: protobuf support not compiled, data_size=" +
      std::to_string(data.size()));
#endif
}

executor::TaskExecutorConfig parseCreateExecutorCommand(
    const std::vector<uint8_t>& data) {
  if (data.empty()) {
    executor::TaskExecutorConfig config;
    applyDefaults(config);
    return config;
  }

#if PANGROVA_HAS_PROTO
  proto::CreateExecutorCommand cmd;
  if (!cmd.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
    throw std::runtime_error(
        "Failed to parse CreateExecutorCommand, data_size=" +
        std::to_string(data.size()));
  }

  // Extract inner TaskRequest bytes and parse into config.
  const auto& task_request = cmd.task_request();
  std::vector<uint8_t> request_bytes(task_request.begin(), task_request.end());
  auto config = parseTaskConfig(request_bytes);

  // Apply the is_result_task flag from the command envelope.
  config.is_result_task = cmd.is_result_task();

  return config;
#else
  throw std::runtime_error(
      "Failed to parse CreateExecutorCommand: protobuf support not compiled, data_size=" +
      std::to_string(data.size()));
#endif
}

}  // namespace pangrova::jni
