#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace pangrova::split {

// File format for data files. Mirrors proto FileFormat enum.
enum class FileFormatType : uint8_t {
  PARQUET = 0,
  ORC = 1,
  LANCE = 2,
};

// Convert FileFormatType to string for logging/error messages.
inline const char* fileFormatName(FileFormatType fmt) {
  switch (fmt) {
    case FileFormatType::PARQUET: return "parquet";
    case FileFormatType::ORC: return "orc";
    case FileFormatType::LANCE: return "lance";
  }
  return "parquet";
}

// Delete file descriptor for Iceberg splits.
struct DeleteFileConfig {
  std::string file_path;
  std::string delete_type;  // "position", "equality", or "deletion_vector"
  std::vector<std::string> equality_fields;
  int64_t record_count = 0;
  int64_t data_sequence_number = 0;
  // Deletion vector file path for Iceberg v3 / Paimon MOW DV support.
  // Only populated when delete_type == "deletion_vector".
  std::string dv_file_path;
};

// Iceberg-specific split configuration.
struct IcebergConfig {
  int64_t snapshot_id = 0;
  int64_t data_sequence_number = 0;
  int spec_id = 0;
  std::string content;  // "DATA", "POSITION_DELETES", "EQUALITY_DELETES"
  std::string read_mode;  // "FULL", "INCREMENTAL", or "TIME_TRAVEL"
  int64_t from_snapshot_id = 0;
  int64_t to_snapshot_id = 0;
  std::vector<DeleteFileConfig> delete_files;
};

// LSM level descriptor for Paimon splits.
struct LsmLevelConfig {
  int level = 0;
  std::vector<std::string> file_paths;
  int64_t total_record_count = 0;
};

// Paimon-specific split configuration.
struct PaimonConfig {
  std::string merge_engine;  // "deduplicate", "partial_update", etc.
  bool changelog_mode = false;
  std::vector<std::string> primary_keys;
  int bucket_id = -1;
  int64_t snapshot_id = 0;
  int64_t from_snapshot_id = 0;
  int64_t to_snapshot_id = 0;
  std::string read_mode;  // "FULL" or "INCREMENTAL"
  std::string changelog_producer;
  std::vector<LsmLevelConfig> lsm_levels;
  // MOW (Merge-On-Write) deletion vector fields.
  bool deletion_vectors_enabled = false;
  std::string deletion_vector_file_path;
  // MOR merge control: false = COW / read-optimized (skip merge).
  bool needs_merge = false;
  // Sequence column for ordering during merge.
  std::string sequence_column;
  // Aggregation merge engine: column name → aggregation function mapping.
  std::unordered_map<std::string, std::string> aggregation_specs;
  // Paimon partition column values.
  std::unordered_map<std::string, std::string> partition_values;
};

// Column mapping for schema evolution.
struct ColumnMappingConfig {
  int source_id = 0;
  int target_id = 0;
  std::string source_name;
  std::string target_name;
  // Default value for missing columns (schema evolution).
  // When non-empty, missing columns are filled with this value
  // instead of null. Format: string representation of the value,
  // parsed according to the target column type.
  std::string default_value;
};

// Type promotion for schema evolution.
struct TypePromotionConfig {
  int field_id = 0;
  std::string source_type;
  std::string target_type;
  // Column name for name-based matching (preferred over field_id index).
  std::string column_name;
};

// Simplified split configuration mirroring proto SplitInfo.
// Used until proto generation is available.
struct SplitConfig {
  std::string split_id;
  std::string connector_id;  // "iceberg" or "paimon"
  std::string table_name;    // Logical table name for split routing in multi-table joins.
  std::string file_path;
  FileFormatType file_format = FileFormatType::PARQUET;
  int64_t start_offset = 0;
  int64_t length = 0;

  // Partition column values (shared by Iceberg; Paimon uses PaimonConfig.partition_values).
  std::unordered_map<std::string, std::string> partition_values;

  // Legacy typed config — kept for backward compatibility with existing
  // code that reads them directly. Factories should prefer these fields.
  std::optional<IcebergConfig> iceberg;
  std::optional<PaimonConfig> paimon;

  // Schema evolution
  std::vector<ColumnMappingConfig> column_mappings;
  std::vector<TypePromotionConfig> type_promotions;

  // Subfield pruning: when non-empty, only the listed subfields of nested
  // columns (STRUCT/MAP/ARRAY) are materialized by the reader. Format:
  // top-level column name → list of required subfield paths (dot-separated).
  // Empty map means read all subfields (no pruning).
  std::unordered_map<std::string, std::vector<std::string>> required_subfields;
};

}  // namespace pangrova::split
