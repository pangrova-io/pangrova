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
  METADATA_JSON = 3,
  METADATA_DIRECTORY = 4,
  METADATA_AVRO = 5,
};

// Convert FileFormatType to string for logging/error messages.
inline const char* fileFormatName(FileFormatType fmt) {
  switch (fmt) {
    case FileFormatType::PARQUET: return "parquet";
    case FileFormatType::ORC: return "orc";
    case FileFormatType::LANCE: return "lance";
    case FileFormatType::METADATA_JSON: return "metadata_json";
    case FileFormatType::METADATA_DIRECTORY: return "metadata_directory";
    case FileFormatType::METADATA_AVRO: return "metadata_avro";
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

enum class SystemTableKind : uint8_t {
  ICEBERG_SNAPSHOTS,
  ICEBERG_HISTORY,
  PAIMON_SNAPSHOTS,
  PAIMON_TAGS,
  PAIMON_OPTIONS,
  PAIMON_SCHEMAS,
  PAIMON_BRANCHES,
  ICEBERG_MANIFESTS,
};

struct IcebergManifestPartitionField {
  std::string transform;
  std::string result_type;
};

struct IcebergManifestPartitionSpec {
  int32_t spec_id = 0;
  std::vector<IcebergManifestPartitionField> fields;
};

struct ColumnMappingConfig {
  int source_id = 0;
  int target_id = 0;
  std::string source_name;
  std::string target_name;
  std::string default_value;
};

struct TypePromotionConfig {
  int field_id = 0;
  std::string source_type;
  std::string target_type;
  std::string column_name;
};

struct PhysicalFileConfig {
  std::string file_path;
  FileFormatType file_format = FileFormatType::PARQUET;
  uint64_t file_size = 0;
  std::string file_scan_detail;
  uint64_t min_sequence_number = 0;
  uint64_t max_sequence_number = 0;
  std::vector<ColumnMappingConfig> column_mappings;
  std::vector<TypePromotionConfig> type_promotions;
};

// Every LSM file keeps its own physical format, immutable size and row domain.
struct LsmLevelConfig {
  int level = 0;
  std::vector<PhysicalFileConfig> files;
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
  // MOR merge control: false = COW / read-optimized (skip merge).
  bool needs_merge = false;
  bool ignore_delete = false;
  bool remove_record_on_delete = false;
  // Sequence column for ordering during merge.
  std::string sequence_column;
  // Aggregation merge engine: column name → aggregation function mapping.
  std::unordered_map<std::string, std::string> aggregation_specs;
  // Paimon partition column values.
  std::unordered_map<std::string, std::string> partition_values;
};

// Simplified split configuration mirroring proto SplitInfo.
// Used until proto generation is available.
struct SplitConfig {
  std::string split_id;
  std::string connector_id;  // "iceberg" or "paimon"
  std::string table_name;    // Logical table name for split routing in multi-table joins.
  std::string source_snapshot;  // Immutable table snapshot pinned by the query.
  std::string data_object_id;   // Stable object identity within source_snapshot.
  // JNI-local catalog credentials and canonical FileSplitDetail read unit.
  std::string storage_binding;
  std::string file_scan_detail;
  bool physical_lance_file = false;
  std::string file_path;
  FileFormatType file_format = FileFormatType::PARQUET;
  int64_t start_offset = 0;
  int64_t length = 0;

  // Partition column values (shared by Iceberg; Paimon uses PaimonConfig.partition_values).
  std::unordered_map<std::string, std::string> partition_values;

  // Typed connector configuration consumed by reader factories.
  std::optional<IcebergConfig> iceberg;
  std::optional<PaimonConfig> paimon;
  std::optional<SystemTableKind> system_table;
  uint64_t system_table_snapshot_id = 0;
  std::string system_table_object_sha256;
  std::string system_table_tag_name;
  std::optional<uint64_t> system_table_schema_id;
  std::string system_table_branch_name;
  int64_t system_table_directory_mtime_ms = 0;
  uint64_t system_table_directory_device = 0;
  uint64_t system_table_directory_inode = 0;
  int32_t system_table_directory_zone_offset_seconds = 0;
  int32_t system_table_iceberg_format_version = 0;
  uint64_t system_table_iceberg_snapshot_sequence_number = 0;
  std::vector<IcebergManifestPartitionSpec> system_table_iceberg_partition_specs;

  // Schema evolution
  std::vector<ColumnMappingConfig> column_mappings;
  std::vector<TypePromotionConfig> type_promotions;

  // Subfield pruning: when non-empty, only the listed subfields of nested
  // columns (STRUCT/MAP/ARRAY) are materialized by the reader. Format:
  // top-level column name → list of required subfield paths (dot-separated).
  // Empty map means read all subfields (no pruning).
  std::unordered_map<std::string, std::vector<std::string>> required_subfields;

  // Native retrieval may bind a source-object-scoped set of physical row
  // positions. The separate enabled bit distinguishes an empty ANN result
  // (scan no rows) from an ordinary exact scan (no filter).
  bool row_position_filter_enabled = false;
  std::vector<uint64_t> row_position_allowlist;
  // Some retrieval capabilities (for example BM25) materialize their exact
  // score while searching the immutable artifact. The scan injects this
  // bounded source-object map into a reserved DOUBLE column; it is never
  // serialized across the Coordinator control plane.
  bool materialized_retrieval_score_enabled = false;
  std::unordered_map<uint64_t, double> materialized_retrieval_scores;
};

}  // namespace pangrova::split
