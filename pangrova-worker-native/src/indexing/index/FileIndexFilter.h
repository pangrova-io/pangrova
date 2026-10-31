// FileIndexFilter: Paimon file-level index evaluation for bloom filter,
// bitmap, and BSI (Bit-Sliced Index) based file skipping.
// Returns true if the file MAY contain matching rows (must be read).
// Returns false if the file DEFINITELY does not contain matching rows (skip).
//
// The bloom filter uses Split Block Bloom Filter (SBBF) algorithm, aligned
// with Java's org.pangrova.common.bloom.SplitBlockBloomFilter for cross-language
// compatibility. Hash function: xxHash64 (seed=0), Parquet-compatible.
//
// Placement: index/ — file index filter implementations.
#pragma once

#include <cstdint>
#include <string>

namespace pangrova::index {

/// Supported file index types in Paimon.
enum class FileIndexType { BLOOM_FILTER, BITMAP, BSI };

/// Evaluates file-level indexes to determine if a data file can be skipped
/// during scan. Applied BEFORE reading the file — if the index says
/// "no matching rows", the entire file is skipped (I/O reduction 50-90%).
///
/// Each method returns:
///   true  — file MAY contain matching rows (must be read)
///   false — file DEFINITELY does not contain matching rows (skip)
///
/// The bloom filter uses Split Block Bloom Filter (SBBF): 256-bit blocks with
/// 8 salted sub-hashes per probe, xxHash64 for hashing. Cross-language
/// compatible with Java's SplitBlockBloomFilter. Bitmap uses exact value
/// presence tracking for low-cardinality columns. BSI uses bit-sliced
/// representation for efficient numeric range evaluation.
class FileIndexFilter {
 public:
  /// Check bloom filter (SBBF): does the file potentially contain the given
  /// value in the specified column?
  ///
  /// @param index_data  Serialized SBBF bytes (magic "SBBF" + blocks).
  /// @param column_name Column name the bloom filter was built on.
  /// @param value       The value to probe (UTF-8 bytes).
  /// @return true if the bloom filter indicates the value MAY exist.
  static bool mayContainValue(
      const std::string& index_data,
      const std::string& column_name,
      const std::string& value);

  /// Check bitmap index: does the file contain the given value in a
  /// low-cardinality column?
  ///
  /// @param index_data  Serialized bitmap index bytes from Paimon file index.
  /// @param column_name Column name the bitmap was built on.
  /// @param value       The integer value to check presence for.
  /// @return true if the bitmap indicates the value exists in this file.
  static bool bitmapContains(
      const std::string& index_data,
      const std::string& column_name,
      int32_t value);

  /// Check BSI (Bit-Sliced Index): does the file contain values in the
  /// given numeric range [min_value, max_value]?
  ///
  /// @param index_data  Serialized BSI bytes from Paimon file index.
  /// @param column_name Column name the BSI was built on.
  /// @param min_value   Inclusive lower bound of the range query.
  /// @param max_value   Inclusive upper bound of the range query.
  /// @return true if the BSI indicates values in range MAY exist.
  static bool bsiInRange(
      const std::string& index_data,
      const std::string& column_name,
      int64_t min_value,
      int64_t max_value);
};

}  // namespace pangrova::index
