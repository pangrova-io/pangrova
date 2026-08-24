// IndexHintFilter: Native-side L2 index evaluation for split skipping.
//
// Evaluates IndexEvalHintInfo proto messages attached to splits by the
// coordinator. For each hint, loads the .idx file from storage, parses
// the predicate JSON, and delegates to FileIndexFilter for evaluation.
//
// Thread-safe: all methods are stateless static functions.
//
// Placement: index/ — alongside FileIndexFilter.
#pragma once

#include <string>
#include <vector>

namespace pangrova::index {

/// Result of evaluating a single index hint.
enum class IndexEvalResult {
  SKIP,    // File definitely has no matching rows — skip.
  KEEP,    // File may contain matching rows — must read.
  UNKNOWN  // Cannot evaluate (missing file, bad format) — conservatively keep.
};

/// Predicate operation types supported in predicate_json.
enum class PredicateOp { EQ, IN, RANGE, UNSUPPORTED };

/// Parsed predicate structure extracted from predicate_json.
struct ParsedPredicate {
  PredicateOp op = PredicateOp::UNSUPPORTED;
  std::string column_name;
  std::string value;               // For EQ
  std::vector<std::string> values; // For IN
  int64_t range_min = 0;           // For RANGE
  int64_t range_max = 0;           // For RANGE
};

/// Evaluates index hints attached to splits for native-side data skipping.
///
/// The coordinator attaches IndexEvalHintInfo messages to each split,
/// instructing the worker to load specific index files and evaluate
/// predicates before reading data files. This avoids full-file I/O when
/// the index proves no rows match.
///
/// Integration point: called by PlanExecutionDriver after receiving split
/// configuration and before scheduling file reads.
class IndexHintFilter {
 public:
  /// Evaluate all index hints for a split. Returns true if the split
  /// should be kept (at least one data file may match), false if ALL
  /// hints indicate the data file can be skipped.
  ///
  /// AND semantics: if ANY single hint says SKIP, the file is skipped.
  /// This is safe because each hint represents a necessary condition.
  ///
  /// @param index_eval_hints  Vector of (index_type, index_file_path,
  ///                          data_file_path, predicate_json) tuples
  ///                          extracted from SplitInfo proto.
  /// @return true = keep split, false = skip split.
  static bool shouldKeepSplit(
      const std::vector<std::tuple<
          std::string,  // index_type
          std::string,  // index_file_path
          std::string,  // data_file_path
          std::string   // predicate_json
          >>& index_eval_hints);

  /// Evaluate a single index hint against a data file.
  ///
  /// @param index_type      Index type: "BLOOM_FILTER", "BITMAP", or "BSI".
  /// @param index_file_path Path to the .idx file on local/remote storage.
  /// @param predicate_json  JSON predicate, e.g.:
  ///                        {"op":"eq","column":"user_id","value":"abc123"}
  ///                        {"op":"range","column":"amount","min":100,"max":999}
  /// @return IndexEvalResult indicating skip/keep/unknown.
  static IndexEvalResult evaluateHint(
      const std::string& index_type,
      const std::string& index_file_path,
      const std::string& predicate_json);

  /// Parse a predicate JSON string into a structured ParsedPredicate.
  ///
  /// Supported formats:
  ///   {"op":"eq","column":"col","value":"val"}
  ///   {"op":"in","column":"col","values":["v1","v2"]}
  ///   {"op":"range","column":"col","min":10,"max":99}
  ///
  /// @param predicate_json  Raw JSON string.
  /// @return Parsed predicate, or PredicateOp::UNSUPPORTED on failure.
  static ParsedPredicate parsePredicate(const std::string& predicate_json);

 private:
  /// Load index file bytes from disk.
  /// Returns empty string on I/O failure (caller treats as UNKNOWN).
  static std::string loadIndexFile(const std::string& file_path);
};

}  // namespace pangrova::index
