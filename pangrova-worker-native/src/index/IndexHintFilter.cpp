// IndexHintFilter: Native-side L2 index evaluation implementation.
//
// Loads .idx files, parses predicate JSON, and delegates evaluation to
// FileIndexFilter (bloom/bitmap/BSI). Conservative on any failure — never
// incorrectly skips a file that may contain matching rows.

#include "index/IndexHintFilter.h"

#include <fstream>
#include <sstream>

#include <folly/json.h>

#include "index/FileIndexFilter.h"

namespace pangrova::index {

namespace {

// F-NATIVE-WRITE-10: JSON extraction using folly::parseJson instead of
// hand-written parsers. More robust against whitespace, escaping, and
// structural variations.

/// Extract a string value for a given key from a JSON object.
/// Returns empty string if key not found or parsing fails.
std::string extractJsonString(const std::string& json, const std::string& key) {
  try {
    auto parsed = folly::parseJson(json);
    if (parsed.isObject() && parsed.count(key)) {
      return parsed[key].asString();
    }
  } catch (const std::exception&) {}
  return "";
}

/// Extract an integer value for a given key from a JSON object.
/// Returns 0 if key not found or parsing fails.
int64_t extractJsonInt(const std::string& json, const std::string& key) {
  try {
    auto parsed = folly::parseJson(json);
    if (parsed.isObject() && parsed.count(key)) {
      return parsed[key].asInt();
    }
  } catch (const std::exception&) {}
  return 0;
}

/// Extract a string array for a given key from a JSON object.
/// Only extracts elements that are actually strings; non-string elements
/// (numbers, booleans, nulls) are silently skipped.
/// Returns empty vector if key not found or parsing fails.
std::vector<std::string> extractJsonStringArray(
    const std::string& json,
    const std::string& key) {
  std::vector<std::string> result;
  try {
    auto parsed = folly::parseJson(json);
    if (parsed.isObject() && parsed.count(key)) {
      const auto& arr = parsed[key];
      if (arr.isArray()) {
        for (const auto& item : arr) {
          if (item.isString()) {
            result.push_back(item.asString());
          }
        }
      }
    }
  } catch (const std::exception&) {}
  return result;
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

bool IndexHintFilter::shouldKeepSplit(
    const std::vector<std::tuple<
        std::string, std::string, std::string, std::string>>& index_eval_hints) {
  if (index_eval_hints.empty()) {
    return true; // No hints — keep split.
  }

  for (const auto& [index_type, index_file_path, data_file_path, predicate_json]
       : index_eval_hints) {
    IndexEvalResult result = evaluateHint(index_type, index_file_path, predicate_json);
    if (result == IndexEvalResult::SKIP) {
      // ANY hint proving no match → skip the entire split.
      return false;
    }
  }
  // All hints say KEEP or UNKNOWN — must read the split.
  return true;
}

IndexEvalResult IndexHintFilter::evaluateHint(
    const std::string& index_type,
    const std::string& index_file_path,
    const std::string& predicate_json) {
  // 1. Load index file bytes.
  std::string index_data = loadIndexFile(index_file_path);
  if (index_data.empty()) {
    return IndexEvalResult::UNKNOWN; // Cannot load — conservatively keep.
  }

  // 2. Parse predicate.
  ParsedPredicate predicate = parsePredicate(predicate_json);
  if (predicate.op == PredicateOp::UNSUPPORTED) {
    return IndexEvalResult::UNKNOWN; // Cannot parse — conservatively keep.
  }

  // 3. Dispatch to appropriate FileIndexFilter method.
  if (index_type == "BLOOM_FILTER") {
    if (predicate.op == PredicateOp::EQ) {
      bool mayContain = FileIndexFilter::mayContainValue(
          index_data, predicate.column_name, predicate.value);
      return mayContain ? IndexEvalResult::KEEP : IndexEvalResult::SKIP;
    }
    if (predicate.op == PredicateOp::IN) {
      // For IN predicates, file can be skipped only if ALL values are absent.
      for (const auto& val : predicate.values) {
        if (FileIndexFilter::mayContainValue(index_data, predicate.column_name, val)) {
          return IndexEvalResult::KEEP; // At least one value may be present.
        }
      }
      return IndexEvalResult::SKIP; // None of the values are present.
    }
    // Bloom filter does not support range predicates.
    return IndexEvalResult::UNKNOWN;
  }

  if (index_type == "BITMAP") {
    if (predicate.op == PredicateOp::EQ) {
      try {
        int32_t intValue = std::stoi(predicate.value);
        bool contains = FileIndexFilter::bitmapContains(
            index_data, predicate.column_name, intValue);
        return contains ? IndexEvalResult::KEEP : IndexEvalResult::SKIP;
      } catch (...) {
        return IndexEvalResult::UNKNOWN; // Non-integer value for bitmap.
      }
    }
    if (predicate.op == PredicateOp::IN) {
      for (const auto& val : predicate.values) {
        try {
          int32_t intValue = std::stoi(val);
          if (FileIndexFilter::bitmapContains(
                  index_data, predicate.column_name, intValue)) {
            return IndexEvalResult::KEEP;
          }
        } catch (...) {
          return IndexEvalResult::UNKNOWN;
        }
      }
      return IndexEvalResult::SKIP;
    }
    return IndexEvalResult::UNKNOWN;
  }

  if (index_type == "BSI") {
    if (predicate.op == PredicateOp::RANGE) {
      bool inRange = FileIndexFilter::bsiInRange(
          index_data, predicate.column_name,
          predicate.range_min, predicate.range_max);
      return inRange ? IndexEvalResult::KEEP : IndexEvalResult::SKIP;
    }
    if (predicate.op == PredicateOp::EQ) {
      // EQ can be treated as range [v, v].
      try {
        int64_t eqValue = std::stoll(predicate.value);
        bool inRange = FileIndexFilter::bsiInRange(
            index_data, predicate.column_name, eqValue, eqValue);
        return inRange ? IndexEvalResult::KEEP : IndexEvalResult::SKIP;
      } catch (...) {
        return IndexEvalResult::UNKNOWN;
      }
    }
    return IndexEvalResult::UNKNOWN;
  }

  // Unknown index type — conservatively keep.
  return IndexEvalResult::UNKNOWN;
}

ParsedPredicate IndexHintFilter::parsePredicate(const std::string& predicate_json) {
  ParsedPredicate result;

  std::string op = extractJsonString(predicate_json, "op");
  result.column_name = extractJsonString(predicate_json, "column");

  if (result.column_name.empty()) {
    result.op = PredicateOp::UNSUPPORTED;
    return result;
  }

  if (op == "eq") {
    result.op = PredicateOp::EQ;
    result.value = extractJsonString(predicate_json, "value");
    if (result.value.empty()) {
      result.op = PredicateOp::UNSUPPORTED;
    }
  } else if (op == "in") {
    result.op = PredicateOp::IN;
    result.values = extractJsonStringArray(predicate_json, "values");
    if (result.values.empty()) {
      result.op = PredicateOp::UNSUPPORTED;
    }
  } else if (op == "range") {
    result.op = PredicateOp::RANGE;
    result.range_min = extractJsonInt(predicate_json, "min");
    result.range_max = extractJsonInt(predicate_json, "max");
  } else {
    result.op = PredicateOp::UNSUPPORTED;
  }

  return result;
}

std::string IndexHintFilter::loadIndexFile(const std::string& file_path) {
  std::ifstream file(file_path, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    return ""; // Cannot open — signal failure to caller.
  }

  auto size = file.tellg();
  if (size <= 0) {
    return "";
  }

  file.seekg(0, std::ios::beg);
  std::string buffer(static_cast<size_t>(size), '\0');
  if (!file.read(buffer.data(), size)) {
    return ""; // Read failed.
  }
  return buffer;
}

}  // namespace pangrova::index
