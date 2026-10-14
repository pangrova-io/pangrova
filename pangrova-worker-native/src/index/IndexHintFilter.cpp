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
#include "observability/PangrovaLog.h"
#include "pangrova/protocol/v2/connector.pb.h"
#include "split/PathUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::index {

namespace {

namespace proto = pangrova::protocol::v2;
constexpr uint32_t kIndexMagic = 0x41455448;  // AETH
constexpr size_t kIndexHeaderBytes = 16;
constexpr size_t kIndexTrailerBytes = 4;
constexpr uint64_t kMaxIndexBytes = 50ULL * 1024 * 1024;

uint32_t readU32(const unsigned char* p) {
  return (static_cast<uint32_t>(p[0]) << 24) |
      (static_cast<uint32_t>(p[1]) << 16) |
      (static_cast<uint32_t>(p[2]) << 8) | p[3];
}

uint16_t readU16(const unsigned char* p) {
  return static_cast<uint16_t>((p[0] << 8) | p[1]);
}

uint32_t crc32(const unsigned char* bytes, size_t length) {
  uint32_t crc = 0xffffffffU;
  for (size_t i = 0; i < length; ++i) {
    crc ^= bytes[i];
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
  }
  return ~crc;
}

std::string readIndexArtifact(const std::string& uri) {
  const auto path = split::normalizeToS3Uri(split::stripFilePrefix(uri));
  auto fs = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!fs) {
    return {};
  }
  auto file = fs->openFileForRead(path);
  const auto length = file->size();
  if (length < kIndexHeaderBytes + kIndexTrailerBytes || length > kMaxIndexBytes) {
    return {};
  }
  std::string bytes(length, '\0');
  file->pread(0, length, bytes.data());
  return bytes;
}

// IndexSerializer's v1 envelope: header, BloomFilterSerializer payload, CRC32.
// A future or corrupt artifact must never produce a negative membership result.
std::string bloomPayload(const std::string& bytes) {
  if (bytes.size() < kIndexHeaderBytes + kIndexTrailerBytes + 16) {
    return {};
  }
  const auto* raw = reinterpret_cast<const unsigned char*>(bytes.data());
  const auto payload = static_cast<size_t>(readU32(raw + 12));
  if (readU32(raw) != kIndexMagic || readU16(raw + 4) != 1 ||
      readU16(raw + 6) != 3 || payload > bytes.size() - kIndexHeaderBytes - kIndexTrailerBytes ||
      bytes.size() != kIndexHeaderBytes + payload + kIndexTrailerBytes ||
      readU32(raw + kIndexHeaderBytes + payload) !=
          crc32(raw, kIndexHeaderBytes + payload)) {
    return {};
  }
  const auto* value = raw + kIndexHeaderBytes;
  if (payload < 16) {
    return {};
  }
  const auto bloom_length = static_cast<size_t>(readU32(value + 12));
  if (bloom_length == 0 || bloom_length != payload - 16 ||
      readU32(value) != 8 ||
      readU32(value + 4) != 0 || readU32(value + 8) == 0) {
    return {};
  }
  return bytes.substr(kIndexHeaderBytes + 16, bloom_length);
}

bool canonicalValue(const proto::ComparableValue& value, std::string& result) {
  switch (value.value_case()) {
    case proto::ComparableValue::kStringValue:
      result = value.string_value();
      return true;
    case proto::ComparableValue::kInt64Value:
      result = std::to_string(value.int64_value());
      return true;
    case proto::ComparableValue::kInt32Value:
      result = std::to_string(value.int32_value());
      return true;
    case proto::ComparableValue::kBoolValue:
      result = value.bool_value() ? "true" : "false";
      return true;
    default:
      // Floating-point decimal canonicalization must exactly match Java's
      // IndexValueNormalizer before it can participate in safe pruning.
      return false;
  }
}

bool bloomProvesAbsent(const proto::IndexPredicate& predicate,
                      const std::string& bloom) {
  auto absent = [&](const proto::ComparableValue& value) {
    std::string key;
    return canonicalValue(value, key) && !key.empty() &&
        !FileIndexFilter::mayContainValue(bloom, predicate.column(), key);
  };
  if (predicate.has_equal_to()) {
    return absent(predicate.equal_to().value());
  }
  if (predicate.has_in_list() && predicate.in_list().values_size() > 0) {
    for (const auto& value : predicate.in_list().values()) {
      if (!absent(value)) {
        return false;
      }
    }
    return true;
  }
  return false;
}

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

bool IndexHintFilter::shouldKeepTypedSplit(
    const std::string& file_scan_detail,
    const std::string& data_file_path,
    bool* evaluated) {
  if (evaluated) {
    *evaluated = false;
  }
  proto::FileSplitDetail detail;
  if (!detail.ParseFromString(file_scan_detail)) {
    return true;
  }
  for (const auto& hint : detail.index_eval_hints()) {
    if (hint.data_file_path() != data_file_path ||
        hint.index_type() != "BLOOM_FILTER" || hint.index_file_path().empty() ||
        !hint.has_predicate()) {
      continue;
    }
    if (evaluated) {
      *evaluated = true;
    }
    try {
      const auto bloom = bloomPayload(readIndexArtifact(hint.index_file_path()));
      if (bloom.empty()) {
        PANGROVA_LOG_WARN << "event=native_index_artifact_unavailable file="
                          << hint.index_file_path();
        continue;
      }
      if (bloomProvesAbsent(hint.predicate(), bloom)) {
        return false;
      }
    } catch (const std::exception& failure) {
      PANGROVA_LOG_WARN << "event=native_index_artifact_read_failed file="
                        << hint.index_file_path() << " error=" << failure.what();
      // Index I/O is advisory. Any inaccessible artifact retains the split.
    }
  }
  return true;
}

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
