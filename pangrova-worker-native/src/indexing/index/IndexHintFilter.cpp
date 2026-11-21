#include "indexing/index/IndexHintFilter.h"

#include <array>
#include <string_view>

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "indexing/index/FileIndexFilter.h"
#include "engine/observability/PangrovaLog.h"
#include "pangrova/protocol/v2/connector.pb.h"
#include "engine/split/PathUtils.h"
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

std::string readIndexArtifact(const proto::IndexEvalHintDetail& hint) {
  const auto length = hint.index_size_bytes();
  if (length < kIndexHeaderBytes + kIndexTrailerBytes || length > kMaxIndexBytes ||
      hint.index_sha256().size() != SHA256_DIGEST_LENGTH) {
    return {};
  }
  const auto path = split::normalizeToS3Uri(split::stripFilePrefix(hint.index_file_path()));
  auto fs = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!fs) {
    return {};
  }
  auto file = fs->openFileForRead(path);
  if (file->size() != length) {
    return {};
  }
  std::string bytes(length, '\0');
  const auto read = file->pread(0, length, bytes.data());
  std::array<unsigned char, SHA256_DIGEST_LENGTH> sha256;
  unsigned int digestSize = 0;
  if (read.size() != length ||
      EVP_Digest(bytes.data(), bytes.size(), sha256.data(), &digestSize, EVP_sha256(), nullptr) != 1 ||
      digestSize != sha256.size() ||
      std::string_view(reinterpret_cast<const char*>(sha256.data()), digestSize) != hint.index_sha256()) {
    return {};
  }
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
      const auto bloom = bloomPayload(readIndexArtifact(hint));
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

}  // namespace pangrova::index
