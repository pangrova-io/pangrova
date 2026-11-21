#include "engine/delete/io/DeletionVectorReader.h"

#include <cstring>
#include <charconv>
#include <optional>
#include <limits>
#include <zlib.h>

#include "engine/split/PathUtils.h"
#include <fstream>
#include <stdexcept>

#include <folly/json.h>

#if __has_include("velox/common/file/FileSystems.h")
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#define PANGROVA_HAS_VELOX_FS 1
#else
#define PANGROVA_HAS_VELOX_FS 0
#endif

namespace pangrova::delete_filter::io {

namespace {

// Decode a 4-byte little-endian integer from raw bytes.
int32_t decodeInt32LE(const char* data) {
  return static_cast<int32_t>(
      static_cast<uint8_t>(data[0]) |
      (static_cast<uint8_t>(data[1]) << 8) |
      (static_cast<uint8_t>(data[2]) << 16) |
      (static_cast<uint8_t>(data[3]) << 24));
}

// Read file bytes using std::ifstream (local filesystem fallback).
std::string readLocalFileBytes(const std::string& filePath) {
  std::ifstream file(filePath, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    throw std::runtime_error(
        "Cannot open deletion vector file: " + filePath);
  }
  auto fileSize = file.tellg();
  if (fileSize <= 0) {
    throw std::runtime_error(
        "Deletion vector file is empty or unreadable: " + filePath);
  }
  file.seekg(0, std::ios::beg);
  std::string content(static_cast<size_t>(fileSize), '\0');
  if (!file.read(content.data(), fileSize)) {
    throw std::runtime_error(
        "Failed to read deletion vector file: " + filePath);
  }
  return content;
}

}  // namespace

std::string DeletionVectorReader::readFileBytes(const std::string& filePath) {
#if PANGROVA_HAS_VELOX_FS
  // Prefer Velox FileSystem API for S3/HDFS/GCS/Local support.
  // Fall back to local ifstream for paths without a registered scheme
  // (e.g., plain local paths in unit tests before filesystem registration).
  try {
    auto fs = facebook::velox::filesystems::getFileSystem(
        split::normalizeToS3Uri(split::stripFilePrefix(filePath)), nullptr);
    if (fs) {
      auto readFile = fs->openFileForRead(
          split::normalizeToS3Uri(split::stripFilePrefix(filePath)));
      auto fileSize = readFile->size();
      if (fileSize == 0) {
        throw std::runtime_error(
            "Deletion vector file is empty: " + filePath);
      }
      std::string content(fileSize, '\0');
      readFile->pread(0, fileSize, content.data());
      return content;
    }
  } catch (const std::runtime_error&) {
    // Re-throw our own runtime_error (e.g., empty file).
    throw;
  } catch (const std::exception&) {
    // Velox filesystem error (VeloxUserError, VeloxRuntimeError, etc.)
    // — fall through to local ifstream read.
  }
#endif
  // Fallback: read via std::ifstream (local filesystem only).
  return readLocalFileBytes(filePath);
}

void DeletionVectorReader::validatePuffinMagic(
    const std::string& data,
    size_t offset,
    const std::string& context) {
  if (offset + PuffinConstants::kMagicSize > data.size()) {
    throw std::runtime_error(
        "Puffin file too small for magic bytes at " + context +
        ": size=" + std::to_string(data.size()));
  }
  if (std::memcmp(
          data.data() + offset,
          PuffinConstants::kMagic,
          PuffinConstants::kMagicSize) != 0) {
    throw std::runtime_error(
        "Invalid Puffin magic bytes at " + context +
        ": expected PFA1");
  }
}

DeletionVectorReader::BlobLocation DeletionVectorReader::parsePuffinFooter(
    const std::string& content, const std::string& dataFilePath) {
  // Puffin: header magic, blobs, footer magic, JSON, size/flags/end magic.
  const auto size = content.size();
  if (size < 2 * PuffinConstants::kMagicSize + PuffinConstants::kFooterFixedSize) {
    throw std::runtime_error("Puffin file is too small");
  }
  validatePuffinMagic(content, size - 4, "footer-end");
  const auto fixed = size - PuffinConstants::kFooterFixedSize;
  const auto payloadSize = decodeInt32LE(content.data() + fixed);
  if (payloadSize <= 0 || static_cast<size_t>(payloadSize) > fixed - 8) {
    throw std::runtime_error("Invalid Puffin footer size");
  }
  if (decodeInt32LE(content.data() + fixed + 4) != 0) {
    throw std::runtime_error("Compressed or unknown Puffin footer flags are unsupported");
  }
  const auto payloadStart = fixed - static_cast<size_t>(payloadSize);
  const auto blobEnd = payloadStart - 4;
  validatePuffinMagic(content, blobEnd, "footer-start");
  const auto footer = folly::parseJson(content.substr(payloadStart, payloadSize));
  if (!footer.isObject() || !footer.count("blobs") || !footer["blobs"].isArray()) {
    throw std::runtime_error("Puffin footer has no blob inventory");
  }
  std::optional<BlobLocation> selected;
  for (const auto& blob : footer["blobs"]) {
    if (!blob.isObject() || !blob.count("type") ||
        blob["type"].asString() != "deletion-vector-v1") {
      continue;
    }
    if (!blob.count("properties") || !blob["properties"].isObject()) {
      throw std::runtime_error("Deletion vector lacks data-file properties");
    }
    const auto& properties = blob["properties"];
    if (!properties.count("referenced-data-file") || !properties.count("cardinality")) {
      throw std::runtime_error("Deletion vector lacks file identity or cardinality");
    }
    const auto& referenced = properties["referenced-data-file"].asString();
    if (referenced.empty()) {
      throw std::runtime_error("Deletion vector has an empty data-file identity");
    }
    if (!dataFilePath.empty() && referenced != dataFilePath) {
      continue;
    }
    if (selected) {
      throw std::runtime_error("Ambiguous deletion vector for data file: " + dataFilePath);
    }
    if (blob.count("compression-codec") && !blob["compression-codec"].isNull()) {
      throw std::runtime_error("Compressed deletion vector blobs are unsupported");
    }
    const auto offset = blob["offset"].asInt();
    const auto length = blob["length"].asInt();
    if (offset < 4 || length < 20 || static_cast<uint64_t>(offset) > blobEnd ||
        static_cast<uint64_t>(length) > blobEnd - static_cast<uint64_t>(offset)) {
      throw std::runtime_error("Deletion vector blob lies outside the Puffin data region");
    }
    uint64_t cardinality = 0;
    const auto& text = properties["cardinality"].asString();
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), cardinality);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
      throw std::runtime_error("Invalid deletion vector cardinality");
    }
    selected = BlobLocation{offset, length, cardinality};
  }
  if (!selected) {
    throw std::runtime_error("No deletion vector for data file: " + dataFilePath);
  }
  return *selected;
}

roaring::Roaring64Map DeletionVectorReader::readIcebergDV(
    const std::string& filePath, const std::string& dataFilePath) {
  const auto content = readFileBytes(filePath);
  validatePuffinMagic(content, 0, "header");
  const auto blob = parsePuffinFooter(content, dataFilePath);
  const auto* bytes = reinterpret_cast<const unsigned char*>(content.data() + blob.offset);
  const auto readBe32 = [](const unsigned char* p) {
    return (uint32_t{p[0]} << 24) | (uint32_t{p[1]} << 16) |
        (uint32_t{p[2]} << 8) | uint32_t{p[3]};
  };
  const auto readLe64 = [](const unsigned char* p) {
    uint64_t result = 0;
    for (unsigned i = 0; i < 8; ++i) result |= uint64_t{p[i]} << (8 * i);
    return result;
  };
  // Iceberg BitmapPositionDeleteIndex: BE length, LE magic + portable
  // 64-bit Roaring map, BE CRC-32 (IEEE, not CRC32C).
  const auto length = readBe32(bytes);
  if (length != static_cast<uint64_t>(blob.length) - 8 || length < 12) {
    throw std::runtime_error("Invalid deletion vector payload length");
  }
  if (static_cast<uint32_t>(decodeInt32LE(reinterpret_cast<const char*>(bytes + 4))) != 1681511377U) {
    throw std::runtime_error("Invalid deletion vector magic");
  }
  const auto crc = ::crc32(0, bytes + 4, length);
  if (crc != readBe32(bytes + 4 + length)) {
    throw std::runtime_error("Invalid deletion vector CRC-32");
  }
  const auto* bitmap = bytes + 8;
  const size_t bitmapLength = length - 4;
  const auto count = readLe64(bitmap);
  if (count > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      count > (bitmapLength - 8) / 12) {
    throw std::runtime_error("Invalid deletion vector bitmap count");
  }
  size_t cursor = 8;
  int64_t previousKey = -1;
  for (uint64_t i = 0; i < count; ++i) {
    if (bitmapLength - cursor < 4) throw std::runtime_error("Truncated deletion vector key");
    const auto key = static_cast<uint32_t>(decodeInt32LE(
        reinterpret_cast<const char*>(bitmap + cursor)));
    if (key >= static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) || key <= previousKey) {
      throw std::runtime_error("Invalid or unordered deletion vector key");
    }
    previousKey = key;
    cursor += 4;
    const auto consumed = roaring::api::roaring_bitmap_portable_deserialize_size(
        reinterpret_cast<const char*>(bitmap + cursor), bitmapLength - cursor);
    if (consumed == 0) throw std::runtime_error("Invalid deletion vector bitmap");
    cursor += consumed;
  }
  if (cursor != bitmapLength) throw std::runtime_error("Trailing deletion vector bitmap bytes");
  auto result = roaring::Roaring64Map::readSafe(reinterpret_cast<const char*>(bitmap), bitmapLength);
  if (result.cardinality() != blob.cardinality) {
    throw std::runtime_error("Deletion vector cardinality does not match metadata");
  }
  return result;
}

roaring::Roaring DeletionVectorReader::readRawRoaringBitmap(
    const std::string& filePath) {
  auto fileContent = readFileBytes(filePath);

  // A generic portable bitmap payload; not an SDK DeletionFile container.
  return roaring::Roaring::readSafe(
      fileContent.data(),
      fileContent.size());
}

}  // namespace pangrova::delete_filter::io
