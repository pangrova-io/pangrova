#include "delete/io/DeletionVectorReader.h"

#include <cstring>
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
    auto fs = facebook::velox::filesystems::getFileSystem(filePath, nullptr);
    if (fs) {
      auto readFile = fs->openFileForRead(filePath);
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
        ": expected PUF1");
  }
}

std::pair<int64_t, int64_t> DeletionVectorReader::parsePuffinFooter(
    const std::string& fileContent) {
  // Puffin file layout:
  //   [magic(4)] [blob1_data] [blob2_data] ... [footer_payload] [footer_fixed(12)]
  //
  // Footer fixed (last 12 bytes):
  //   footer_payload_size (4 bytes, little-endian)
  //   flags (4 bytes)
  //   magic (4 bytes, "PUF1")
  //
  // Footer payload is JSON: { "blobs": [{"type":..., "offset":N, "length":N}, ...] }

  auto fileSize = fileContent.size();
  auto minSize = PuffinConstants::kMagicSize + PuffinConstants::kFooterFixedSize;
  if (fileSize < minSize) {
    throw std::runtime_error(
        "Puffin file too small: size=" + std::to_string(fileSize) +
        ", minimum=" + std::to_string(minSize));
  }

  // Validate trailing magic.
  validatePuffinMagic(
      fileContent, fileSize - PuffinConstants::kMagicSize, "footer-end");

  // Read footer_payload_size from footer fixed area.
  auto footerFixedStart = fileSize - PuffinConstants::kFooterFixedSize;
  auto footerPayloadSize = decodeInt32LE(
      fileContent.data() + footerFixedStart);

  if (footerPayloadSize < 0 ||
      static_cast<size_t>(footerPayloadSize) >
          fileSize - PuffinConstants::kMagicSize - PuffinConstants::kFooterFixedSize) {
    throw std::runtime_error(
        "Invalid Puffin footer payload size: " +
        std::to_string(footerPayloadSize));
  }

  // P3-BUG-21: Parse footer payload JSON to extract blob offset/length
  // instead of arithmetic assumption that only works for single-blob files.
  // For multi-blob Puffin files (e.g., Iceberg v3 with column stats + DV),
  // the arithmetic approach computes incorrect blob boundaries.
  if (footerPayloadSize > 0) {
    auto footerPayloadStart = footerFixedStart - footerPayloadSize;
    std::string footerJson = fileContent.substr(
        footerPayloadStart, footerPayloadSize);

    // F-NATIVE-WRITE-10: Use folly::parseJson instead of hand-written
    // JSON extraction. More robust against whitespace/formatting variations.
    try {
      auto parsed = folly::parseJson(footerJson);
      if (parsed.isObject() && parsed.count("blobs")) {
        const auto& blobs = parsed["blobs"];
        if (blobs.isArray() && !blobs.empty()) {
          // F-NATIVE-WRITE-06: Select the DV blob by type "deletion-vector-v1"
          // instead of blindly picking the last blob. For multi-blob Puffin
          // files, the last blob may be column stats, not the DV.
          const folly::dynamic* dvBlob = nullptr;
          for (size_t i = 0; i < blobs.size(); ++i) {
            const auto& b = blobs[i];
            if (b.isObject() && b.count("type")) {
              auto blobType = b["type"].asString();
              if (blobType == "deletion-vector-v1" ||
                  blobType == "apache-datasketches-theta-v1") {
                dvBlob = &b;
                break;
              }
            }
          }
          // Fallback: if no typed DV blob found, use the last blob.
          if (!dvBlob) {
            dvBlob = &blobs[blobs.size() - 1];
          }
          if (dvBlob->isObject() &&
              dvBlob->count("offset") && dvBlob->count("length")) {
            auto blobOffset = (*dvBlob)["offset"].asInt();
            auto blobLength = (*dvBlob)["length"].asInt();
            if (blobOffset >= 0 && blobLength > 0) {
              return {blobOffset, blobLength};
            }
          }
        }
      }
    } catch (const std::exception&) {
      // Fall through to single-blob arithmetic below.
    }
  }

  // Single-blob arithmetic: when footer payload is empty (no JSON metadata),
  // the file contains exactly one blob between header magic and footer.
  auto blobOffset = static_cast<int64_t>(PuffinConstants::kMagicSize);
  auto blobLength = static_cast<int64_t>(footerFixedStart) -
      footerPayloadSize - blobOffset;

  if (blobLength <= 0) {
    throw std::runtime_error(
        "Puffin file contains no blob data: computed blob_length=" +
        std::to_string(blobLength));
  }

  return {blobOffset, blobLength};
}

roaring::Roaring DeletionVectorReader::readIcebergDV(
    const std::string& filePath) {
  auto fileContent = readFileBytes(filePath);

  // Validate header magic.
  validatePuffinMagic(fileContent, 0, "header");

  // Parse footer to locate the first blob.
  auto [blobOffset, blobLength] = parsePuffinFooter(fileContent);

  if (static_cast<size_t>(blobOffset + blobLength) > fileContent.size()) {
    throw std::runtime_error(
        "Puffin blob extends beyond file: offset=" +
        std::to_string(blobOffset) + ", length=" +
        std::to_string(blobLength) + ", file_size=" +
        std::to_string(fileContent.size()));
  }

  const char* blobData = fileContent.data() + blobOffset;
  size_t blobLen = static_cast<size_t>(blobLength);

  // F-NATIVE-WRITE-05: Parse deletion-vector-v1 framing.
  // Format: [4-byte magic 0xD1D33964][serialized Roaring bitmap][4-byte CRC32C]
  // If the blob starts with the DV magic, strip the framing; otherwise
  // fall back to raw Roaring deserialization (for older/non-standard blobs).
  static constexpr uint32_t kDvMagic = 0xD1D33964;
  if (blobLen >= 8) {
    uint32_t magic = 0;
    std::memcpy(&magic, blobData, sizeof(magic));
    // DV magic is stored in little-endian.
    if (magic == kDvMagic) {
      // Strip 4-byte magic prefix and 4-byte CRC32C suffix.
      const char* bitmapStart = blobData + 4;
      size_t bitmapLen = blobLen - 8; // subtract magic(4) + crc(4)
      return roaring::Roaring::readSafe(bitmapStart, bitmapLen);
    }
  }

  // Fallback: raw Roaring bitmap (no framing).
  return roaring::Roaring::readSafe(blobData, blobLen);
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
