// DeletionVectorReader: Reads deletion vector files for both Iceberg v3
// (Puffin format with Roaring Bitmap payload) and Paimon (raw Roaring
// Bitmap bytes). Shared utility reusable across both connectors.
//
// All I/O uses Velox FileSystem API (supports S3/HDFS/GCS/Local).
//
// Placement: delete/io/ — file reading and caching utilities.
#pragma once

#include <roaring.hh>
#include <roaring64map.hh>
#include <string>

namespace pangrova::delete_filter::io {

// Puffin file constants (Iceberg v3 deletion vector container format).
// Puffin spec: https://iceberg.apache.org/puffin-spec/
struct PuffinConstants {
  // Magic bytes at file start and end: "PFA1" (4 bytes).
  static constexpr char kMagic[] = "PFA1";
  static constexpr size_t kMagicSize = 4;
  // Footer fixed fields: footer_payload_size (4) + flags (4) + magic (4).
  static constexpr size_t kFooterFixedSize = 12;
};

// Reads deletion vector files and returns Roaring Bitmaps.
//
// Two formats are supported:
//   1. Iceberg v3 Puffin: magic("PFA1") → blobs → footer → magic("PFA1")
//      The matching blob payload contains the serialized Roaring Bitmap.
//   2. A raw portable bitmap payload (not a Paimon DeletionFile container).
//
// Thread-safety: all methods are stateless and thread-safe.
class DeletionVectorReader {
 public:
  DeletionVectorReader() = delete;

  // Read an Iceberg v3 deletion vector file in Puffin format.
  // Parses the Puffin container, extracts the matching blob payload,
  // and deserializes it as a Roaring Bitmap.
  //
  // Throws std::runtime_error on I/O failure or invalid Puffin format.
  static roaring::Roaring64Map readIcebergDV(
      const std::string& filePath, const std::string& dataFilePath = "");

  // Read a generic raw portable Roaring Bitmap payload.
  // Paimon snapshot deletion containers use PaimonSnapshotDeletionFilter.
  // The entire file content is deserialized as a Roaring Bitmap.
  //
  // Throws std::runtime_error on I/O failure or invalid bitmap data.
  static roaring::Roaring readRawRoaringBitmap(const std::string& filePath);

 private:
  // Read entire file content via Velox FileSystem API.
  // Supports local, S3, HDFS, GCS paths based on registered filesystems.
  static std::string readFileBytes(const std::string& filePath);

  // Parse Puffin footer to extract blob metadata.
  // Returns the offset and length of the matching blob payload.
  struct BlobLocation {
    int64_t offset;
    int64_t length;
    uint64_t cardinality;
  };
  static BlobLocation parsePuffinFooter(
      const std::string& fileContent, const std::string& dataFilePath);

  // Validate Puffin magic bytes at the given offset.
  static void validatePuffinMagic(
      const std::string& data,
      size_t offset,
      const std::string& context);
};

}  // namespace pangrova::delete_filter::io
