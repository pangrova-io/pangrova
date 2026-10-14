// FileIndexFilter: file-level index membership evaluation.
//
// The caller passes a validated, unwrapped index payload. Supported payloads:
//   - Bloom filter: Split Block Bloom Filter (SBBF) — Parquet-compatible
//   - Bitmap: value-to-bit mapping for low-cardinality columns
//   - BSI: bit-sliced representation of numeric column min/max bounds
//
// For Bloom evaluation, index_data is the SBBF byte array written by Java's
// SplitBlockBloomFilter. The caller must bind it to the exact data file.

#include "index/FileIndexFilter.h"

#include <cstring>
#include <functional>

namespace pangrova::index {

namespace {

// =============================================================================
// Split Block Bloom Filter (SBBF) — Algorithm aligned with Java's
// org.pangrova.common.bloom.SplitBlockBloomFilter.
//
// Structure:
//   - Filter divided into 256-bit blocks (8 words × 32 bits each).
//   - numBlocks is always a power of 2.
//   - Each value maps to exactly one block via primary hash (xxHash64).
//   - Within that block, 8 bit positions are probed using salted sub-hashes.
//
// Hash function: xxHash64 (seed=0), matching Parquet's Bloom filter spec.
//
// Serialization format (big-endian):
//   [0..3]   uint32_t magic (0x53424246 = "SBBF")
//   [4..7]   uint32_t version (1)
//   [8..11]  uint32_t numBlocks
//   [12..15] uint32_t insertedCount
//   [16..]   int32_t[] blocks (numBlocks * 8 words, big-endian)
// =============================================================================

/// 8 fixed salts for intra-block bit selection (from Parquet SBBF spec).
static constexpr uint32_t SALTS[8] = {
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

static constexpr int WORDS_PER_BLOCK = 8;
static constexpr uint32_t HEADER_MAGIC = 0x53424246; // "SBBF"
static constexpr int SBBF_HEADER_SIZE = 16;          // magic + version + numBlocks + insertedCount

// xxHash64 constants (seed=0).
static constexpr uint64_t XX_PRIME1 = 0x9E3779B185EBCA87ULL;
static constexpr uint64_t XX_PRIME2 = 0x14DEF9DEA2F79CD6ULL;
static constexpr uint64_t XX_PRIME3 = 0x165667B19E3779F9ULL;
static constexpr uint64_t XX_PRIME4 = 0xC2B2AE3D27D4EB4FULL;
static constexpr uint64_t XX_PRIME5 = 0x27D4EB2F165667C5ULL;

/// Rotate left for 64-bit values.
inline uint64_t rotl64(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

/// Read a little-endian uint64 from a byte pointer.
/// Java's SplitBlockBloomFilter.getLong() reads in little-endian order.
inline uint64_t getLittleEndianUint64(const uint8_t* p) {
  return static_cast<uint64_t>(p[0]) |
      (static_cast<uint64_t>(p[1]) << 8) |
      (static_cast<uint64_t>(p[2]) << 16) |
      (static_cast<uint64_t>(p[3]) << 24) |
      (static_cast<uint64_t>(p[4]) << 32) |
      (static_cast<uint64_t>(p[5]) << 40) |
      (static_cast<uint64_t>(p[6]) << 48) |
      (static_cast<uint64_t>(p[7]) << 56);
}

/// Read a little-endian uint32 from a byte pointer.
inline uint32_t getLittleEndianUint32(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) |
      (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) |
      (static_cast<uint32_t>(p[3]) << 24);
}

/// Read a big-endian uint32 from a byte pointer.
inline uint32_t getBigEndianUint32(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) |
      (static_cast<uint32_t>(p[1]) << 16) |
      (static_cast<uint32_t>(p[2]) << 8) |
      static_cast<uint32_t>(p[3]);
}

inline uint64_t xxRound(uint64_t acc, uint64_t input) {
  acc += input * XX_PRIME2;
  return rotl64(acc, 31) * XX_PRIME1;
}

inline uint64_t mergeRound(uint64_t acc, uint64_t val) {
  acc ^= xxRound(0, val);
  return acc * XX_PRIME1 + XX_PRIME4;
}

inline uint64_t avalanche(uint64_t h64) {
  h64 ^= h64 >> 33;
  h64 *= XX_PRIME2;
  h64 ^= h64 >> 29;
  h64 *= XX_PRIME3;
  h64 ^= h64 >> 32;
  return h64;
}

/// xxHash64 (seed=0) — Parquet-compatible 64-bit hash function.
/// Matches Java's SplitBlockBloomFilter.hash64() exactly.
uint64_t xxHash64(const uint8_t* data, size_t len) {
  uint64_t h64;
  size_t offset = 0;

  if (len >= 32) {
    uint64_t v1 = XX_PRIME1 + XX_PRIME2;
    uint64_t v2 = XX_PRIME2;
    uint64_t v3 = 0;
    uint64_t v4 = static_cast<uint64_t>(0) - XX_PRIME1; // -XX_PRIME1 as unsigned

    do {
      v1 = xxRound(v1, getLittleEndianUint64(data + offset));
      v2 = xxRound(v2, getLittleEndianUint64(data + offset + 8));
      v3 = xxRound(v3, getLittleEndianUint64(data + offset + 16));
      v4 = xxRound(v4, getLittleEndianUint64(data + offset + 24));
      offset += 32;
    } while (offset <= len - 32);

    h64 = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
    h64 = mergeRound(h64, v1);
    h64 = mergeRound(h64, v2);
    h64 = mergeRound(h64, v3);
    h64 = mergeRound(h64, v4);
  } else {
    h64 = XX_PRIME5;
  }

  h64 += static_cast<uint64_t>(len);

  // Process remaining 8-byte chunks.
  while (offset + 8 <= len) {
    uint64_t k1 = getLittleEndianUint64(data + offset) * XX_PRIME2;
    k1 = rotl64(k1, 31) * XX_PRIME1;
    h64 ^= k1;
    h64 = rotl64(h64, 27) * XX_PRIME1 + XX_PRIME4;
    offset += 8;
  }

  // Process remaining 4-byte chunks.
  while (offset + 4 <= len) {
    h64 ^= static_cast<uint64_t>(getLittleEndianUint32(data + offset)) * XX_PRIME1;
    h64 = rotl64(h64, 23) * XX_PRIME2 + XX_PRIME3;
    offset += 4;
  }

  // Process remaining bytes.
  while (offset < len) {
    h64 ^= static_cast<uint64_t>(data[offset]) * XX_PRIME5;
    h64 = rotl64(h64, 11) * XX_PRIME1;
    offset++;
  }

  return avalanche(h64);
}

/// Compute the salted bit index within a 32-bit word.
/// Matches Java: ((key & 0xFFFFFFFFL) * (SALTS[i] & 0xFFFFFFFFL) >>> 27) & 0x1F
inline int saltedBitIndex(uint32_t key, int saltIndex) {
  uint64_t product =
      static_cast<uint64_t>(key) * static_cast<uint64_t>(SALTS[saltIndex]);
  return static_cast<int>((product >> 27) & 0x1F);
}

/// Evaluate a Split Block Bloom Filter (SBBF).
///
/// Serialization format (big-endian):
///   [0..3]   magic 0x53424246 ("SBBF")
///   [4..7]   version (1)
///   [8..11]  numBlocks (power of 2)
///   [12..15] insertedCount
///   [16..]   blocks: numBlocks * 8 int32 words (big-endian)
///
/// Returns false (skip file) only when the filter definitively
/// indicates the value is NOT present.
bool evaluateBloomFilter(
    const std::string& index_data,
    const std::string& value) {
  if (index_data.size() < static_cast<size_t>(SBBF_HEADER_SIZE)) {
    // Malformed index — conservatively return true (do not skip).
    return true;
  }

  const auto* raw = reinterpret_cast<const uint8_t*>(index_data.data());

  // Validate magic.
  uint32_t magic = getBigEndianUint32(raw);
  if (magic != HEADER_MAGIC) {
    return true; // Unknown format — do not skip.
  }

  // Read header fields (big-endian).
  uint32_t version = getBigEndianUint32(raw + 4);
  uint32_t numBlocks = getBigEndianUint32(raw + 8);
  // uint32_t insertedCount = getBigEndianUint32(raw + 12); // unused for probing

  if (version != 1 || numBlocks == 0 || (numBlocks & (numBlocks - 1)) != 0) {
    return true; // numBlocks must be a positive power of 2.
  }

  size_t totalWords = static_cast<size_t>(numBlocks) * WORDS_PER_BLOCK;
  size_t expectedSize = SBBF_HEADER_SIZE + totalWords * 4;
  if (index_data.size() != expectedSize) {
    return true; // Malformed data — do not skip.
  }

  // Hash the value using xxHash64 (seed=0).
  uint64_t hash = xxHash64(
      reinterpret_cast<const uint8_t*>(value.data()), value.size());

  // Block selection: upper 32 bits masked by (numBlocks - 1).
  uint32_t blockIndex =
      static_cast<uint32_t>(hash >> 32) & (numBlocks - 1);

  // Key for intra-block probing: lower 32 bits.
  uint32_t key = static_cast<uint32_t>(hash & 0xFFFFFFFFULL);

  // Read the 8 words of the target block (big-endian int32).
  const uint8_t* blockStart =
      raw + SBBF_HEADER_SIZE + static_cast<size_t>(blockIndex) * WORDS_PER_BLOCK * 4;

  for (int i = 0; i < WORDS_PER_BLOCK; ++i) {
    uint32_t word = getBigEndianUint32(blockStart + i * 4);
    int bitIndex = saltedBitIndex(key, i);
    if ((word & (1U << bitIndex)) == 0) {
      return false; // Definitely not present — skip file.
    }
  }
  return true; // May be present — must read file.
}

/// Paimon bitmap index binary layout:
///   [0..3]   uint32_t num_values (big-endian)
///   [4..]    For each value: int32_t value (big-endian) + 1-byte presence flag
///
/// The bitmap tracks which distinct values exist in the file.
/// Returns false (skip file) when the queried value is not in the bitmap.
bool evaluateBitmap(const std::string& index_data, int32_t value) {
  if (index_data.size() < 4) {
    return true;
  }

  const auto* raw = reinterpret_cast<const uint8_t*>(index_data.data());

  uint32_t num_values =
      (static_cast<uint32_t>(raw[0]) << 24) |
      (static_cast<uint32_t>(raw[1]) << 16) |
      (static_cast<uint32_t>(raw[2]) << 8) |
      static_cast<uint32_t>(raw[3]);

  // Each entry: 4 bytes (int32 value) + 1 byte (presence flag).
  size_t entry_size = 5;
  size_t expected_size = 4 + static_cast<size_t>(num_values) * entry_size;
  if (index_data.size() < expected_size) {
    return true;
  }

  const uint8_t* entries = raw + 4;
  for (uint32_t i = 0; i < num_values; ++i) {
    const uint8_t* entry = entries + i * entry_size;
    int32_t entry_value =
        (static_cast<int32_t>(entry[0]) << 24) |
        (static_cast<int32_t>(entry[1]) << 16) |
        (static_cast<int32_t>(entry[2]) << 8) |
        static_cast<int32_t>(entry[3]);
    if (entry_value == value) {
      return entry[4] != 0;  // Presence flag: 1 = present, 0 = absent.
    }
  }
  // Value not found in bitmap — file does not contain this value.
  return false;
}

/// Paimon BSI (Bit-Sliced Index) binary layout:
///   [0..7]   int64_t file_min_value (big-endian)
///   [8..15]  int64_t file_max_value (big-endian)
///
/// BSI stores the min/max bounds of the numeric column in the file.
/// Range query [min_value, max_value] can skip the file if the file's
/// value range does not overlap with the query range.
bool evaluateBsi(const std::string& index_data, int64_t min_value, int64_t max_value) {
  if (index_data.size() < 16) {
    return true;
  }

  const auto* raw = reinterpret_cast<const uint8_t*>(index_data.data());

  auto readInt64BE = [](const uint8_t* p) -> int64_t {
    uint64_t v =
        (static_cast<uint64_t>(p[0]) << 56) |
        (static_cast<uint64_t>(p[1]) << 48) |
        (static_cast<uint64_t>(p[2]) << 40) |
        (static_cast<uint64_t>(p[3]) << 32) |
        (static_cast<uint64_t>(p[4]) << 24) |
        (static_cast<uint64_t>(p[5]) << 16) |
        (static_cast<uint64_t>(p[6]) << 8) |
        static_cast<uint64_t>(p[7]);
    return static_cast<int64_t>(v);
  };

  int64_t file_min = readInt64BE(raw);
  int64_t file_max = readInt64BE(raw + 8);

  // No overlap: file range is entirely below or above query range.
  if (file_max < min_value || file_min > max_value) {
    return false;  // No overlap — skip file.
  }
  return true;  // Ranges overlap — must read file.
}

}  // namespace

bool FileIndexFilter::mayContainValue(
    const std::string& index_data,
    const std::string& /*column_name*/,
    const std::string& value) {
  return evaluateBloomFilter(index_data, value);
}

bool FileIndexFilter::bitmapContains(
    const std::string& index_data,
    const std::string& /*column_name*/,
    int32_t value) {
  return evaluateBitmap(index_data, value);
}

bool FileIndexFilter::bsiInRange(
    const std::string& index_data,
    const std::string& /*column_name*/,
    int64_t min_value,
    int64_t max_value) {
  return evaluateBsi(index_data, min_value, max_value);
}

}  // namespace pangrova::index
