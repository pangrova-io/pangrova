#include "indexing/index/FileIndexFilter.h"

#include <cstring>
#include <cstdint>

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

}  // namespace

bool FileIndexFilter::mayContainValue(
    const std::string& index_data,
    const std::string& /*column_name*/,
    const std::string& value) {
  return evaluateBloomFilter(index_data, value);
}

}  // namespace pangrova::index
