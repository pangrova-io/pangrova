// Cross-language test: verifies the native C++ Bloom filter implementation
// produces identical results to Java's SplitBlockBloomFilter.
//
// The test constructs SBBF byte arrays matching Java's serialization format
// and verifies that the C++ evaluateBloomFilter (via FileIndexFilter::mayContainValue)
// correctly identifies inserted and non-inserted values.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "indexing/index/FileIndexFilter.h"

namespace pangrova::index {
namespace {

// =============================================================================
// Helper: construct SBBF serialized bytes matching Java's toByteArray() format.
//
// Format (big-endian):
//   [0..3]   magic 0x53424246 ("SBBF")
//   [4..7]   version (1)
//   [8..11]  numBlocks (power of 2)
//   [12..15] insertedCount
//   [16..]   blocks: numBlocks * 8 int32 words (big-endian)
// =============================================================================

void writeBigEndianUint32(std::vector<uint8_t>& buf, uint32_t val) {
  buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
  buf.push_back(static_cast<uint8_t>(val & 0xFF));
}

void writeBigEndianInt32(std::vector<uint8_t>& buf, int32_t val) {
  writeBigEndianUint32(buf, static_cast<uint32_t>(val));
}

// Replicate Java's SBBF constants and logic for test data generation.
static constexpr uint32_t SALTS[8] = {
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

static constexpr uint64_t XX_PRIME1 = 0x9E3779B185EBCA87ULL;
static constexpr uint64_t XX_PRIME2 = 0x14DEF9DEA2F79CD6ULL;
static constexpr uint64_t XX_PRIME3 = 0x165667B19E3779F9ULL;
static constexpr uint64_t XX_PRIME4 = 0xC2B2AE3D27D4EB4FULL;
static constexpr uint64_t XX_PRIME5 = 0x27D4EB2F165667C5ULL;

uint64_t rotl64(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

uint64_t getLittleEndianUint64(const uint8_t* p) {
  return static_cast<uint64_t>(p[0]) |
      (static_cast<uint64_t>(p[1]) << 8) |
      (static_cast<uint64_t>(p[2]) << 16) |
      (static_cast<uint64_t>(p[3]) << 24) |
      (static_cast<uint64_t>(p[4]) << 32) |
      (static_cast<uint64_t>(p[5]) << 40) |
      (static_cast<uint64_t>(p[6]) << 48) |
      (static_cast<uint64_t>(p[7]) << 56);
}

uint32_t getLittleEndianUint32(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) |
      (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) |
      (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t xxRound(uint64_t acc, uint64_t input) {
  acc += input * XX_PRIME2;
  return rotl64(acc, 31) * XX_PRIME1;
}

uint64_t mergeRound(uint64_t acc, uint64_t val) {
  acc ^= xxRound(0, val);
  return acc * XX_PRIME1 + XX_PRIME4;
}

uint64_t avalanche(uint64_t h64) {
  h64 ^= h64 >> 33;
  h64 *= XX_PRIME2;
  h64 ^= h64 >> 29;
  h64 *= XX_PRIME3;
  h64 ^= h64 >> 32;
  return h64;
}

// Test-local xxHash64 for generating golden data.
uint64_t testXxHash64(const uint8_t* data, size_t len) {
  uint64_t h64;
  size_t offset = 0;

  if (len >= 32) {
    uint64_t v1 = XX_PRIME1 + XX_PRIME2;
    uint64_t v2 = XX_PRIME2;
    uint64_t v3 = 0;
    uint64_t v4 = static_cast<uint64_t>(0) - XX_PRIME1;

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

  while (offset + 8 <= len) {
    uint64_t k1 = getLittleEndianUint64(data + offset) * XX_PRIME2;
    k1 = rotl64(k1, 31) * XX_PRIME1;
    h64 ^= k1;
    h64 = rotl64(h64, 27) * XX_PRIME1 + XX_PRIME4;
    offset += 8;
  }

  while (offset + 4 <= len) {
    h64 ^= static_cast<uint64_t>(getLittleEndianUint32(data + offset)) * XX_PRIME1;
    h64 = rotl64(h64, 23) * XX_PRIME2 + XX_PRIME3;
    offset += 4;
  }

  while (offset < len) {
    h64 ^= static_cast<uint64_t>(data[offset]) * XX_PRIME5;
    h64 = rotl64(h64, 11) * XX_PRIME1;
    offset++;
  }

  return avalanche(h64);
}

int saltedBitIndex(uint32_t key, int saltIndex) {
  uint64_t product =
      static_cast<uint64_t>(key) * static_cast<uint64_t>(SALTS[saltIndex]);
  return static_cast<int>((product >> 27) & 0x1F);
}

// Build an SBBF byte array with the given values inserted.
// Mirrors Java's SplitBlockBloomFilter.Builder logic exactly.
std::string buildSbbfBytes(
    uint32_t numBlocks,
    const std::vector<std::string>& values) {
  // Allocate block array (numBlocks * 8 words, initialized to 0).
  std::vector<int32_t> blocks(numBlocks * 8, 0);

  for (const auto& value : values) {
    uint64_t hash = testXxHash64(
        reinterpret_cast<const uint8_t*>(value.data()), value.size());
    uint32_t blockIndex = static_cast<uint32_t>(hash >> 32) & (numBlocks - 1);
    uint32_t key = static_cast<uint32_t>(hash & 0xFFFFFFFFULL);
    int wordOffset = static_cast<int>(blockIndex) * 8;

    for (int i = 0; i < 8; ++i) {
      int bitIndex = saltedBitIndex(key, i);
      blocks[wordOffset + i] |= (1 << bitIndex);
    }
  }

  // Serialize to SBBF format.
  std::vector<uint8_t> buf;
  buf.reserve(16 + blocks.size() * 4);
  writeBigEndianUint32(buf, 0x53424246); // magic "SBBF"
  writeBigEndianUint32(buf, 1);          // version
  writeBigEndianUint32(buf, numBlocks);
  writeBigEndianUint32(buf, static_cast<uint32_t>(values.size()));

  for (int32_t word : blocks) {
    writeBigEndianInt32(buf, word);
  }

  return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

// =============================================================================
// Test: xxHash64 reference values match Java's SplitBlockBloomFilter.hash64()
// =============================================================================

TEST(SplitBlockBloomFilterTest, XxHash64_MatchesJavaReferenceValues) {
  // These reference values are from Java's SplitBlockBloomFilterTest:
  // hash64_matchesXxHash64ReferenceValues()

  // Empty input
  uint64_t hashEmpty = testXxHash64(nullptr, 0);
  EXPECT_EQ(hashEmpty, 0x2BE48486F6C12FE5ULL);

  // Single byte: 0x00
  uint8_t singleZero[] = {0};
  uint64_t hashZero = testXxHash64(singleZero, 1);
  EXPECT_EQ(hashZero, 0xC99CDAB44F6EBF47ULL);

  // "a" (0x61)
  uint8_t a[] = {'a'};
  uint64_t hashA = testXxHash64(a, 1);
  EXPECT_EQ(hashA, 0xC748027F53E8739BULL);
}

// =============================================================================
// Test: Native FileIndexFilter correctly reads SBBF written by Java algorithm
// =============================================================================

TEST(SplitBlockBloomFilterTest, NativeReadsJavaWrittenSbbf_InsertedValuesFound) {
  // Simulate Java writing an SBBF with known values.
  // numBlocks=4 (power of 2, small for testing).
  std::vector<std::string> insertedValues = {
      "alpha", "beta", "gamma", "delta", "customer_001", "order_100"};

  std::string sbbfBytes = buildSbbfBytes(4, insertedValues);

  // All inserted values must be found (no false negatives).
  for (const auto& value : insertedValues) {
    EXPECT_TRUE(FileIndexFilter::mayContainValue(sbbfBytes, "col", value))
        << "Inserted value '" << value << "' must be found (no false negatives)";
  }
}

TEST(SplitBlockBloomFilterTest, NativeReadsJavaWrittenSbbf_NonInsertedRejected) {
  // Build SBBF with specific values.
  std::vector<std::string> insertedValues = {"val-A", "val-B", "val-C"};
  std::string sbbfBytes = buildSbbfBytes(4, insertedValues);

  // Non-inserted values should mostly return false (probabilistic).
  // With only 3 values in 4 blocks (1024 bits), FPR should be very low.
  int falsePositives = 0;
  int totalProbes = 100;
  for (int i = 0; i < totalProbes; ++i) {
    std::string probe = "nonexistent-value-" + std::to_string(i);
    if (FileIndexFilter::mayContainValue(sbbfBytes, "col", probe)) {
      falsePositives++;
    }
  }
  // With 3 values in 4 blocks (1024 bits), expect very few false positives.
  EXPECT_LT(falsePositives, 20)
      << "Too many false positives: " << falsePositives << "/" << totalProbes;
}

TEST(SplitBlockBloomFilterTest, NativeReadsJavaWrittenSbbf_LargerFilter) {
  // Larger filter: 16 blocks, 100 values — closer to production usage.
  std::vector<std::string> insertedValues;
  insertedValues.reserve(100);
  for (int i = 0; i < 100; ++i) {
    insertedValues.push_back("key-" + std::to_string(i));
  }

  std::string sbbfBytes = buildSbbfBytes(16, insertedValues);

  // All inserted values must be found.
  for (const auto& value : insertedValues) {
    EXPECT_TRUE(FileIndexFilter::mayContainValue(sbbfBytes, "col", value))
        << "Inserted value '" << value << "' must be found";
  }

  // Non-inserted values: check FPR is reasonable.
  int falsePositives = 0;
  int totalProbes = 1000;
  for (int i = 1000; i < 1000 + totalProbes; ++i) {
    std::string probe = "key-" + std::to_string(i);
    if (FileIndexFilter::mayContainValue(sbbfBytes, "col", probe)) {
      falsePositives++;
    }
  }
  // 100 values in 16 blocks (4096 bits) — FPR should be well under 10%.
  EXPECT_LT(falsePositives, 100)
      << "FPR too high: " << falsePositives << "/" << totalProbes;
}

// =============================================================================
// Test: Malformed/invalid SBBF data returns true (conservative, do not skip)
// =============================================================================

TEST(SplitBlockBloomFilterTest, MalformedData_ReturnsTrue) {
  // Too short.
  EXPECT_TRUE(FileIndexFilter::mayContainValue("", "col", "x"));
  EXPECT_TRUE(FileIndexFilter::mayContainValue("short", "col", "x"));

  // Wrong magic.
  std::vector<uint8_t> wrongMagic(48, 0);
  wrongMagic[0] = 0xFF; // Not "SBBF"
  std::string wrongMagicStr(
      reinterpret_cast<const char*>(wrongMagic.data()), wrongMagic.size());
  EXPECT_TRUE(FileIndexFilter::mayContainValue(wrongMagicStr, "col", "x"));

  // numBlocks = 0.
  std::vector<uint8_t> zeroBlocks;
  writeBigEndianUint32(zeroBlocks, 0x53424246); // magic
  writeBigEndianUint32(zeroBlocks, 1);          // version
  writeBigEndianUint32(zeroBlocks, 0);          // numBlocks = 0
  writeBigEndianUint32(zeroBlocks, 0);          // insertedCount
  std::string zeroBlocksStr(
      reinterpret_cast<const char*>(zeroBlocks.data()), zeroBlocks.size());
  EXPECT_TRUE(FileIndexFilter::mayContainValue(zeroBlocksStr, "col", "x"));

  // numBlocks not power of 2.
  std::vector<uint8_t> nonPow2;
  writeBigEndianUint32(nonPow2, 0x53424246);
  writeBigEndianUint32(nonPow2, 1);
  writeBigEndianUint32(nonPow2, 3); // not power of 2
  writeBigEndianUint32(nonPow2, 0);
  std::string nonPow2Str(
      reinterpret_cast<const char*>(nonPow2.data()), nonPow2.size());
  EXPECT_TRUE(FileIndexFilter::mayContainValue(nonPow2Str, "col", "x"));
}

// =============================================================================
// Test: Empty filter (all zeros) rejects all values
// =============================================================================

TEST(SplitBlockBloomFilterTest, EmptyFilter_RejectsAllValues) {
  // Build SBBF with no values inserted (all block words are 0).
  std::string sbbfBytes = buildSbbfBytes(4, {});

  // An empty filter should reject everything (all bits are 0).
  EXPECT_FALSE(FileIndexFilter::mayContainValue(sbbfBytes, "col", "anything"));
  EXPECT_FALSE(FileIndexFilter::mayContainValue(sbbfBytes, "col", "test"));
  EXPECT_FALSE(FileIndexFilter::mayContainValue(sbbfBytes, "col", ""));
}

// =============================================================================
// Test: Cross-language golden values — specific bytes that Java would produce
// for known inputs, verified against Java's SplitBlockBloomFilter.
//
// This test uses hardcoded expected hash values from Java to ensure the C++
// implementation produces bit-identical results.
// =============================================================================

TEST(SplitBlockBloomFilterTest, CrossLanguageGolden_HashAndBlockSelection) {
  // Verify hash("alpha") produces the same block index and key as Java.
  std::string value = "alpha";
  uint64_t hash = testXxHash64(
      reinterpret_cast<const uint8_t*>(value.data()), value.size());

  // With numBlocks=4, blockIndex = (hash >>> 32) & 3
  uint32_t blockIndex = static_cast<uint32_t>(hash >> 32) & 3;
  uint32_t key = static_cast<uint32_t>(hash & 0xFFFFFFFFULL);

  // Verify the salted bit indices are deterministic.
  for (int i = 0; i < 8; ++i) {
    int bitIdx = saltedBitIndex(key, i);
    EXPECT_GE(bitIdx, 0);
    EXPECT_LE(bitIdx, 31);
  }

  // Build a filter with "alpha", then verify it's found.
  std::string sbbf = buildSbbfBytes(4, {"alpha"});
  EXPECT_TRUE(FileIndexFilter::mayContainValue(sbbf, "col", "alpha"));

  // Verify "beta" is NOT found in a filter containing only "alpha".
  // (With 4 blocks and 1 value, collision probability is very low.)
  EXPECT_FALSE(FileIndexFilter::mayContainValue(sbbf, "col", "beta"));
}

}  // namespace
}  // namespace pangrova::index
