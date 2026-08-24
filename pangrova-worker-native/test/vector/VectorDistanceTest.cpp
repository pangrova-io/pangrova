// VectorDistanceTest: Unit tests for SIMD-optimized distance functions.
// Validates: Requirements 2.2 (distance function correctness)
//
// Tests cover all three metrics (L2, cosine, inner product) with:
//   - Mathematical properties (non-negativity, identity, symmetry, range)
//   - Zero vector handling
//   - Various dimensions (1, 3, 4, 7, 8, 15, 16, 128, 768)
//   - Consistency with Java VectorDistanceCalculator semantics

#include <gtest/gtest.h>

#include <cmath>
#include <numeric>
#include <vector>

#include "vector/VectorDistance.h"

namespace pangrova::vector {
namespace {

constexpr double kEpsilon = 1e-5;

// --- L2 Distance Tests ---

TEST(VectorDistanceTest, L2DistanceIdenticalVectors) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f, 4.0f};
  EXPECT_DOUBLE_EQ(l2Distance(a.data(), a.data(), a.size()), 0.0);
}

TEST(VectorDistanceTest, L2DistanceNonNegative) {
  std::vector<float> a = {1.0f, -2.0f, 3.0f};
  std::vector<float> b = {-4.0f, 5.0f, -6.0f};
  EXPECT_GE(l2Distance(a.data(), b.data(), a.size()), 0.0);
}

TEST(VectorDistanceTest, L2DistanceSymmetry) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  std::vector<float> b = {5.0f, 4.0f, 3.0f, 2.0f, 1.0f};
  double ab = l2Distance(a.data(), b.data(), a.size());
  double ba = l2Distance(b.data(), a.data(), a.size());
  EXPECT_DOUBLE_EQ(ab, ba);
}

TEST(VectorDistanceTest, L2DistanceKnownValue) {
  // sqrt((3-1)^2 + (4-2)^2) = sqrt(4+4) = sqrt(8) = 2*sqrt(2)
  std::vector<float> a = {1.0f, 2.0f};
  std::vector<float> b = {3.0f, 4.0f};
  double expected = std::sqrt(8.0);
  EXPECT_NEAR(l2Distance(a.data(), b.data(), a.size()), expected, kEpsilon);
}

TEST(VectorDistanceTest, L2DistanceSingleDimension) {
  std::vector<float> a = {3.0f};
  std::vector<float> b = {7.0f};
  EXPECT_NEAR(l2Distance(a.data(), b.data(), 1), 4.0, kEpsilon);
}

// Test with dimension 8 (AVX2 boundary) and 16 (AVX-512 boundary).
TEST(VectorDistanceTest, L2DistanceDimension8) {
  std::vector<float> a(8, 1.0f);
  std::vector<float> b(8, 2.0f);
  // sqrt(8 * 1.0) = sqrt(8)
  EXPECT_NEAR(l2Distance(a.data(), b.data(), 8), std::sqrt(8.0), kEpsilon);
}

TEST(VectorDistanceTest, L2DistanceDimension16) {
  std::vector<float> a(16, 1.0f);
  std::vector<float> b(16, 2.0f);
  // sqrt(16 * 1.0) = 4.0
  EXPECT_NEAR(l2Distance(a.data(), b.data(), 16), 4.0, kEpsilon);
}

TEST(VectorDistanceTest, L2DistanceNonAlignedDimension) {
  // Dimension 7 — tests scalar tail handling for SIMD paths.
  std::vector<float> a = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f};
  std::vector<float> b = {7.0f, 6.0f, 5.0f, 4.0f, 3.0f, 2.0f, 1.0f};
  // sum of (6^2 + 4^2 + 2^2 + 0 + 2^2 + 4^2 + 6^2) = 36+16+4+0+4+16+36 = 112
  EXPECT_NEAR(
      l2Distance(a.data(), b.data(), a.size()), std::sqrt(112.0), kEpsilon);
}

TEST(VectorDistanceTest, L2DistanceLargeDimension) {
  // 768-dim (typical embedding size) — exercises full SIMD loop.
  constexpr size_t kDim = 768;
  std::vector<float> a(kDim, 0.1f);
  std::vector<float> b(kDim, 0.2f);
  // sqrt(768 * 0.01) = sqrt(7.68)
  EXPECT_NEAR(
      l2Distance(a.data(), b.data(), kDim), std::sqrt(7.68), kEpsilon);
}

// --- Cosine Distance Tests ---

TEST(VectorDistanceTest, CosineDistanceIdenticalVectors) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f};
  EXPECT_NEAR(cosineDistance(a.data(), a.data(), a.size()), 0.0, kEpsilon);
}

TEST(VectorDistanceTest, CosineDistanceOppositeVectors) {
  std::vector<float> a = {1.0f, 0.0f};
  std::vector<float> b = {-1.0f, 0.0f};
  EXPECT_NEAR(cosineDistance(a.data(), b.data(), a.size()), 2.0, kEpsilon);
}

TEST(VectorDistanceTest, CosineDistanceOrthogonalVectors) {
  std::vector<float> a = {1.0f, 0.0f};
  std::vector<float> b = {0.0f, 1.0f};
  EXPECT_NEAR(cosineDistance(a.data(), b.data(), a.size()), 1.0, kEpsilon);
}

TEST(VectorDistanceTest, CosineDistanceZeroVectorReturnsOne) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f};
  std::vector<float> zero = {0.0f, 0.0f, 0.0f};
  EXPECT_DOUBLE_EQ(cosineDistance(a.data(), zero.data(), a.size()), 1.0);
  EXPECT_DOUBLE_EQ(cosineDistance(zero.data(), a.data(), a.size()), 1.0);
  EXPECT_DOUBLE_EQ(cosineDistance(zero.data(), zero.data(), a.size()), 1.0);
}

TEST(VectorDistanceTest, CosineDistanceRangeZeroToTwo) {
  // Parallel vectors → 0, orthogonal → 1, anti-parallel → 2.
  std::vector<float> a = {3.0f, 4.0f};
  std::vector<float> b = {6.0f, 8.0f};  // same direction, different magnitude
  EXPECT_NEAR(cosineDistance(a.data(), b.data(), a.size()), 0.0, kEpsilon);
}

TEST(VectorDistanceTest, CosineDistanceSymmetry) {
  std::vector<float> a = {1.0f, 2.0f, 3.0f, 4.0f};
  std::vector<float> b = {4.0f, 3.0f, 2.0f, 1.0f};
  double ab = cosineDistance(a.data(), b.data(), a.size());
  double ba = cosineDistance(b.data(), a.data(), a.size());
  EXPECT_DOUBLE_EQ(ab, ba);
}

TEST(VectorDistanceTest, CosineDistanceDimension8) {
  std::vector<float> a(8, 1.0f);
  std::vector<float> b(8, 1.0f);
  EXPECT_NEAR(cosineDistance(a.data(), b.data(), 8), 0.0, kEpsilon);
}

TEST(VectorDistanceTest, CosineDistanceLargeDimension) {
  constexpr size_t kDim = 128;
  std::vector<float> a(kDim);
  std::vector<float> b(kDim);
  for (size_t i = 0; i < kDim; ++i) {
    a[i] = static_cast<float>(i) * 0.01f;
    b[i] = static_cast<float>(kDim - i) * 0.01f;
  }
  double d = cosineDistance(a.data(), b.data(), kDim);
  EXPECT_GE(d, 0.0);
  EXPECT_LE(d, 2.0);
}

// --- Inner Product Tests ---

TEST(VectorDistanceTest, InnerProductKnownValue) {
  // dot(a, b) = 1*4 + 2*5 + 3*6 = 32, innerProduct = -32
  std::vector<float> a = {1.0f, 2.0f, 3.0f};
  std::vector<float> b = {4.0f, 5.0f, 6.0f};
  EXPECT_NEAR(innerProduct(a.data(), b.data(), a.size()), -32.0, kEpsilon);
}

TEST(VectorDistanceTest, InnerProductZeroVectors) {
  std::vector<float> a = {1.0f, 2.0f};
  std::vector<float> zero = {0.0f, 0.0f};
  EXPECT_DOUBLE_EQ(innerProduct(a.data(), zero.data(), a.size()), 0.0);
}

TEST(VectorDistanceTest, InnerProductSymmetry) {
  std::vector<float> a = {1.0f, -2.0f, 3.0f, -4.0f};
  std::vector<float> b = {5.0f, 6.0f, -7.0f, 8.0f};
  double ab = innerProduct(a.data(), b.data(), a.size());
  double ba = innerProduct(b.data(), a.data(), a.size());
  EXPECT_DOUBLE_EQ(ab, ba);
}

TEST(VectorDistanceTest, InnerProductDimension8) {
  std::vector<float> a(8, 2.0f);
  std::vector<float> b(8, 3.0f);
  // dot = 8 * 6 = 48, innerProduct = -48
  EXPECT_NEAR(innerProduct(a.data(), b.data(), 8), -48.0, kEpsilon);
}

TEST(VectorDistanceTest, InnerProductDimension15) {
  // Non-aligned dimension to test scalar tail.
  std::vector<float> a(15, 1.0f);
  std::vector<float> b(15, 1.0f);
  // dot = 15, innerProduct = -15
  EXPECT_NEAR(innerProduct(a.data(), b.data(), 15), -15.0, kEpsilon);
}

TEST(VectorDistanceTest, InnerProductLargeDimension) {
  constexpr size_t kDim = 768;
  std::vector<float> a(kDim, 0.5f);
  std::vector<float> b(kDim, 0.5f);
  // dot = 768 * 0.25 = 192, innerProduct = -192
  EXPECT_NEAR(innerProduct(a.data(), b.data(), kDim), -192.0, kEpsilon);
}

}  // namespace
}  // namespace pangrova::vector
