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
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "vector/VectorDistance.h"
#include "vector/VectorDistanceFunctions.h"
#include "TestMemoryHelper.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::vector {
namespace {

constexpr double kEpsilon = 1e-5;

TEST(VectorDistanceTest, DimensionMismatchFailsClosed) {
  EXPECT_NO_THROW(validateVectorDimensions(768, 768));
  try {
    validateVectorDimensions(768, 1024);
    FAIL() << "dimension mismatch must fail the query";
  } catch (const std::exception& error) {
    EXPECT_NE(
        std::string(error.what()).find("Vector dimension mismatch"),
        std::string::npos);
  }
}

TEST(VectorDistanceTest, DoublePrecisionIsNotRoundedToFloat) {
  const std::vector<double> a = {1.0000000001, 1e-50};
  const std::vector<double> b = {1.0, 0.0};
  EXPECT_DOUBLE_EQ(l2Distance(a.data(), b.data(), a.size()), a[0] - b[0]);
  EXPECT_DOUBLE_EQ(innerProduct(a.data(), b.data(), a.size()), -1.0000000001);

  const std::vector<double> tiny = {1e-50, 0.0};
  EXPECT_NEAR(cosineDistance(tiny.data(), tiny.data(), tiny.size()), 0.0,
              1e-15);
}

TEST(VectorDistanceTest, DoubleNormsPreserveExtremeFiniteMagnitudes) {
  const double zero[] = {0.0, 0.0};
  for (double magnitude : {1e-200, 1e200,
                           std::numeric_limits<double>::denorm_min(),
                           std::numeric_limits<double>::max()}) {
    const double value[] = {magnitude, 0.0};
    const double opposite[] = {-magnitude, 0.0};
    EXPECT_DOUBLE_EQ(l2Distance(value, zero, 2), magnitude);
    EXPECT_DOUBLE_EQ(cosineDistance(value, value, 2), 0.0);
    EXPECT_DOUBLE_EQ(cosineDistance(value, opposite, 2), 2.0);
    EXPECT_DOUBLE_EQ(cosineDistance(value, zero, 2), 1.0);
  }
}

TEST(VectorDistanceTest, DoubleNormsHandleMixedScalesAndCommonLargeCoordinates) {
  const double left[] = {1e200, 1e-200};
  const double right[] = {1e200, 0.0};
  const double orthogonal[] = {0.0, -1e-200};
  EXPECT_DOUBLE_EQ(l2Distance(left, right, 2), 1e-200);
  EXPECT_DOUBLE_EQ(cosineDistance(right, orthogonal, 2), 1.0);
  const double a[] = {3e200, -4e200};
  const double zero[] = {0.0, 0.0};
  EXPECT_NEAR(l2Distance(a, zero, 2) / 5e200, 1.0, 1e-15);
  const double b[] = {3e-200, -4e-200};
  EXPECT_NEAR(cosineDistance(a, b, 2), 0.0, 1e-15);
}

TEST(VectorDistanceTest, DoubleMetricsRejectNonFiniteInputsAndRealOverflow) {
  const double zero[] = {0.0};
  for (double invalid : {std::numeric_limits<double>::infinity(),
                         -std::numeric_limits<double>::infinity(),
                         std::numeric_limits<double>::quiet_NaN()}) {
    EXPECT_THROW(l2Distance(&invalid, zero, 1), std::invalid_argument);
    EXPECT_THROW(cosineDistance(&invalid, zero, 1), std::invalid_argument);
    EXPECT_THROW(innerProduct(&invalid, zero, 1), std::invalid_argument);
  }
  const double maximum[] = {std::numeric_limits<double>::max()};
  const double opposite[] = {-std::numeric_limits<double>::max()};
  const double two[] = {2.0};
  EXPECT_THROW(l2Distance(maximum, opposite, 1), std::overflow_error);
  EXPECT_THROW(innerProduct(maximum, two, 1), std::overflow_error);
}

TEST(VectorDistanceTest, DoubleInnerProductCancelsUnrepresentableProducts) {
  const double left[] = {1e200, 1e200, 1.0};
  const double right[] = {1e200, -1e200, 1.0};
  EXPECT_DOUBLE_EQ(innerProduct(left, right, 3), -1.0);
  const double negative[] = {1e200, -1e200, -1.0};
  EXPECT_DOUBLE_EQ(innerProduct(left, negative, 3), 1.0);
  const double productLeft[] = {1e200, 1e-200};
  const double productRight[] = {1e-200, 1e200};
  EXPECT_DOUBLE_EQ(innerProduct(productLeft, productRight, 2), -2.0);
}

TEST(VectorDistanceTest, DoubleInnerProductHandlesAccumulatorCarryAndBorrow) {
  const double large = std::ldexp(1.0, 600);
  const double factor = std::ldexp(1.0, 451);
  const double carryLeft[] = {large, large, large, large, 1.0};
  const double carryRight[] = {factor, factor, -factor, -factor, 1.0};
  EXPECT_DOUBLE_EQ(innerProduct(carryLeft, carryRight, 5), -1.0);
  const double borrowLeft[] = {large, large};
  const double borrowRight[] = {factor, -(factor - std::ldexp(1.0, 399))};
  EXPECT_DOUBLE_EQ(innerProduct(borrowLeft, borrowRight, 2), -std::ldexp(1.0, 999));
}

TEST(VectorDistanceTest, DoubleInnerProductRoundsSubnormalSumsTiesToEven) {
  const double tiny = std::numeric_limits<double>::denorm_min();
  const double half[] = {0.5};
  EXPECT_DOUBLE_EQ(innerProduct(&tiny, half, 1), 0.0);
  const double oneAndHalf[] = {1.5};
  EXPECT_DOUBLE_EQ(innerProduct(&tiny, oneAndHalf, 1), -2.0 * tiny);
  const double left[] = {tiny, tiny, tiny};
  const double right[] = {0.5, 0.5, 0.5};
  EXPECT_DOUBLE_EQ(innerProduct(left, right, 3), -2.0 * tiny);
  const double negative[] = {-tiny, -tiny, -tiny};
  EXPECT_DOUBLE_EQ(innerProduct(negative, right, 3), 2.0 * tiny);
}

TEST(VectorDistanceTest, DoubleInnerProductRoundsNormalSumsTiesToEven) {
  const double right[] = {1e200, -1e200, 1.0, 1.0};
  const double even[] = {1e200, 1e200, 1.0, std::ldexp(1.0, -53)};
  EXPECT_DOUBLE_EQ(innerProduct(even, right, 4), -1.0);
  const double next = std::nextafter(1.0, 2.0);
  const double odd[] = {1e200, 1e200, next, std::ldexp(1.0, -53)};
  EXPECT_DOUBLE_EQ(innerProduct(odd, right, 4), -std::nextafter(next, 2.0));
}

TEST(VectorDistanceTest, DoubleInnerProductPreservesFiniteLimitAndDetectsOverflow) {
  const double maximum = std::numeric_limits<double>::max();
  const double finite[] = {1e200, 1e200, maximum, std::ldexp(1.0, 969)};
  const double right[] = {1e200, -1e200, 1.0, 1.0};
  EXPECT_DOUBLE_EQ(innerProduct(finite, right, 4), -maximum);
  const double overflow[] = {1e200, 1e200, maximum, std::ldexp(1.0, 970)};
  EXPECT_THROW(innerProduct(overflow, right, 4), std::overflow_error);
}

TEST(VectorDistanceTest, DoubleInnerProductCompensatesOrdinaryCancellation) {
  const double left[] = {1e100, 1.0, 1e100};
  const double right[] = {1.0, 1.0, -1.0};
  EXPECT_DOUBLE_EQ(innerProduct(left, right, 3), -1.0);
  const double zero[] = {0.0, -0.0};
  EXPECT_DOUBLE_EQ(innerProduct(zero, zero, 2), 0.0);
  EXPECT_TRUE(std::signbit(innerProduct(
      static_cast<const double*>(nullptr), static_cast<const double*>(nullptr), 0)));
}

class VectorDistanceFunctionBoundaryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("vector-distance-function-boundary");
    maker_ = std::make_unique<facebook::velox::test::VectorMaker>(pool_.get());
    registerVectorDistanceFunctions();
  }

  template <typename T>
  facebook::velox::VectorPtr array(const std::vector<T>& value) {
    return maker_->arrayVector<T>(std::vector<std::vector<T>>{value});
  }

  double evaluate(
      const std::string& name,
      facebook::velox::VectorPtr left,
      facebook::velox::VectorPtr right) {
    facebook::velox::core::QueryConfig config{{}};
    auto function = facebook::velox::exec::getVectorFunction(
        name, {left->type(), right->type()}, {}, config);
    if (!function) {
      throw std::runtime_error("Distance SQL function was not registered");
    }
    facebook::velox::core::ExecCtx execution(pool_.get(), nullptr);
    facebook::velox::exec::EvalCtx context(&execution);
    facebook::velox::SelectivityVector rows(1);
    std::vector<facebook::velox::VectorPtr> args{std::move(left), std::move(right)};
    facebook::velox::VectorPtr result;
    function->apply(rows, args, facebook::velox::DOUBLE(), context, result);
    return result->asFlatVector<double>()->valueAt(0);
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::unique_ptr<facebook::velox::test::VectorMaker> maker_;
};

TEST_F(VectorDistanceFunctionBoundaryTest, DoubleAndMixedArraysRetainExtremeMagnitudes) {
  for (double magnitude : {1e-200, 1e200}) {
    auto value = array<double>({magnitude, 0.0});
    EXPECT_DOUBLE_EQ(evaluate("cosine_distance", value, value), 0.0);
    EXPECT_DOUBLE_EQ(evaluate("pangrova_cosine_similarity", value, value), 1.0);
    auto direction = array<float>({1.0F, 0.0F});
    EXPECT_DOUBLE_EQ(evaluate("cosine_distance", value, direction), 0.0);
    EXPECT_DOUBLE_EQ(evaluate("cosine_distance", direction, value), 0.0);
    EXPECT_DOUBLE_EQ(evaluate("l2_distance", value, array<float>({0.0F, 0.0F})), magnitude);
    EXPECT_DOUBLE_EQ(evaluate("inner_product", value, direction), -magnitude);
  }
}

TEST_F(VectorDistanceFunctionBoundaryTest, DoubleInnerProductRetainsCancellationResidual) {
  EXPECT_DOUBLE_EQ(evaluate("inner_product",
      array<double>({1e200, 1e200, 1.0}),
      array<double>({1e200, -1e200, 1.0})), -1.0);
  EXPECT_DOUBLE_EQ(evaluate("inner_product",
      array<double>({1e100, 1.0, 1e100}),
      array<float>({1.0F, 1.0F, -1.0F})), -1.0);
}

TEST_F(VectorDistanceFunctionBoundaryTest, RejectsNonFiniteInputsAndResults) {
  for (const auto* name : {"l2_distance", "cosine_distance", "inner_product",
                           "pangrova_cosine_similarity"}) {
    for (double invalid : {std::numeric_limits<double>::infinity(),
                           std::numeric_limits<double>::quiet_NaN()}) {
      EXPECT_THROW(evaluate(name, array<double>({invalid}), array<float>({0.0F})),
                   std::exception);
    }
  }
  EXPECT_THROW(evaluate("inner_product",
      array<double>({std::numeric_limits<double>::max()}), array<float>({2.0F})),
      std::exception);
}

TEST(VectorDistanceTest, RegisteredDistanceFunctionsAcceptAllVectorPrecisions) {
  registerVectorDistanceFunctions();
  const auto realArray = facebook::velox::ARRAY(facebook::velox::REAL());
  const auto doubleArray = facebook::velox::ARRAY(facebook::velox::DOUBLE());
  for (const auto* name : {"l2_distance", "cosine_distance", "inner_product",
                           "pangrova_cosine_similarity"}) {
    for (const auto& left : {realArray, doubleArray}) {
      for (const auto& right : {realArray, doubleArray}) {
        const auto result = facebook::velox::exec::resolveVectorFunction(
            name, {left, right});
        ASSERT_NE(result, nullptr) << name << " left=" << left->toString()
                                   << " right=" << right->toString();
        EXPECT_EQ(result->kind(), facebook::velox::TypeKind::DOUBLE);
      }
    }
  }
}

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
