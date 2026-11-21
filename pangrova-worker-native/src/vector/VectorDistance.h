// VectorDistance: SIMD-optimized distance functions for vector similarity search.
//
// Provides L2 (Euclidean), cosine, and inner product distance computations
// with compile-time SIMD dispatch: AVX-512 > AVX2 > NEON > scalar fallback.
// All functions are stateless and thread-safe.
//
// Placement: vector/ — Native vector index computation components.
#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace pangrova::vector {

enum class DistanceMetric : uint8_t { L2 = 0, COSINE = 1, INNER_PRODUCT = 2 };

inline bool validDistanceMetric(uint64_t metric) {
  return metric <= static_cast<uint64_t>(DistanceMetric::INNER_PRODUCT);
}

inline DistanceMetric distanceMetricFromName(std::string_view name) {
  if (name == "L2") return DistanceMetric::L2;
  if (name == "COSINE") return DistanceMetric::COSINE;
  if (name == "INNER_PRODUCT") return DistanceMetric::INNER_PRODUCT;
  throw std::invalid_argument("Unsupported vector distance metric");
}

// Euclidean (L2) distance: sqrt(sum((a[i] - b[i])^2)).
// Properties: non-negativity (>= 0), identity (l2Distance(a, a) == 0),
// symmetry (l2Distance(a, b) == l2Distance(b, a)).
// Inputs point to at least `dimension` elements of their declared precision.
double l2Distance(const float* a, const float* b, size_t dimension);
double l2Distance(const double* a, const double* b, size_t dimension);

// Cosine distance: 1 - dot(a, b) / (||a|| * ||b||).
// Returns value in [0, 2] for non-zero vectors.
// When either vector is zero, returns 1.0 (maximum dissimilarity).
// Inputs point to at least `dimension` elements of their declared precision.
double cosineDistance(const float* a, const float* b, size_t dimension);
double cosineDistance(const double* a, const double* b, size_t dimension);

// Negative inner (dot) product: -dot(a, b).
// Lower values indicate more similar vectors.
// Inputs point to at least `dimension` elements of their declared precision.
double innerProduct(const float* a, const float* b, size_t dimension);
double innerProduct(const double* a, const double* b, size_t dimension);

inline double metricDistance(
    DistanceMetric metric, const float* a, const float* b, size_t dimension) {
  switch (metric) {
    case DistanceMetric::L2: return l2Distance(a, b, dimension);
    case DistanceMetric::COSINE: return cosineDistance(a, b, dimension);
    case DistanceMetric::INNER_PRODUCT: return innerProduct(a, b, dimension);
  }
  throw std::invalid_argument("Unsupported vector distance metric");
}

}  // namespace pangrova::vector
