// VectorDistance: SIMD-optimized distance functions for vector similarity search.
//
// Provides L2 (Euclidean), cosine, and inner product distance computations
// with compile-time SIMD dispatch: AVX-512 > AVX2 > NEON > scalar fallback.
// All functions are stateless and thread-safe.
//
// Placement: vector/ — Native vector index computation components.
#pragma once

#include <cstddef>

namespace pangrova::vector {

// Euclidean (L2) distance: sqrt(sum((a[i] - b[i])^2)).
// Properties: non-negativity (>= 0), identity (l2Distance(a, a) == 0),
// symmetry (l2Distance(a, b) == l2Distance(b, a)).
// Precondition: dimension > 0, a and b point to at least `dimension` floats.
double l2Distance(const float* a, const float* b, size_t dimension);

// Cosine distance: 1 - dot(a, b) / (||a|| * ||b||).
// Returns value in [0, 2] for non-zero vectors.
// When either vector is zero, returns 1.0 (maximum dissimilarity).
// Precondition: dimension > 0, a and b point to at least `dimension` floats.
double cosineDistance(const float* a, const float* b, size_t dimension);

// Negative inner (dot) product: -dot(a, b).
// Lower values indicate more similar vectors.
// Precondition: dimension > 0, a and b point to at least `dimension` floats.
double innerProduct(const float* a, const float* b, size_t dimension);

}  // namespace pangrova::vector
