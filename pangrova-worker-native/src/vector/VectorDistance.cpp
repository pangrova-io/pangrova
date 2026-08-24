// VectorDistance: SIMD-optimized distance functions for vector similarity search.
//
// Compile-time SIMD dispatch hierarchy:
//   AVX-512F (#ifdef __AVX512F__)  — 16 floats per iteration
//   AVX2     (#ifdef __AVX2__)     — 8 floats per iteration
//   NEON     (#ifdef __ARM_NEON)   — 4 floats per iteration
//   Scalar   (fallback)            — 1 float per iteration
//
// All functions accumulate in double precision to match the Java
// VectorDistanceCalculator semantics exactly.

#include "vector/VectorDistance.h"

#include <algorithm>
#include <cmath>

#ifdef __AVX512F__
#include <immintrin.h>
#elif defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace pangrova::vector {
namespace {

// =============================================================================
// Scalar fallback implementations
// =============================================================================
#if !defined(__AVX512F__) && !defined(__AVX2__) && !defined(__ARM_NEON)

double l2DistanceScalar(const float* a, const float* b, size_t dimension) {
  double sum = 0.0;
  for (size_t i = 0; i < dimension; ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

double cosineDistanceScalar(const float* a, const float* b, size_t dimension) {
  double dot = 0.0;
  double normA = 0.0;
  double normB = 0.0;
  for (size_t i = 0; i < dimension; ++i) {
    double ai = static_cast<double>(a[i]);
    double bi = static_cast<double>(b[i]);
    dot += ai * bi;
    normA += ai * ai;
    normB += bi * bi;
  }
  if (normA == 0.0 || normB == 0.0) {
    return 1.0;
  }
  double similarity = dot / (std::sqrt(normA) * std::sqrt(normB));
  // Clamp to [-1, 1] to guard against floating-point drift.
  similarity = std::max(-1.0, std::min(1.0, similarity));
  return 1.0 - similarity;
}

double innerProductScalar(const float* a, const float* b, size_t dimension) {
  double dot = 0.0;
  for (size_t i = 0; i < dimension; ++i) {
    dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return -dot;
}

#endif

// =============================================================================
// AVX-512 implementations (16 floats per iteration)
// =============================================================================
#ifdef __AVX512F__

double l2DistanceAvx512(const float* a, const float* b, size_t dimension) {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;
  for (; i + 16 <= dimension; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    __m512 diff = _mm512_sub_ps(va, vb);
    sum = _mm512_fmadd_ps(diff, diff, sum);
  }
  double result = static_cast<double>(_mm512_reduce_add_ps(sum));
  // Handle remaining elements with scalar.
  for (; i < dimension; ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    result += diff * diff;
  }
  return std::sqrt(result);
}

double cosineDistanceAvx512(
    const float* a,
    const float* b,
    size_t dimension) {
  __m512 vDot = _mm512_setzero_ps();
  __m512 vNormA = _mm512_setzero_ps();
  __m512 vNormB = _mm512_setzero_ps();
  size_t i = 0;
  for (; i + 16 <= dimension; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    vDot = _mm512_fmadd_ps(va, vb, vDot);
    vNormA = _mm512_fmadd_ps(va, va, vNormA);
    vNormB = _mm512_fmadd_ps(vb, vb, vNormB);
  }
  double dot = static_cast<double>(_mm512_reduce_add_ps(vDot));
  double normA = static_cast<double>(_mm512_reduce_add_ps(vNormA));
  double normB = static_cast<double>(_mm512_reduce_add_ps(vNormB));
  for (; i < dimension; ++i) {
    double ai = static_cast<double>(a[i]);
    double bi = static_cast<double>(b[i]);
    dot += ai * bi;
    normA += ai * ai;
    normB += bi * bi;
  }
  if (normA == 0.0 || normB == 0.0) {
    return 1.0;
  }
  double similarity = dot / (std::sqrt(normA) * std::sqrt(normB));
  similarity = std::max(-1.0, std::min(1.0, similarity));
  return 1.0 - similarity;
}

double innerProductAvx512(const float* a, const float* b, size_t dimension) {
  __m512 vDot = _mm512_setzero_ps();
  size_t i = 0;
  for (; i + 16 <= dimension; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    vDot = _mm512_fmadd_ps(va, vb, vDot);
  }
  double dot = static_cast<double>(_mm512_reduce_add_ps(vDot));
  for (; i < dimension; ++i) {
    dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return -dot;
}

#endif  // __AVX512F__

// =============================================================================
// AVX2 implementations (8 floats per iteration)
// =============================================================================
#ifdef __AVX2__

// Horizontal sum of 8 floats in a __m256 register.
inline float hsum256(__m256 v) {
  __m128 hi = _mm256_extractf128_ps(v, 1);
  __m128 lo = _mm256_castps256_ps128(v);
  __m128 sum128 = _mm_add_ps(lo, hi);
  __m128 shuf = _mm_movehdup_ps(sum128);
  __m128 sums = _mm_add_ps(sum128, shuf);
  shuf = _mm_movehl_ps(shuf, sums);
  sums = _mm_add_ss(sums, shuf);
  return _mm_cvtss_f32(sums);
}

double l2DistanceAvx2(const float* a, const float* b, size_t dimension) {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;
  for (; i + 8 <= dimension; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    __m256 diff = _mm256_sub_ps(va, vb);
    sum = _mm256_fmadd_ps(diff, diff, sum);
  }
  double result = static_cast<double>(hsum256(sum));
  for (; i < dimension; ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    result += diff * diff;
  }
  return std::sqrt(result);
}

double cosineDistanceAvx2(const float* a, const float* b, size_t dimension) {
  __m256 vDot = _mm256_setzero_ps();
  __m256 vNormA = _mm256_setzero_ps();
  __m256 vNormB = _mm256_setzero_ps();
  size_t i = 0;
  for (; i + 8 <= dimension; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    vDot = _mm256_fmadd_ps(va, vb, vDot);
    vNormA = _mm256_fmadd_ps(va, va, vNormA);
    vNormB = _mm256_fmadd_ps(vb, vb, vNormB);
  }
  double dot = static_cast<double>(hsum256(vDot));
  double normA = static_cast<double>(hsum256(vNormA));
  double normB = static_cast<double>(hsum256(vNormB));
  for (; i < dimension; ++i) {
    double ai = static_cast<double>(a[i]);
    double bi = static_cast<double>(b[i]);
    dot += ai * bi;
    normA += ai * ai;
    normB += bi * bi;
  }
  if (normA == 0.0 || normB == 0.0) {
    return 1.0;
  }
  double similarity = dot / (std::sqrt(normA) * std::sqrt(normB));
  similarity = std::max(-1.0, std::min(1.0, similarity));
  return 1.0 - similarity;
}

double innerProductAvx2(const float* a, const float* b, size_t dimension) {
  __m256 vDot = _mm256_setzero_ps();
  size_t i = 0;
  for (; i + 8 <= dimension; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    vDot = _mm256_fmadd_ps(va, vb, vDot);
  }
  double dot = static_cast<double>(hsum256(vDot));
  for (; i < dimension; ++i) {
    dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return -dot;
}

#endif  // __AVX2__

// =============================================================================
// NEON implementations (4 floats per iteration)
// =============================================================================
#ifdef __ARM_NEON

double l2DistanceNeon(const float* a, const float* b, size_t dimension) {
  float32x4_t sum = vdupq_n_f32(0.0f);
  size_t i = 0;
  for (; i + 4 <= dimension; i += 4) {
    float32x4_t va = vld1q_f32(a + i);
    float32x4_t vb = vld1q_f32(b + i);
    float32x4_t diff = vsubq_f32(va, vb);
    sum = vfmaq_f32(sum, diff, diff);
  }
  // Horizontal sum of 4 floats.
  float32x2_t sum2 = vadd_f32(vget_low_f32(sum), vget_high_f32(sum));
  float32x2_t sum1 = vpadd_f32(sum2, sum2);
  double result = static_cast<double>(vget_lane_f32(sum1, 0));
  for (; i < dimension; ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    result += diff * diff;
  }
  return std::sqrt(result);
}

double cosineDistanceNeon(const float* a, const float* b, size_t dimension) {
  float32x4_t vDot = vdupq_n_f32(0.0f);
  float32x4_t vNormA = vdupq_n_f32(0.0f);
  float32x4_t vNormB = vdupq_n_f32(0.0f);
  size_t i = 0;
  for (; i + 4 <= dimension; i += 4) {
    float32x4_t va = vld1q_f32(a + i);
    float32x4_t vb = vld1q_f32(b + i);
    vDot = vfmaq_f32(vDot, va, vb);
    vNormA = vfmaq_f32(vNormA, va, va);
    vNormB = vfmaq_f32(vNormB, vb, vb);
  }
  // Horizontal sums.
  double dot = static_cast<double>(vget_lane_f32(
      vpadd_f32(
          vadd_f32(vget_low_f32(vDot), vget_high_f32(vDot)),
          vadd_f32(vget_low_f32(vDot), vget_high_f32(vDot))),
      0));
  double normA = static_cast<double>(vget_lane_f32(
      vpadd_f32(
          vadd_f32(vget_low_f32(vNormA), vget_high_f32(vNormA)),
          vadd_f32(vget_low_f32(vNormA), vget_high_f32(vNormA))),
      0));
  double normB = static_cast<double>(vget_lane_f32(
      vpadd_f32(
          vadd_f32(vget_low_f32(vNormB), vget_high_f32(vNormB)),
          vadd_f32(vget_low_f32(vNormB), vget_high_f32(vNormB))),
      0));
  for (; i < dimension; ++i) {
    double ai = static_cast<double>(a[i]);
    double bi = static_cast<double>(b[i]);
    dot += ai * bi;
    normA += ai * ai;
    normB += bi * bi;
  }
  if (normA == 0.0 || normB == 0.0) {
    return 1.0;
  }
  double similarity = dot / (std::sqrt(normA) * std::sqrt(normB));
  similarity = std::max(-1.0, std::min(1.0, similarity));
  return 1.0 - similarity;
}

double innerProductNeon(const float* a, const float* b, size_t dimension) {
  float32x4_t vDot = vdupq_n_f32(0.0f);
  size_t i = 0;
  for (; i + 4 <= dimension; i += 4) {
    float32x4_t va = vld1q_f32(a + i);
    float32x4_t vb = vld1q_f32(b + i);
    vDot = vfmaq_f32(vDot, va, vb);
  }
  float32x2_t sum2 = vadd_f32(vget_low_f32(vDot), vget_high_f32(vDot));
  float32x2_t sum1 = vpadd_f32(sum2, sum2);
  double dot = static_cast<double>(vget_lane_f32(sum1, 0));
  for (; i < dimension; ++i) {
    dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return -dot;
}

#endif  // __ARM_NEON

}  // namespace

// =============================================================================
// Public API — dispatch to best available SIMD implementation
// =============================================================================

double l2Distance(const float* a, const float* b, size_t dimension) {
#ifdef __AVX512F__
  return l2DistanceAvx512(a, b, dimension);
#elif defined(__AVX2__)
  return l2DistanceAvx2(a, b, dimension);
#elif defined(__ARM_NEON)
  return l2DistanceNeon(a, b, dimension);
#else
  return l2DistanceScalar(a, b, dimension);
#endif
}

double cosineDistance(const float* a, const float* b, size_t dimension) {
#ifdef __AVX512F__
  return cosineDistanceAvx512(a, b, dimension);
#elif defined(__AVX2__)
  return cosineDistanceAvx2(a, b, dimension);
#elif defined(__ARM_NEON)
  return cosineDistanceNeon(a, b, dimension);
#else
  return cosineDistanceScalar(a, b, dimension);
#endif
}

double innerProduct(const float* a, const float* b, size_t dimension) {
#ifdef __AVX512F__
  return innerProductAvx512(a, b, dimension);
#elif defined(__AVX2__)
  return innerProductAvx2(a, b, dimension);
#elif defined(__ARM_NEON)
  return innerProductNeon(a, b, dimension);
#else
  return innerProductScalar(a, b, dimension);
#endif
}

}  // namespace pangrova::vector
