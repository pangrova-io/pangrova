// VectorScanOperator: Brute-force vector scan implementation.
//
// Iterates over contiguous row-major float vectors, computes SIMD distances,
// and accumulates Top-K results using a bounded max-heap. Supports optional
// pre-filter bitmap to skip non-qualifying rows before distance computation.

#include "vector/VectorScanOperator.h"

#include "vector/QuantizedVectorCodec.h"
#include "vector/VectorDistance.h"

#include <cmath>

namespace pangrova::vector {

namespace {

using DistanceFn = double (*)(const float*, const float*, size_t);

DistanceFn resolveMetric(DistanceMetric metric) {
  switch (metric) {
    case DistanceMetric::L2:
      return l2Distance;
    case DistanceMetric::COSINE:
      return cosineDistance;
    case DistanceMetric::INNER_PRODUCT:
      return innerProduct;
  }
  return l2Distance;
}

bool isBitSet(const uint8_t* bitmap, int64_t index) {
  return (bitmap[index >> 3] & (1u << (index & 7))) != 0;
}

}  // namespace

VectorScanResult bruteForceScan(
    const float* data,
    int64_t numVectors,
    int64_t dimension,
    const float* query,
    int64_t topK,
    DistanceMetric metric,
    const uint8_t* validBitmap,
    int64_t bitmapSize) {
  VectorScanResult result{};
  result.vectorsScanned = 0;
  result.vectorsSkipped = 0;

  if (data == nullptr || query == nullptr || numVectors <= 0 || dimension <= 0 ||
      topK <= 0) {
    return result;
  }

  DistanceFn distFn = resolveMetric(metric);
  VectorTopKHeap heap(static_cast<size_t>(topK));

  for (int64_t i = 0; i < numVectors; ++i) {
    if (validBitmap != nullptr && !isBitSet(validBitmap, i)) {
      result.vectorsSkipped++;
      continue;
    }

    const float* vec = data + i * dimension;
    double dist = distFn(vec, query, static_cast<size_t>(dimension));
    heap.push(i, dist);
    result.vectorsScanned++;
  }

  result.rows = heap.drainSorted();
  return result;
}

VectorScanResult bruteForceScanFp16(
    const uint16_t* data,
    int64_t numVectors,
    int64_t dimension,
    const float* query,
    int64_t topK,
    DistanceMetric metric,
    const uint8_t* validBitmap,
    int64_t bitmapSize) {
  VectorScanResult result{};
  result.vectorsScanned = 0;
  result.vectorsSkipped = 0;

  if (data == nullptr || query == nullptr || numVectors <= 0 || dimension <= 0 ||
      topK <= 0) {
    return result;
  }

  auto dim = static_cast<size_t>(dimension);
  auto queryFp16 = quantizeFp16(query, dim);
  VectorTopKHeap heap(static_cast<size_t>(topK));

  // Pre-allocate decode buffer once — reused across all vectors to avoid
  // per-row heap allocation (critical at TB scale with billions of vectors).
  std::vector<float> decodeBuf(dim);

  for (int64_t i = 0; i < numVectors; ++i) {
    if (validBitmap != nullptr && !isBitSet(validBitmap, i)) {
      result.vectorsSkipped++;
      continue;
    }

    const uint16_t* vec = data + i * dimension;
    double dist = 0.0;
    switch (metric) {
      case DistanceMetric::L2:
        dist = l2DistanceFp16(vec, queryFp16.data(), dim);
        break;
      case DistanceMetric::COSINE: {
        for (size_t d = 0; d < dim; ++d) {
          decodeBuf[d] = halfToFloat(vec[d]);
        }
        dist = cosineDistance(decodeBuf.data(), query, dim);
        break;
      }
      case DistanceMetric::INNER_PRODUCT: {
        for (size_t d = 0; d < dim; ++d) {
          decodeBuf[d] = halfToFloat(vec[d]);
        }
        dist = innerProduct(decodeBuf.data(), query, dim);
        break;
      }
    }
    heap.push(i, dist);
    result.vectorsScanned++;
  }

  result.rows = heap.drainSorted();
  return result;
}

VectorScanResult bruteForceScanInt8(
    const uint8_t* data,
    int64_t numVectors,
    int64_t dimension,
    const float* query,
    int64_t topK,
    DistanceMetric metric,
    const float* mins,
    const float* scales,
    const uint8_t* validBitmap,
    int64_t bitmapSize) {
  VectorScanResult result{};
  result.vectorsScanned = 0;
  result.vectorsSkipped = 0;

  if (data == nullptr || query == nullptr || numVectors <= 0 || dimension <= 0 ||
      topK <= 0 || mins == nullptr || scales == nullptr) {
    return result;
  }

  auto dim = static_cast<size_t>(dimension);

  std::vector<float> decodeBuf(dim);

  VectorTopKHeap heap(static_cast<size_t>(topK));

  for (int64_t i = 0; i < numVectors; ++i) {
    if (validBitmap != nullptr && !isBitSet(validBitmap, i)) {
      result.vectorsSkipped++;
      continue;
    }

    const uint8_t* vec = data + i * dimension;

    // Dequantize per-dimension for all metrics to ensure correctness
    for (size_t d = 0; d < dim; ++d) {
      decodeBuf[d] = mins[d] + static_cast<float>(vec[d]) * scales[d];
    }

    double dist = 0.0;
    switch (metric) {
      case DistanceMetric::L2:
        dist = l2Distance(decodeBuf.data(), query, dim);
        break;
      case DistanceMetric::COSINE:
        dist = cosineDistance(decodeBuf.data(), query, dim);
        break;
      case DistanceMetric::INNER_PRODUCT:
        dist = innerProduct(decodeBuf.data(), query, dim);
        break;
    }
    heap.push(i, dist);
    result.vectorsScanned++;
  }

  result.rows = heap.drainSorted();
  return result;
}

}  // namespace pangrova::vector
