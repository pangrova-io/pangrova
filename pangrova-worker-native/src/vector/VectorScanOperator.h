// VectorScanOperator: Brute-force vector scan using SIMD distance functions.
//
// Scans contiguous float arrays of vectors, computes distances against a query
// vector using the specified metric (L2, cosine, inner product), and returns
// the Top-K nearest rows. Uses VectorTopKHeap for efficient bounded selection
// and VectorDistance SIMD functions for compute.
//
// Thread-safe: all state is stack-local or owned by the caller.
// Placement: vector/ — Native vector index computation components.
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "vector/VectorTopKHeap.h"
#include "vector/VectorDistance.h"

namespace pangrova::vector {

struct VectorScanResult {
  std::vector<ScoredRow> rows;
  int64_t vectorsScanned;
  int64_t vectorsSkipped;
};

/// Brute-force scans a contiguous array of row vectors against a query vector
/// and returns the Top-K nearest neighbors.
///
/// @param data      Pointer to row-major packed float vectors (numVectors * dimension floats).
/// @param numVectors Number of vectors in the data array.
/// @param dimension  Dimensionality of each vector.
/// @param query     Query vector (dimension floats).
/// @param topK      Number of nearest neighbors to return.
/// @param metric    Distance metric to use.
/// @param validBitmap Optional bitmap — bit i set means row i is valid (pre-filter).
///                   nullptr means all rows are valid.
/// @param bitmapSize Number of bytes in validBitmap (must be >= ceil(numVectors/8) if non-null).
/// @return Scan result with Top-K rows sorted by distance (ascending).
VectorScanResult bruteForceScan(
    const float* data,
    int64_t numVectors,
    int64_t dimension,
    const float* query,
    int64_t topK,
    DistanceMetric metric,
    const uint8_t* validBitmap = nullptr,
    int64_t bitmapSize = 0);

/// Brute-force scan over fp16-encoded vectors. The query vector is fp32 and
/// gets quantized internally. Provides 2x memory bandwidth reduction.
VectorScanResult bruteForceScanFp16(
    const uint16_t* data,
    int64_t numVectors,
    int64_t dimension,
    const float* query,
    int64_t topK,
    DistanceMetric metric,
    const uint8_t* validBitmap = nullptr,
    int64_t bitmapSize = 0);

/// Brute-force scan over int8 scalar-quantized vectors. The query vector is
/// fp32 and gets quantized using the provided per-dimension parameters.
/// Provides 4x memory bandwidth reduction at <1% recall loss with reranking.
VectorScanResult bruteForceScanInt8(
    const uint8_t* data,
    int64_t numVectors,
    int64_t dimension,
    const float* query,
    int64_t topK,
    DistanceMetric metric,
    const float* mins,
    const float* scales,
    const uint8_t* validBitmap = nullptr,
    int64_t bitmapSize = 0);

}  // namespace pangrova::vector
