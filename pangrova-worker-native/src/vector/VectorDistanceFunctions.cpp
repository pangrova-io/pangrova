// VectorDistanceFunctions: Velox VectorFunction implementations for
// l2_distance, cosine_distance, and inner_product.
//
// Each function reads two ARRAY<DOUBLE> arguments, extracts the raw float
// arrays, delegates to the SIMD-optimized VectorDistance.h functions, and
// writes the scalar DOUBLE result.
#include "vector/VectorDistanceFunctions.h"

#include <cmath>
#include <memory>
#include <stdexcept>
#include <vector>

#include "vector/VectorDistance.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::vector {

namespace {

using namespace facebook::velox;

// Batch-extracts float arrays from a decoded ArrayVector into contiguous memory.
// Returns a single contiguous buffer containing all vectors packed sequentially,
// plus a vector of offsets into that buffer for each row.
// This SIMD-friendly layout avoids per-row heap allocation that defeats
// vectorization (Requirement I.2).
struct BatchFloatArrays {
  std::vector<float> data;       // Contiguous float buffer for all rows.
  std::vector<size_t> offsets;   // Per-row offset into data.
  std::vector<size_t> sizes;     // Per-row dimension count.
};

BatchFloatArrays batchExtractFloatArrays(
    const DecodedVector& decoded,
    const SelectivityVector& rows) {
  BatchFloatArrays result;
  auto base = decoded.base()->as<ArrayVector>();
  auto elements = base->elements()->asFlatVector<double>();

  // First pass: compute total size needed.
  size_t total_elements = 0;
  rows.applyToSelected([&](vector_size_t row) {
    if (!decoded.isNullAt(row)) {
      auto index = decoded.index(row);
      total_elements += base->sizeAt(index);
    }
  });

  // Pre-allocate contiguous buffer.
  result.data.reserve(total_elements);
  result.offsets.resize(rows.end(), 0);
  result.sizes.resize(rows.end(), 0);

  // Second pass: extract into contiguous memory.
  rows.applyToSelected([&](vector_size_t row) {
    if (!decoded.isNullAt(row)) {
      auto index = decoded.index(row);
      auto offset = base->offsetAt(index);
      auto size = base->sizeAt(index);
      result.offsets[row] = result.data.size();
      result.sizes[row] = size;
      for (vector_size_t i = 0; i < size; ++i) {
        result.data.push_back(
            static_cast<float>(elements->valueAt(offset + i)));
      }
    }
  });

  return result;
}

// Distance function type matching VectorDistance.h signatures.
using DistanceFn = double (*)(const float*, const float*, size_t);

// Generic Velox VectorFunction for vector distance computation.
// Uses batch extraction for SIMD-friendly contiguous memory layout.
class VectorDistanceFunction : public exec::VectorFunction {
 public:
  explicit VectorDistanceFunction(DistanceFn fn) : fn_(fn) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto localResult = BaseVector::create(DOUBLE(), rows.end(), context.pool());
    auto flatResult = localResult->asFlatVector<double>();

    // Decode both ARRAY arguments.
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto& decodedLeft = *decodedArgs.at(0);
    auto& decodedRight = *decodedArgs.at(1);

    // Batch-extract both arrays into contiguous SIMD-friendly memory.
    auto leftArrays = batchExtractFloatArrays(decodedLeft, rows);
    auto rightArrays = batchExtractFloatArrays(decodedRight, rows);

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedLeft.isNullAt(row) || decodedRight.isNullAt(row)) {
        flatResult->setNull(row, true);
        return;
      }

      auto leftSize = leftArrays.sizes[row];
      auto rightSize = rightArrays.sizes[row];

      if (leftSize != rightSize) {
        // Dimension mismatch — set null rather than crash.
        flatResult->setNull(row, true);
        return;
      }

      const float* leftPtr = leftArrays.data.data() + leftArrays.offsets[row];
      const float* rightPtr = rightArrays.data.data() + rightArrays.offsets[row];

      double dist = fn_(leftPtr, rightPtr, leftSize);
      flatResult->set(row, dist);
    });

    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  DistanceFn fn_;
};

}  // namespace

// Builds a fresh signature vector: (array(double), array(double)) -> double.
// Each registerVectorFunction call takes signatures by value (move),
// so a new vector is needed per registration.
std::vector<exec::FunctionSignaturePtr> makeDistanceSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("double")
              .argumentType("array(double)")
              .argumentType("array(double)")
              .build()};
}

void registerVectorDistanceFunctions() {
  exec::registerVectorFunction(
      "l2_distance",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(l2Distance));

  exec::registerVectorFunction(
      "cosine_distance",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(cosineDistance));

  exec::registerVectorFunction(
      "inner_product",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(innerProduct));
}

}  // namespace pangrova::vector
