// VectorDistanceFunctions: Velox VectorFunction implementations for
// l2_distance, cosine_distance, inner_product, and the Pangrova SQL
// cosine_similarity alias. REAL/REAL uses the float32 SIMD kernel; any DOUBLE
// input is evaluated in double precision without rounding stored values.
#include "vector/VectorDistanceFunctions.h"

#include <cmath>
#include <memory>
#include <utility>
#include <vector>

#include "vector/VectorDistance.h"
#include "velox/common/base/Exceptions.h"
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

// Batch-extract arrays into contiguous memory to avoid per-row allocations.
template <typename T>
struct BatchArrays {
  std::vector<T> data;           // Contiguous elements for all rows.
  std::vector<size_t> offsets;   // Per-row offset into data.
  std::vector<size_t> sizes;     // Per-row dimension count.
};

template <typename T>
BatchArrays<T> batchExtractArrays(
    const DecodedVector& decoded,
    const SelectivityVector& rows) {
  BatchArrays<T> result;
  auto base = decoded.base()->as<ArrayVector>();
  auto elements = base->elements();
  auto elementKind = elements->typeKind();
  VELOX_USER_CHECK(
      elementKind == TypeKind::REAL || elementKind == TypeKind::DOUBLE,
      "Vector distance requires REAL or DOUBLE array elements");
  DecodedVector decodedElements(*elements);

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
        auto elementIndex = offset + i;
        VELOX_USER_CHECK(
            !decodedElements.isNullAt(elementIndex),
            "Vector distance does not accept null elements");
        const double source = elementKind == TypeKind::REAL
            ? decodedElements.valueAt<float>(elementIndex)
            : decodedElements.valueAt<double>(elementIndex);
        VELOX_USER_CHECK(std::isfinite(source),
                         "Vector distance requires finite elements");
        result.data.push_back(static_cast<T>(source));
      }
    }
  });

  return result;
}

using FloatDistanceFn = double (*)(const float*, const float*, size_t);
using DoubleDistanceFn = double (*)(const double*, const double*, size_t);

template <typename T>
void evaluateRows(
    const SelectivityVector& rows,
    const DecodedVector& left,
    const DecodedVector& right,
    FlatVector<double>* result,
    double (*distance)(const T*, const T*, size_t),
    bool similarity) {
  auto leftArrays = batchExtractArrays<T>(left, rows);
  auto rightArrays = batchExtractArrays<T>(right, rows);
  rows.applyToSelected([&](vector_size_t row) {
    if (left.isNullAt(row) || right.isNullAt(row)) {
      result->setNull(row, true);
      return;
    }
    const auto dimension = leftArrays.sizes[row];
    validateVectorDimensions(dimension, rightArrays.sizes[row]);
    const T* leftPtr = leftArrays.data.empty()
        ? nullptr
        : leftArrays.data.data() + leftArrays.offsets[row];
    const T* rightPtr = rightArrays.data.empty()
        ? nullptr
        : rightArrays.data.data() + rightArrays.offsets[row];
    const double value = distance(leftPtr, rightPtr, dimension);
    VELOX_USER_CHECK(std::isfinite(value),
                     "Vector distance result exceeds DOUBLE range");
    result->set(row, similarity ? 1.0 - value : value);
  });
}

// Generic Velox VectorFunction for vector distance computation.
// Uses batch extraction for SIMD-friendly contiguous memory layout.
class VectorDistanceFunction : public exec::VectorFunction {
 public:
  VectorDistanceFunction(
      FloatDistanceFn floatFn,
      DoubleDistanceFn doubleFn,
      bool similarity = false)
      : floatFn_(floatFn), doubleFn_(doubleFn), similarity_(similarity) {}

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

    const auto leftKind = decodedLeft.base()->as<ArrayVector>()
                              ->elements()->typeKind();
    const auto rightKind = decodedRight.base()->as<ArrayVector>()
                               ->elements()->typeKind();
    if (leftKind == TypeKind::REAL && rightKind == TypeKind::REAL) {
      evaluateRows<float>(rows, decodedLeft, decodedRight, flatResult,
                          floatFn_, similarity_);
    } else {
      evaluateRows<double>(rows, decodedLeft, decodedRight, flatResult,
                           doubleFn_, similarity_);
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  FloatDistanceFn floatFn_;
  DoubleDistanceFn doubleFn_;
  bool similarity_;
};

}  // namespace

void validateVectorDimensions(
    std::size_t left_size,
    std::size_t right_size) {
  VELOX_USER_CHECK_EQ(
      left_size,
      right_size,
      "Vector dimension mismatch: left={}, right={}",
      left_size,
      right_size);
}

// Builds both declared VECTOR element precisions and mixed SQL literals.
// Each registerVectorFunction call takes signatures by value (move),
// so a new vector is needed per registration.
std::vector<exec::FunctionSignaturePtr> makeDistanceSignatures() {
  std::vector<exec::FunctionSignaturePtr> signatures;
  for (const auto& elementTypes : {
           std::pair{"array(real)", "array(real)"},
           std::pair{"array(real)", "array(double)"},
           std::pair{"array(double)", "array(real)"},
           std::pair{"array(double)", "array(double)"}}) {
    signatures.push_back(exec::FunctionSignatureBuilder()
                             .returnType("double")
                             .argumentType(elementTypes.first)
                             .argumentType(elementTypes.second)
                             .build());
  }
  return signatures;
}

void registerVectorDistanceFunctions() {
  exec::registerVectorFunction(
      "l2_distance",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(
          static_cast<FloatDistanceFn>(l2Distance),
          static_cast<DoubleDistanceFn>(l2Distance)));

  exec::registerVectorFunction(
      "cosine_distance",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(
          static_cast<FloatDistanceFn>(cosineDistance),
          static_cast<DoubleDistanceFn>(cosineDistance)));

  exec::registerVectorFunction(
      "pangrova_cosine_similarity",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(
          static_cast<FloatDistanceFn>(cosineDistance),
          static_cast<DoubleDistanceFn>(cosineDistance), true));

  exec::registerVectorFunction(
      "inner_product",
      makeDistanceSignatures(),
      std::make_unique<VectorDistanceFunction>(
          static_cast<FloatDistanceFn>(innerProduct),
          static_cast<DoubleDistanceFn>(innerProduct)));
}

}  // namespace pangrova::vector
