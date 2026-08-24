// LanceVectorIndex: Brute-force ANN search over Lance datasets via C FFI.

#include "lance/LanceVectorIndex.h"

#if PANGROVA_LANCE_AVAILABLE

#include <arrow/c/abi.h>

#include "lance_c_ffi.h"
#include "vector/VectorDistance.h"
#include "vector/VectorTopKHeap.h"

#include <stdexcept>

namespace pangrova::lance {

std::vector<LanceSearchResult> LanceVectorIndex::bruteForceSearch(
    const std::string& datasetUri,
    const float* queryVector,
    int64_t dimension,
    const LanceVectorSearchConfig& config) const {
  if (!queryVector || dimension <= 0 || config.topK <= 0) {
    return {};
  }

  LanceCReader* reader = lance_ffi_reader_open(datasetUri.c_str());
  if (!reader) {
    const char* err = lance_ffi_last_error();
    throw std::runtime_error(
        std::string("Failed to open Lance dataset: ") + (err ? err : "unknown"));
  }

  if (lance_ffi_reader_set_projection(reader, config.columnName.c_str()) != 0) {
    lance_ffi_reader_close(reader);
    throw std::runtime_error("Failed to set projection on Lance reader");
  }

  if (!config.filter.empty()) {
    if (lance_ffi_reader_set_filter(reader, config.filter.c_str()) != 0) {
      lance_ffi_reader_close(reader);
      throw std::runtime_error("Failed to set filter on Lance reader");
    }
  }

  auto distFn = [&](const float* a, const float* b, size_t dim) -> double {
    switch (config.metric) {
      case pangrova::vector::DistanceMetric::L2:
        return pangrova::vector::l2Distance(a, b, dim);
      case pangrova::vector::DistanceMetric::COSINE:
        return pangrova::vector::cosineDistance(a, b, dim);
      case pangrova::vector::DistanceMetric::INNER_PRODUCT:
        return pangrova::vector::innerProduct(a, b, dim);
    }
    return pangrova::vector::l2Distance(a, b, dim);
  };

  pangrova::vector::VectorTopKHeap heap(config.topK);
  int64_t rowOffset = 0;

  // L4-11D: Pre-filter bitset support.
  // When a pre-filter bitset is provided, only rows whose bit is set are
  // considered for distance computation, significantly reducing work for
  // selective WHERE clauses.
  const bool hasPreFilter = !config.preFilterBitset.empty();
  // Simple bitset check (assumes raw bitset encoding: bit i corresponds to row i).
  // For production, this would use roaring bitmap deserialization.
  auto isRowAllowed = [&](int64_t rowId) -> bool {
    if (!hasPreFilter) return true;
    size_t byteIdx = static_cast<size_t>(rowId) / 8;
    uint8_t bitIdx = static_cast<uint8_t>(rowId % 8);
    if (byteIdx >= config.preFilterBitset.size()) return false;
    return (config.preFilterBitset[byteIdx] & (1u << bitIdx)) != 0;
  };

  struct ArrowArray array;
  struct ArrowSchema schema;

  while (true) {
    int32_t status = lance_ffi_reader_next(reader, &array, &schema, 4096);
    if (status == 0) {
      break;
    }
    if (status < 0) {
      lance_ffi_reader_close(reader);
      const char* err = lance_ffi_last_error();
      throw std::runtime_error(
          std::string("Lance read error: ") + (err ? err : "unknown"));
    }

    int64_t numRows = array.length;
    if (numRows > 0 && array.n_children > 0 && array.children[0]) {
      auto* vecArray = array.children[0];
      if (vecArray->n_buffers >= 2 && vecArray->buffers[1]) {
        auto* vectors = static_cast<const float*>(vecArray->buffers[1]);
        for (int64_t i = 0; i < numRows; ++i) {
          // L4-11D: Skip rows not in pre-filter bitset.
          if (!isRowAllowed(rowOffset + i)) {
            continue;
          }
          const float* vec = vectors + i * dimension;
          double dist = distFn(vec, queryVector, static_cast<size_t>(dimension));
          heap.push(rowOffset + i, static_cast<float>(dist));
        }
      }
    }

    rowOffset += numRows;

    if (array.release) {
      array.release(&array);
    }
    if (schema.release) {
      schema.release(&schema);
    }
  }

  lance_ffi_reader_close(reader);

  auto topK = heap.drainSorted();
  std::vector<LanceSearchResult> results;
  results.reserve(topK.size());
  for (auto& row : topK) {
    results.push_back({row.rowId, static_cast<float>(row.distance)});
  }
  return results;
}

int64_t LanceVectorIndex::countVectors(
    const std::string& datasetUri,
    const std::string& columnName) const {
  LanceCReader* reader = lance_ffi_reader_open(datasetUri.c_str());
  if (!reader) {
    return 0;
  }

  if (lance_ffi_reader_set_projection(reader, columnName.c_str()) != 0) {
    lance_ffi_reader_close(reader);
    return 0;
  }

  int64_t count = 0;
  struct ArrowArray array;
  struct ArrowSchema schema;

  while (true) {
    int32_t status = lance_ffi_reader_next(reader, &array, &schema, 8192);
    if (status <= 0) {
      break;
    }
    count += array.length;
    if (array.release) {
      array.release(&array);
    }
    if (schema.release) {
      schema.release(&schema);
    }
  }

  lance_ffi_reader_close(reader);
  return count;
}

}  // namespace pangrova::lance

#else  // !PANGROVA_LANCE_AVAILABLE

namespace pangrova::lance {

std::vector<LanceSearchResult> LanceVectorIndex::bruteForceSearch(
    const std::string&, const float*, int64_t,
    const LanceVectorSearchConfig&) const {
  throw std::runtime_error("Lance C FFI not available in this build");
}

int64_t LanceVectorIndex::countVectors(
    const std::string&, const std::string&) const {
  throw std::runtime_error("Lance C FFI not available in this build");
}

}  // namespace pangrova::lance

#endif  // PANGROVA_LANCE_AVAILABLE
