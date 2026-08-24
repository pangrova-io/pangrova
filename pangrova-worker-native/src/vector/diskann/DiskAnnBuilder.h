// DiskAnnBuilder: Index build orchestrator for DiskANN.
//
// Orchestrates end-to-end index construction:
//   1. Load vectors from source
//   2. Build Vamana graph with alpha-pruning
//   3. Train PQ codebooks and encode vectors
//   4. Serialize index to SSD
//
// Thread-safe: build parallelizes PQ codebook training across subvectors.
// Placement: vector/diskann/ — Native disk-resident vector index.
#pragma once

#include <cstdint>
#include <string>

#include "vector/diskann/VamanaGraph.h"

namespace pangrova::vector::diskann {

struct BuildStats {
  int64_t numVectors;
  int64_t dimension;
  int64_t graphSizeBytes;
  int64_t pqCodeSizeBytes;
  double buildTimeSeconds;
  int64_t medoid;
};

class DiskAnnBuilder {
 public:
  explicit DiskAnnBuilder(VamanaConfig config);

  BuildStats buildFromVectors(
      const float* vectors, int64_t numVectors, int64_t dimension,
      const std::string& outputPath);

 private:
  VamanaConfig config_;
};

}  // namespace pangrova::vector::diskann
