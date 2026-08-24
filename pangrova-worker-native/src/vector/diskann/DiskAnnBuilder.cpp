// DiskAnnBuilder: Index build orchestrator implementation.

#include "vector/diskann/DiskAnnBuilder.h"

#include <chrono>
#include <fstream>

namespace pangrova::vector::diskann {

DiskAnnBuilder::DiskAnnBuilder(VamanaConfig config)
    : config_(config) {}

BuildStats DiskAnnBuilder::buildFromVectors(
    const float* vectors, int64_t numVectors, int64_t dimension,
    const std::string& outputPath) {
  auto start = std::chrono::steady_clock::now();

  VamanaIndex index(config_, outputPath);
  index.build(vectors, numVectors, dimension);
  if (!index.saveToDisk(outputPath)) {
    throw std::runtime_error("Failed to save DiskANN index to: " + outputPath);
  }

  // Save companion raw vector file for reranking during search.
  std::string vecsPath = outputPath + ".vecs";
  std::ofstream vecsOut(vecsPath, std::ios::binary);
  if (vecsOut) {
    vecsOut.write(
        reinterpret_cast<const char*>(vectors),
        static_cast<std::streamsize>(
            static_cast<size_t>(numVectors) *
            static_cast<size_t>(dimension) * sizeof(float)));
  }

  auto end = std::chrono::steady_clock::now();
  double elapsed =
      std::chrono::duration<double>(end - start).count();

  int64_t graphBytes = 0;
  for (int64_t i = 0; i < numVectors; ++i) {
    // Each neighbor entry: nodeId (8) + distance (4) = 12 bytes + degree int32
    graphBytes += 4;
  }
  graphBytes += numVectors * config_.maxDegree * 12;

  int64_t pqBytes = numVectors * (dimension / std::max(int64_t{1}, config_.pqSubvectors > 0
      ? config_.pqSubvectors : dimension / 8));

  return BuildStats{
      numVectors, dimension, graphBytes, pqBytes,
      elapsed, index.medoid()};
}

}  // namespace pangrova::vector::diskann
