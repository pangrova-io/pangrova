// DiskAnnBuilder: Index build orchestrator implementation.

#include "vector/diskann/DiskAnnBuilder.h"

#include <chrono>
#include <bit>
#include <array>
#include <fstream>
#include <limits>
#include <stdexcept>

namespace pangrova::vector::diskann {

DiskAnnBuilder::DiskAnnBuilder(VamanaConfig config)
    : config_(config) {}

BuildStats DiskAnnBuilder::buildFromVectors(
    const float* vectors, int64_t numVectors, int64_t dimension,
    const std::string& outputPath) {
  if (vectors == nullptr || numVectors <= 0 || dimension <= 0 ||
      outputPath.empty() || config_.maxDegree <= 0 ||
      config_.searchListSize < config_.maxDegree || config_.alpha < 1.0 ||
      config_.alpha > 2.0 || config_.pqSubvectors <= 0 ||
      config_.pqSubvectors > dimension ||
      dimension % config_.pqSubvectors != 0 ||
      config_.pqBitsPerCode <= 0 || config_.pqBitsPerCode > 8 ||
      config_.beamWidth <= 0) {
    throw std::invalid_argument("Invalid bounded DiskANN build configuration");
  }
  const auto vector_count = static_cast<uint64_t>(numVectors);
  const auto vector_dimension = static_cast<uint64_t>(dimension);
  if (vector_count >
      std::numeric_limits<size_t>::max() / vector_dimension / sizeof(float)) {
    throw std::overflow_error("DiskANN raw-vector byte size overflows");
  }
  auto start = std::chrono::steady_clock::now();

  VamanaIndex index(config_, outputPath);
  index.build(vectors, numVectors, dimension);
  if (!index.saveToDisk(outputPath)) {
    throw std::runtime_error("Failed to save DiskANN index to: " + outputPath);
  }

  // Save companion raw vector file for reranking during search.
  std::string vecsPath = outputPath + ".vecs";
  std::ofstream vecsOut(vecsPath, std::ios::binary);
  if (!vecsOut) {
    throw std::runtime_error(
        "Failed to create DiskANN raw-vector companion: " + vecsPath);
  }
  for (uint64_t index = 0; index < vector_count * vector_dimension; ++index) {
    const uint32_t bits = std::bit_cast<uint32_t>(vectors[index]);
    std::array<char, 4> bytes{};
    for (size_t byte = 0; byte < bytes.size(); ++byte) {
      bytes[byte] = static_cast<char>((bits >> (byte * 8)) & 0xffU);
    }
    vecsOut.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  vecsOut.flush();
  if (!vecsOut.good()) {
    throw std::runtime_error(
        "Failed to write DiskANN raw-vector companion: " + vecsPath);
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
