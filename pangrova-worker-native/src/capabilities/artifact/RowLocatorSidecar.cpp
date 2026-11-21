// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "capabilities/artifact/RowLocatorSidecar.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>

namespace pangrova::artifact {
namespace {

constexpr std::array<char, 8> kMagic{'P', 'G', 'R', 'V', 'L', 'O', 'C', '2'};
constexpr uint64_t kHeaderBytes = 8 + 4 + 4 + 8;

void writeLittleEndian32(std::ostream& output, uint32_t value) {
  std::array<char, 4> bytes{};
  for (size_t index = 0; index < bytes.size(); ++index) {
    bytes[index] = static_cast<char>((value >> (index * 8)) & 0xffU);
  }
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

void writeLittleEndian64(std::ostream& output, uint64_t value) {
  std::array<char, 8> bytes{};
  for (size_t index = 0; index < bytes.size(); ++index) {
    bytes[index] = static_cast<char>((value >> (index * 8)) & 0xffU);
  }
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

uint32_t readLittleEndian32(std::istream& input) {
  std::array<unsigned char, 4> bytes{};
  input.read(
      reinterpret_cast<char*>(bytes.data()),
      static_cast<std::streamsize>(bytes.size()));
  if (!input) {
    throw std::runtime_error("Truncated row-locator sidecar header");
  }
  uint32_t value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint32_t>(bytes[index]) << (index * 8);
  }
  return value;
}

uint64_t readLittleEndian64(std::istream& input) {
  std::array<unsigned char, 8> bytes{};
  input.read(
      reinterpret_cast<char*>(bytes.data()),
      static_cast<std::streamsize>(bytes.size()));
  if (!input) {
    throw std::runtime_error("Truncated row-locator sidecar payload");
  }
  uint64_t value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint64_t>(bytes[index]) << (index * 8);
  }
  return value;
}

}  // namespace

void RowLocatorSidecar::writePhysicalRowPositions(
    const std::string& path,
    std::span<const uint64_t> row_positions) {
  if (path.empty() || row_positions.empty()) {
    throw std::invalid_argument(
        "Row-locator sidecar path and positions must not be empty");
  }
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("Cannot create row-locator sidecar: " + path);
  }
  output.write(kMagic.data(), static_cast<std::streamsize>(kMagic.size()));
  writeLittleEndian32(output, kSchemaVersion);
  writeLittleEndian32(output, kPhysicalRowPositionKind);
  writeLittleEndian64(output, row_positions.size());
  for (uint64_t row_position : row_positions) {
    writeLittleEndian64(output, row_position);
  }
  output.flush();
  if (!output.good()) {
    throw std::runtime_error("Failed to flush row-locator sidecar: " + path);
  }
}

std::vector<uint64_t> RowLocatorSidecar::readPhysicalRowPositions(
    const std::string& path,
    uint64_t max_entries) {
  std::vector<uint64_t> result;
  visitPhysicalRowPositions(path, max_entries,
      [&](uint64_t, uint64_t position) { result.push_back(position); });
  return result;
}

uint64_t RowLocatorSidecar::visitPhysicalRowPositions(
    const std::string& path,
    uint64_t max_entries,
    const std::function<void(uint64_t, uint64_t)>& visitor) {
  if (!visitor) {
    throw std::invalid_argument("Row-locator visitation requires its owner");
  }
  if (path.empty() || max_entries == 0) {
    throw std::invalid_argument(
        "Row-locator sidecar path and max_entries must be valid");
  }
  const uint64_t file_size = std::filesystem::file_size(path);
  if (file_size < kHeaderBytes) {
    throw std::runtime_error("Row-locator sidecar is shorter than its header");
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open row-locator sidecar: " + path);
  }
  std::array<char, 8> magic{};
  input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  if (!input || magic != kMagic) {
    throw std::runtime_error("Invalid row-locator sidecar magic");
  }
  const uint32_t schema_version = readLittleEndian32(input);
  const uint32_t kind = readLittleEndian32(input);
  const uint64_t count = readLittleEndian64(input);
  if (schema_version != kSchemaVersion ||
      kind != kPhysicalRowPositionKind) {
    throw std::runtime_error("Unsupported row-locator sidecar contract");
  }
  if (count == 0 || count > max_entries ||
      count > (std::numeric_limits<uint64_t>::max() - kHeaderBytes) / 8 ||
      file_size != kHeaderBytes + count * 8) {
    throw std::runtime_error("Invalid row-locator sidecar entry count or size");
  }
  for (uint64_t index = 0; index < count; ++index) {
    visitor(index, readLittleEndian64(input));
  }
  return count;
}

}  // namespace pangrova::artifact
