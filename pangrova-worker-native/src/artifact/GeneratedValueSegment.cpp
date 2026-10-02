// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "artifact/GeneratedValueSegment.h"

#include <array>
#include <bit>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>

namespace pangrova::artifact {
namespace {

constexpr std::array<char, 8> kMagic{'P', 'G', 'G', 'E', 'N', 'V', '0', '2'};
constexpr uint64_t kHeaderBytes = 8 + 4 + 4 + 4 + 4 + 8;
constexpr uint32_t kValidityEncoding = 1;

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
  input.read(reinterpret_cast<char*>(bytes.data()),
             static_cast<std::streamsize>(bytes.size()));
  if (!input) {
    throw std::runtime_error("Truncated generated-value segment");
  }
  uint32_t value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint32_t>(bytes[index]) << (index * 8);
  }
  return value;
}

uint64_t readLittleEndian64(std::istream& input) {
  std::array<unsigned char, 8> bytes{};
  input.read(reinterpret_cast<char*>(bytes.data()),
             static_cast<std::streamsize>(bytes.size()));
  if (!input) {
    throw std::runtime_error("Truncated generated-value segment");
  }
  uint64_t value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint64_t>(bytes[index]) << (index * 8);
  }
  return value;
}

uint64_t payloadBytes(uint64_t rows, uint32_t dimension) {
  if (rows == 0 || dimension == 0 ||
      rows > std::numeric_limits<uint64_t>::max() / dimension ||
      rows * dimension > std::numeric_limits<uint64_t>::max() / sizeof(float)) {
    throw std::invalid_argument("Invalid generated-value segment dimensions");
  }
  return rows * dimension * sizeof(float);
}

uint64_t bitmapBytes(uint64_t rows) { return rows / 8 + (rows % 8 != 0); }

void requireValue(float value, bool valid) {
  if (!std::isfinite(value) ||
      (!valid && std::bit_cast<uint32_t>(value) != 0)) {
    throw std::invalid_argument(
        "Generated-value payload must be finite, with "
        "positive zero for NULL rows");
  }
}

}  // namespace

void GeneratedValueSegment::write(const std::string& path,
                                  std::span<const float> values,
                                  std::span<const uint8_t> validity,
                                  uint64_t row_count, uint32_t dimension) {
  if (path.empty() || dimension > kMaxDimension ||
      payloadBytes(row_count, dimension) / sizeof(float) != values.size() ||
      validity.size() != row_count) {
    throw std::invalid_argument(
        "Generated-value segment path, row count or dimension is invalid");
  }
  std::vector<uint8_t> bitmap(static_cast<size_t>(bitmapBytes(row_count)), 0);
  for (size_t row = 0; row < validity.size(); ++row) {
    if (validity[row] > 1) {
      throw std::invalid_argument(
          "Generated-value validity must contain only 0 or 1");
    }
    bitmap[row / 8] |= static_cast<uint8_t>(validity[row] << (row % 8));
  }
  for (size_t index = 0; index < values.size(); ++index) {
    requireValue(values[index], validity[index / dimension] != 0);
  }
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("Cannot create generated-value segment: " + path);
  }
  output.write(kMagic.data(), static_cast<std::streamsize>(kMagic.size()));
  writeLittleEndian32(output, kSchemaVersion);
  writeLittleEndian32(output, kFloatVectorKind);
  writeLittleEndian32(output, dimension);
  writeLittleEndian32(output, kValidityEncoding);
  writeLittleEndian64(output, row_count);
  output.write(reinterpret_cast<const char*>(bitmap.data()),
               static_cast<std::streamsize>(bitmap.size()));
  for (float value : values) {
    writeLittleEndian32(output, std::bit_cast<uint32_t>(value));
  }
  output.flush();
  if (!output.good()) {
    throw std::runtime_error("Failed to flush generated-value segment: " +
                             path);
  }
}

GeneratedValueSegmentData GeneratedValueSegment::read(
    const std::string& path, uint64_t expected_rows,
    uint32_t expected_dimension, uint64_t max_bytes) {
  if (path.empty() || expected_rows == 0 || expected_dimension == 0 ||
      expected_dimension > kMaxDimension || max_bytes < kHeaderBytes) {
    throw std::invalid_argument("Invalid generated-value read contract");
  }
  const uint64_t file_size = std::filesystem::file_size(path);
  if (file_size > max_bytes || file_size < kHeaderBytes) {
    throw std::runtime_error(
        "Generated-value segment exceeds its byte grant or is truncated");
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open generated-value segment: " + path);
  }
  std::array<char, 8> magic{};
  input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  if (!input || magic != kMagic ||
      readLittleEndian32(input) != kSchemaVersion ||
      readLittleEndian32(input) != kFloatVectorKind) {
    throw std::runtime_error("Unsupported generated-value segment contract");
  }
  const uint32_t dimension = readLittleEndian32(input);
  const uint32_t encoding = readLittleEndian32(input);
  const uint64_t rows = readLittleEndian64(input);
  const uint64_t payload = payloadBytes(rows, dimension);
  const uint64_t bitmap_size = bitmapBytes(rows);
  if (encoding != kValidityEncoding || rows != expected_rows ||
      dimension != expected_dimension ||
      payload >
          std::numeric_limits<uint64_t>::max() - kHeaderBytes - bitmap_size ||
      file_size != kHeaderBytes + bitmap_size + payload) {
    throw std::runtime_error(
        "Generated-value segment row count, dimension or size mismatch");
  }
  GeneratedValueSegmentData result;
  result.dimension = dimension;
  result.row_count = rows;
  std::vector<uint8_t> bitmap(static_cast<size_t>(bitmap_size));
  input.read(reinterpret_cast<char*>(bitmap.data()),
             static_cast<std::streamsize>(bitmap.size()));
  if (!input || (rows % 8 != 0 && (bitmap.back() >> (rows % 8)) != 0)) {
    throw std::runtime_error(
        "Generated-value validity bitmap is truncated or non-canonical");
  }
  result.validity.reserve(static_cast<size_t>(rows));
  for (uint64_t row = 0; row < rows; ++row) {
    result.validity.push_back((bitmap[row / 8] >> (row % 8)) & 1U);
  }
  result.values.reserve(static_cast<size_t>(payload / sizeof(float)));
  for (uint64_t index = 0; index < payload / sizeof(float); ++index) {
    const float value = std::bit_cast<float>(readLittleEndian32(input));
    if (!std::isfinite(value) || (result.validity[index / dimension] == 0 &&
                                  std::bit_cast<uint32_t>(value) != 0)) {
      throw std::runtime_error(
          "Generated-value segment has a non-finite or "
          "non-canonical NULL payload");
    }
    result.values.push_back(value);
  }
  return result;
}

}  // namespace pangrova::artifact
