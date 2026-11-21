// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace pangrova::artifact {

/**
 * Deterministic single-object representation of one source object's generated
 * VECTOR values. Row identity deliberately remains in the separately signed
 * ROW_LOCATORS object so the two roles can be validated independently.
 */
struct GeneratedValueSegmentData final {
  uint32_t dimension{0};
  uint64_t row_count{0};
  std::vector<float> values;
  // One exact 0/1 flag per physical row. NULL rows retain their locator and
  // have canonical positive-zero payload, never an invented embedding.
  std::vector<uint8_t> validity;
};

class GeneratedValueSegment final {
 public:
  static constexpr uint32_t kSchemaVersion = 2;
  static constexpr uint32_t kFloatVectorKind = 1;
  static constexpr uint32_t kMaxDimension = 65'536;

  static void write(const std::string& path, std::span<const float> values,
                    std::span<const uint8_t> validity, uint64_t row_count,
                    uint32_t dimension);

  static GeneratedValueSegmentData read(const std::string& path,
                                        uint64_t expected_rows,
                                        uint32_t expected_dimension,
                                        uint64_t max_bytes);
};

}  // namespace pangrova::artifact
