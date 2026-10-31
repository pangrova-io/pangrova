// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <vector>

namespace pangrova::artifact {

/**
 * Compact, immutable node-ordinal to row-locator sidecar.
 *
 * Schema v1 stores source-object-scoped physical positions. The source object
 * is carried by ArtifactObject, so each entry is one unsigned little-endian
 * 64-bit row position. Readers validate the complete header and exact file
 * length before exposing any locator.
 */
class RowLocatorSidecar final {
 public:
  static constexpr uint32_t kSchemaVersion = 1;
  static constexpr uint32_t kPhysicalRowPositionKind = 1;

  static void writePhysicalRowPositions(
      const std::string& path,
      std::span<const uint64_t> row_positions);

  static std::vector<uint64_t> readPhysicalRowPositions(
      const std::string& path,
      uint64_t max_entries);

  // Validates the same complete file contract while letting the task owner
  // place its locator index in governed storage without a duplicate vector.
  static uint64_t visitPhysicalRowPositions(
      const std::string& path,
      uint64_t max_entries,
      const std::function<void(uint64_t, uint64_t)>& visitor);
};

}  // namespace pangrova::artifact
