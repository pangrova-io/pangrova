// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <charconv>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include "pangrova/protocol/v2/common.pb.h"

namespace arrow {
class Field;
}  // namespace arrow

namespace pangrova::capability {

// Validates the complete storage declaration against its typed interop schema.
// Task-local physical layout comparison alone cannot prove logical nullability,
// VECTOR dimensions, bounded scalars, or ordered nested field names.
void validateStorageLogicalType(
    const protocol::v2::LogicalType& logical_type, const arrow::Field& field);

inline uint32_t requireFloatVectorDimension(
    const protocol::v2::LogicalType& logical_type) {
  constexpr std::string_view kPrefix = "VECTOR(";
  constexpr uint32_t kMaxDimension = 65'535;
  const std::string_view canonical = logical_type.canonical_sql();
  if (!canonical.starts_with(kPrefix) || canonical.size() <= kPrefix.size() + 1 ||
      canonical.back() != ')') {
    throw std::invalid_argument(
        "Embedding output type must be canonical VECTOR(dimension)");
  }
  uint32_t dimension = 0;
  const char* begin = canonical.data() + kPrefix.size();
  const char* end = canonical.data() + canonical.size() - 1;
  const auto [parsed_end, error] = std::from_chars(begin, end, dimension);
  if (error != std::errc{} || parsed_end != end || dimension == 0 ||
      dimension > kMaxDimension) {
    throw std::invalid_argument("Embedding VECTOR dimension is invalid");
  }
  if (!logical_type.has_substrait_type() ||
      !logical_type.substrait_type().has_list() ||
      logical_type.substrait_type().list().nullability() !=
          substrait::Type_Nullability_NULLABILITY_NULLABLE ||
      !logical_type.substrait_type().list().has_type() ||
      !logical_type.substrait_type().list().type().has_fp32() ||
      logical_type.substrait_type().list().type().fp32().nullability() !=
          substrait::Type_Nullability_NULLABILITY_REQUIRED ||
      logical_type.constraints_size() != 2) {
    throw std::invalid_argument(
        "Embedding VECTOR physical type or constraints are incomplete");
  }
  bool found_dimension = false;
  bool found_element_type = false;
  for (const auto& constraint : logical_type.constraints()) {
    if (constraint.name() == "dimension" &&
        constraint.integer_value() == dimension && !found_dimension) {
      found_dimension = true;
    } else if (constraint.name() == "element_type" &&
               constraint.string_value() == "FLOAT32" &&
               !found_element_type) {
      found_element_type = true;
    } else {
      throw std::invalid_argument(
          "Embedding VECTOR contains an unknown or duplicate constraint");
    }
  }
  if (!found_dimension || !found_element_type) {
    throw std::invalid_argument(
        "Embedding VECTOR dimension or element constraint is missing");
  }
  return dimension;
}

}  // namespace pangrova::capability
