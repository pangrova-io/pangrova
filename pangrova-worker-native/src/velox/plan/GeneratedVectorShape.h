#pragma once

#include <charconv>
#include <string_view>

#include "artifact/GeneratedValueSegment.h"

namespace pangrova::velox {
inline uint32_t parseGeneratedVectorDimension(std::string_view canonical_sql) {
  constexpr std::string_view kPrefix = "VECTOR(";
  if (!canonical_sql.starts_with(kPrefix) || canonical_sql.back() != ')') {
    return 0;
  }
  const auto comma = canonical_sql.find(',', kPrefix.size());
  if (comma == std::string_view::npos || comma == kPrefix.size()) {
    return 0;
  }
  uint32_t dimension = 0;
  const auto* begin = canonical_sql.data() + kPrefix.size();
  const auto* end = canonical_sql.data() + comma;
  const auto [parsed_end, error] = std::from_chars(begin, end, dimension);
  if (error != std::errc{} || parsed_end != end || dimension == 0 ||
      dimension > artifact::GeneratedValueSegment::kMaxDimension) {
    return 0;
  }
  return dimension;
}

}  // namespace pangrova::velox

