#pragma once

#include <charconv>
#include <string_view>

namespace pangrova::velox {
inline uint32_t parseGeneratedVectorDimension(std::string_view canonical_sql) {
  constexpr std::string_view kPrefix = "VECTOR(";
  if (!canonical_sql.starts_with(kPrefix) || canonical_sql.back() != ')' ||
      canonical_sql.size() <= kPrefix.size() + 1) {
    return 0;
  }
  uint32_t dimension = 0;
  const auto* begin = canonical_sql.data() + kPrefix.size();
  const auto* end = canonical_sql.data() + canonical_sql.size() - 1;
  const auto [parsed_end, error] = std::from_chars(begin, end, dimension);
  if (error != std::errc{} || parsed_end != end || dimension == 0 ||
      dimension > 65'535) {  // Cross-runtime VECTOR logical type limit.
    return 0;
  }
  return dimension;
}

}  // namespace pangrova::velox
