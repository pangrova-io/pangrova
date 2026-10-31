#pragma once

#include <string_view>

namespace pangrova::observability {
// Byte counters emitted on the active Velox operator at the actual conversion.
inline constexpr std::string_view kColumnarReadCopiedBytes = "columnarReadCopiedBytes";
inline constexpr std::string_view kColumnarReadOffsetBytes = "columnarReadOffsetBytes";
}  // namespace pangrova::observability
