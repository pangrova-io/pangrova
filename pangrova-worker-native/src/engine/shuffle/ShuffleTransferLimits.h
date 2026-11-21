#pragma once

#include <cstddef>

namespace pangrova::shuffle {

inline constexpr size_t kCanonicalShuffleChunkBytes = 1024 * 1024;
inline constexpr size_t kCanonicalShuffleBatchBytes = 4 * 1024 * 1024;

}  // namespace pangrova::shuffle
