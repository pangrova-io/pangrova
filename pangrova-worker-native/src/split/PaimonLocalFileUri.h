#pragma once

#include <string>

#include "split/LocalFileUri.h"

namespace pangrova::split {

/** Paimon branch directories use the same strict local file URI contract. */
inline std::string decodePaimonLocalFileUri(const std::string& uri) {
  return decodeLocalFileUri(uri);
}

}  // namespace pangrova::split
