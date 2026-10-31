#pragma once

#include <string>

namespace pangrova::velox {

// The pinned Paimon SDK exposes Float/Double.toString in its statistics and
// key strings. Preserve the supported JDK 17 text contract in Native code.
std::string paimonFloatingText(float value);
std::string paimonFloatingText(double value);

}  // namespace pangrova::velox
