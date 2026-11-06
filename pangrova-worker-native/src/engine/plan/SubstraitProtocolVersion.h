#pragma once

#include <stdexcept>
#include <string>

#include "substrait/plan.pb.h"

namespace pangrova::plan {

// Wire compatibility policy shared by all Native plan consumers. Keep aligned
// with org.pangrova.protocol.substrait.SubstraitProtocolVersion; accepting a
// version does not imply support for every relation/function in that version.
inline void validateSubstraitVersion(const substrait::Plan& plan) {
  if (!plan.has_version()) {
    throw std::runtime_error("Substrait Plan is missing required Version sub-message");
  }
  const auto& version = plan.version();
  if (version.producer().empty()) {
    throw std::runtime_error("Substrait Plan Version.producer is empty");
  }
  if (version.major_number() != 0) {
    throw std::runtime_error(
        "Substrait spec major-version mismatch: plan declares " +
        std::to_string(version.major_number()) + "; consumer requires major=0");
  }
  // Minor and patch differences are accepted, as on the Java boundary.
}

}  // namespace pangrova::plan
