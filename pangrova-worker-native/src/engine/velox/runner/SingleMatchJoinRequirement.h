#pragma once

#include <string>

namespace pangrova::velox {

// One actual join-node cardinality obligation. Hash joins may preserve the
// build side; other lowerings normalize SINGLE to the actual left/probe side.
struct SingleMatchJoinRequirement {
  std::string nodeId;
  bool inputIsHashBuild = false;
  bool operator==(const SingleMatchJoinRequirement&) const = default;
};

}  // namespace pangrova::velox
