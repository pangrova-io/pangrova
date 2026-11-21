#pragma once

#include <string>

namespace pangrova::velox {

// Derived exclusively from actual aggregate-window registrations at bootstrap.
void registerDistinctAggregateWindows();
std::string distinctAggregateWindowName(const std::string& aggregateName);

}  // namespace pangrova::velox
