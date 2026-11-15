#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#include <google/protobuf/io/coded_stream.h>
#include "substrait/plan.pb.h"

namespace pangrova::plan {

// Protobuf's default nesting budget of 100 rejects plans with chained
// subqueries, such as a recursive CTE resolved into execution levels. Keep
// the budget bounded so malformed input still fails before exhausting the stack.
inline constexpr int kSubstraitPlanRecursionLimit = 1024;

inline bool parseSubstraitPlan(
    const std::uint8_t* data,
    std::size_t size,
    ::substrait::Plan* plan) {
  if (data == nullptr ||
      size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  google::protobuf::io::CodedInputStream input(
      data, static_cast<int>(size));
  input.SetRecursionLimit(kSubstraitPlanRecursionLimit);
  const bool parsed = plan->ParseFromCodedStream(&input);
  return parsed && input.ConsumedEntireMessage();
}

}  // namespace pangrova::plan
