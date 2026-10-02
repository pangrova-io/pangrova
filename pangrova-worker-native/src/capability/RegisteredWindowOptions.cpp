#include "capability/RegisteredWindowOptions.h"

#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_set>

#include "StructuralContractIdentities.h"

namespace pangrova::capability {

bool registeredWindowIgnoreNulls(const std::string& implementation,
    const google::protobuf::RepeatedPtrField<substrait::FunctionOption>& options) {
  bool ignoreNulls = false;
  std::unordered_set<std::string> seen;
  for (const auto& option : options) {
    if (!seen.emplace(option.name()).second || option.preference_size() == 0 ||
        option.GetReflection()->GetUnknownFields(option).field_count() != 0) {
      throw std::runtime_error("Window option is duplicate, has no preferences, or contains unknown fields");
    }
    const generated::FunctionOptionContract* contract = nullptr;
    for (const auto& candidate : generated::functionOptions) {
      if (implementation == candidate.implementation && option.name() == candidate.name) contract = &candidate;
    }
    if (!contract || std::string_view(contract->behavior) != "NULL_TREATMENT") {
      throw std::runtime_error("Window function does not support declared option: " + implementation + "/" + option.name());
    }
    std::optional<bool> selected;
    // Substrait preferences are ordered negotiation, not implicit coercion.
    for (const auto& preference : option.preference()) {
      if (preference == contract->enabledValue) selected = true;
      else if (preference == contract->disabledValue) selected = false;
      if (selected.has_value()) break;
    }
    if (!selected) throw std::runtime_error("Window option has no supported declared preference: " + option.name());
    ignoreNulls = *selected;
  }
  return ignoreNulls;
}

}  // namespace pangrova::capability
