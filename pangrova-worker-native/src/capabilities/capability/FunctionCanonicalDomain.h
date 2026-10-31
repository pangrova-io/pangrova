#pragma once

#include "pangrova/protocol/v2/capability.pb.h"

namespace pangrova::capability {

// An opaque actual type remains in inventory but cannot advertise canonical
// callability. Concrete per-call binding remains a mandatory separate check.
inline bool hasCanonicalDomain(const protocol::v2::FunctionTypeTemplate& type) {
  if (type.has_opaque_registered_type()) return false;
  for (const auto& slot : type.slots()) {
    if (slot.has_type() && !hasCanonicalDomain(slot.type())) return false;
  }
  return type.has_skeleton() || type.has_type_variable() || type.has_any_type();
}

inline bool hasCanonicalDomain(const protocol::v2::FunctionSignatureContract& signature) {
  if (!signature.has_output() || !hasCanonicalDomain(signature.output())) return false;
  if (signature.has_intermediate() && !hasCanonicalDomain(signature.intermediate())) return false;
  for (const auto& argument : signature.arguments()) {
    if (!hasCanonicalDomain(argument)) return false;
  }
  return true;
}

inline bool hasCanonicalDomain(const protocol::v2::RegisteredFunctionContract& registration) {
  for (const auto& signature : registration.signatures()) {
    if (hasCanonicalDomain(signature)) return true;
  }
  for (const auto& implementation : registration.implementations()) {
    if (implementation.has_signature() && hasCanonicalDomain(implementation.signature())) return true;
    if (implementation.family() == protocol::v2::FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM &&
        !implementation.special_type_program().empty()) return true;
  }
  return false;
}

}  // namespace pangrova::capability
