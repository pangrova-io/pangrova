#include "velox/plan/CanonicalPlanSchema.h"

#include <stdexcept>

namespace pangrova::velox::schema {
namespace {
void validate(const substrait::Type& type, int depth, int& remaining) {
  if (depth > 64 || --remaining < 0) {
    throw std::invalid_argument("Canonical value type exceeds its validation bound");
  }
  // Nullability belongs to each declared value, not its physical projection.
  // NamedStruct's outer container is deliberately not passed to this function.
  nullable(type);
  if (type.has_struct_()) {
    for (const auto& child : type.struct_().types()) validate(child, depth + 1, remaining);
  } else if (type.has_list()) {
    validate(type.list().type(), depth + 1, remaining);
  } else if (type.has_map()) {
    validate(type.map().key(), depth + 1, remaining);
    validate(type.map().value(), depth + 1, remaining);
  }
}
}  // namespace

void validateValueType(const substrait::Type& type) {
  int remaining = 2048;
  validate(type, 0, remaining);
}
}  // namespace pangrova::velox::schema
