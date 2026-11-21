#include "engine/velox/serde/CanonicalResultNullability.h"

#include "capabilities/capability/CanonicalPhysicalTypes.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "velox/vector/DecodedVector.h"

#include <stdexcept>
#include <string>

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

bool required(const substrait::Type& type) {
  return !schema::nullable(type);
}

void values(const substrait::Type& type, const VectorPtr& vector,
            const SelectivityVector& active, const std::string& path, bool key = false);

SelectivityVector validBaseRows(const DecodedVector& decoded,
    const SelectivityVector& active, const std::string& path, bool mustExist) {
  SelectivityVector result(decoded.base()->size(), false);
  active.applyToSelected([&](vector_size_t row) {
    if (decoded.isNullAt(row)) {
      if (mustExist) throw std::runtime_error("Required canonical result contains NULL: " + path);
      return;
    }
    result.setValid(decoded.index(row), true);
  });
  result.updateBounds();
  return result;
}

template <typename Collection>
SelectivityVector collectionElements(const Collection& collection,
    const SelectivityVector& active, vector_size_t childSize) {
  SelectivityVector result(childSize, false);
  active.applyToSelected([&](vector_size_t row) {
    const auto offset = collection.offsetAt(row);
    const auto size = collection.sizeAt(row);
    if (offset < 0 || size < 0 || offset > childSize || size > childSize - offset) {
      throw std::runtime_error("Canonical result collection is outside its actual child values");
    }
    result.setValidRange(offset, offset + size, true);
  });
  result.updateBounds();
  return result;
}

void values(const substrait::Type& type, const VectorPtr& vector,
            const SelectivityVector& active, const std::string& path, bool key) {
  if (!vector || active.size() > vector->size()) throw std::runtime_error("Canonical result child shape is invalid");
  if (!active.hasSelections()) return;
  const bool nested = type.has_struct_() || type.has_list() || type.has_map();
  if (!nested && !vector->mayHaveNulls()) return;
  DecodedVector decoded(*vector, active);
  const auto selected = validBaseRows(decoded, active, path, key || required(type));
  if (!selected.hasSelections() || !nested) return;
  if (type.has_struct_()) {
    const auto* row = decoded.base()->as<RowVector>();
    if (row->childrenSize() != static_cast<size_t>(type.struct_().types_size())) {
      throw std::runtime_error("Canonical result ROW arity differs");
    }
    for (int ordinal = 0; ordinal < type.struct_().types_size(); ++ordinal) {
      values(type.struct_().types(ordinal), row->childAt(ordinal), selected, path + "." + std::to_string(ordinal));
    }
  } else if (type.has_list()) {
    const auto* array = decoded.base()->as<ArrayVector>();
    values(type.list().type(), array->elements(),
        collectionElements(*array, selected, array->elements()->size()), path + "[]");
  } else {
    const auto* map = decoded.base()->as<MapVector>();
    const auto elements = collectionElements(*map, selected, map->mapKeys()->size());
    values(type.map().key(), map->mapKeys(), elements, path + ".key", true);
    values(type.map().value(), map->mapValues(), elements, path + ".value");
  }
}
}  // namespace

RowTypePtr admittedCanonicalResultType(const substrait::NamedStruct& schema) {
  const auto declared = capability::canonicalPhysicalSchema(schema);
  for (const auto& type : schema.struct_().types()) schema::validateValueType(type);
  return declared;
}

void validateCanonicalResultNullability(const substrait::NamedStruct& schema,
                                      const RowVectorPtr& rows) {
  const auto declared = admittedCanonicalResultType(schema);
  if (!rows || rows->childrenSize() != declared->size()) {
    throw std::invalid_argument("Canonical result does not match its admitted physical schema");
  }
  for (uint32_t ordinal = 0; ordinal < declared->size(); ++ordinal) {
    if (*declared->childAt(ordinal) != *rows->type()->childAt(ordinal)) {
      throw std::invalid_argument("Canonical result differs from its admitted nested types or names");
    }
  }
  substrait::Type type;
  *type.mutable_struct_() = schema.struct_();
  if (type.struct_().nullability() == substrait::Type::NULLABILITY_UNSPECIFIED) {
    // NamedStruct's outer container is not an expression ROW value. All
    // actual field/child nullability below it must still be explicit.
    type.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  }
  schema::validateValueType(type);
  values(type, rows, SelectivityVector(rows->size()), "result");
}

}  // namespace pangrova::velox
