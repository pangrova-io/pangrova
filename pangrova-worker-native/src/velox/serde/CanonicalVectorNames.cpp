#include "velox/serde/CanonicalVectorNames.h"

#include <stdexcept>
#include <vector>

#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

VectorPtr bind(const VectorPtr& vector, const TypePtr& type) {
  if (!vector || !type || !vector->type()->equivalent(*type)) {
    throw std::invalid_argument("Canonical vector binding requires identical ordinal physical types");
  }
  if (vector->isLazy()) return bind(BaseVector::loadedVectorShared(vector), type);
  const bool sameType = *vector->type() == *type;
  if (vector->encoding() == VectorEncoding::Simple::DICTIONARY) {
    auto values = bind(vector->valueVector(), type);
    if (sameType && values == vector->valueVector()) return vector;
    return BaseVector::wrapInDictionary(vector->nulls(), vector->wrapInfo(), vector->size(),
        std::move(values));
  }
  if (vector->isConstantEncoding()) {
    if (vector->size() == 0 || vector->isNullAt(0)) {
      if (sameType) return vector;
      return BaseVector::createNullConstant(type, vector->size(), vector->pool());
    }
    if (vector->isScalar()) {
      if (sameType) return vector;
      throw std::invalid_argument("Canonical vector binding cannot change primitive type identity");
    }
    auto values = bind(vector->valueVector(), type);
    if (sameType && values == vector->valueVector()) return vector;
    return BaseVector::wrapInConstant(vector->size(), vector->wrapInfo()->as<vector_size_t>()[0],
        std::move(values));
  }
  if (type->isRow()) {
    const auto* row = vector->as<RowVector>();
    std::vector<VectorPtr> children;
    bool unchanged = sameType;
    for (uint32_t i = 0; i < type->size(); ++i) {
      children.push_back(bind(row->childAt(i), type->childAt(i)));
      unchanged = unchanged && children.back() == row->childAt(i);
    }
    if (unchanged) return vector;
    return std::make_shared<RowVector>(vector->pool(), type, vector->nulls(), vector->size(), std::move(children));
  }
  if (type->isArray()) {
    const auto* array = vector->as<ArrayVector>();
    auto elements = bind(array->elements(), type->childAt(0));
    if (sameType && elements == array->elements()) return vector;
    return std::make_shared<ArrayVector>(vector->pool(), type, vector->nulls(), vector->size(),
        array->offsets(), array->sizes(), std::move(elements));
  }
  if (type->isMap()) {
    const auto* map = vector->as<MapVector>();
    auto keys = bind(map->mapKeys(), type->childAt(0));
    auto values = bind(map->mapValues(), type->childAt(1));
    if (sameType && keys == map->mapKeys() && values == map->mapValues()) return vector;
    return std::make_shared<MapVector>(vector->pool(), type, vector->nulls(), vector->size(),
        map->offsets(), map->sizes(), std::move(keys), std::move(values));
  }
  if (sameType) return vector;
  throw std::invalid_argument("Canonical vector binding cannot change primitive type identity");
}
}  // namespace

facebook::velox::VectorPtr bindCanonicalVectorNames(
    const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
  return bind(vector, type);
}

}  // namespace pangrova::velox
