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
  if (*vector->type() == *type) return vector;
  if (vector->isLazy()) return bind(BaseVector::loadedVectorShared(vector), type);
  if (vector->encoding() == VectorEncoding::Simple::DICTIONARY) {
    return BaseVector::wrapInDictionary(vector->nulls(), vector->wrapInfo(), vector->size(),
        bind(vector->valueVector(), type));
  }
  if (vector->isConstantEncoding()) {
    if (vector->size() == 0 || vector->isNullAt(0)) {
      return BaseVector::createNullConstant(type, vector->size(), vector->pool());
    }
    return BaseVector::wrapInConstant(vector->size(), vector->wrapInfo()->as<vector_size_t>()[0],
        bind(vector->valueVector(), type));
  }
  if (type->isRow()) {
    const auto* row = vector->as<RowVector>();
    std::vector<VectorPtr> children;
    for (uint32_t i = 0; i < type->size(); ++i) children.push_back(bind(row->childAt(i), type->childAt(i)));
    return std::make_shared<RowVector>(vector->pool(), type, vector->nulls(), vector->size(), std::move(children));
  }
  if (type->isArray()) {
    const auto* array = vector->as<ArrayVector>();
    return std::make_shared<ArrayVector>(vector->pool(), type, vector->nulls(), vector->size(),
        array->offsets(), array->sizes(), bind(array->elements(), type->childAt(0)));
  }
  if (type->isMap()) {
    const auto* map = vector->as<MapVector>();
    return std::make_shared<MapVector>(vector->pool(), type, vector->nulls(), vector->size(),
        map->offsets(), map->sizes(), bind(map->mapKeys(), type->childAt(0)),
        bind(map->mapValues(), type->childAt(1)));
  }
  throw std::invalid_argument("Canonical vector binding cannot change primitive type identity");
}
}  // namespace

facebook::velox::VectorPtr bindCanonicalVectorNames(
    const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
  return bind(vector, type);
}

}  // namespace pangrova::velox
