#include "engine/velox/plan/VeloxPlanConverter.h"

#include "capabilities/capability/CanonicalPhysicalTypes.h"
#include "velox/core/Expressions.h"

namespace pangrova::velox {

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertNestedExpression(const substrait::Expression::Nested& nested,
    const facebook::velox::RowTypePtr& input_type) {
  if (nested.type_variation_reference() != 0) {
    throw std::invalid_argument("Nested expression type variation has no registered physical bridge");
  }
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> arguments;
  if (nested.has_struct_()) {
    std::vector<facebook::velox::TypePtr> types;
    for (const auto& field : nested.struct_().fields()) {
      const auto expression = convertExpression(field, input_type);
      types.push_back(expression->type());
      arguments.push_back(expression);
    }
    // Actual RowConstructor special form accepts caller-provided field names;
    // its resolver checks child types and ROW::equivalent intentionally ignores names.
    return checkedScalarCall("row_constructor", pangrova::capability::canonicalComputedRowType(std::move(types)),
        std::move(arguments));
  }
  if (nested.has_list()) {
    if (nested.list().values_size() == 0) {
      throw std::invalid_argument("Empty Nested.List must use an explicit typed empty-list literal");
    }
    for (const auto& value : nested.list().values()) arguments.push_back(convertExpression(value, input_type));
    const auto element = arguments.front()->type();
    for (const auto& argument : arguments) {
      if (!element->equivalent(*argument->type())) {
        throw std::invalid_argument("Nested.List requires exactly typed homogeneous elements; CAST explicitly");
      }
    }
    return checkedScalarCall("array_constructor", facebook::velox::ARRAY(element), std::move(arguments));
  }
  throw std::invalid_argument("Nested expression has no supported exact constructor");
}

}  // namespace pangrova::velox
