#include "velox/plan/CanonicalPlanSchema.h"

#include <stdexcept>

namespace pangrova::velox::schema {
namespace {
using Mask = substrait::Expression_MaskExpression;
using Type = substrait::Type;

Field select(Field field, const Mask::Select& selection, bool maintain);

substrait::NamedStruct selectStruct(const substrait::NamedStruct& input,
                                    const Mask::StructSelect& selection, bool maintain) {
  const auto source = fields(input);
  std::vector<Field> output;
  for (const auto& item : selection.struct_items()) {
    if (item.field() < 0 || static_cast<size_t>(item.field()) >= source.size()) {
      throw std::invalid_argument("Canonical mask ordinal is out of bounds");
    }
    auto field = source[item.field()];
    output.push_back(item.has_child() ? select(std::move(field), item.child(), maintain) : std::move(field));
  }
  return named(output, &input.struct_());
}

Field selectStructField(Field field, const Mask::StructSelect& selection, bool maintain) {
  if (!field.type.has_struct_()) throw std::invalid_argument("Canonical struct mask requires a struct");
  substrait::NamedStruct input;
  *input.mutable_struct_() = field.type.struct_();
  for (const auto& name : field.descendants) input.add_names(name);
  auto selected = selectStruct(input, selection, maintain);
  if (!maintain && selected.struct_().types_size() == 1) {
    auto child = fields(selected).front();
    child.name = field.name;
    if (nullable(field.type)) child.type = withNullability(std::move(child.type), Type::NULLABILITY_NULLABLE);
    return child;
  }
  *field.type.mutable_struct_() = selected.struct_();
  field.descendants.assign(selected.names().begin(), selected.names().end());
  return field;
}

Field selectList(Field field, const Mask::ListSelect& selection, bool maintain) {
  if (!field.type.has_list()) throw std::invalid_argument("Canonical list mask requires a list");
  Field child{field.name, field.type.list().type(), field.descendants};
  if (selection.has_child()) child = select(std::move(child), selection.child(), maintain);
  if (!maintain && selection.selection_size() == 1 && selection.selection(0).has_item()) {
    child.type = withNullability(std::move(child.type), Type::NULLABILITY_NULLABLE);
    return child;
  }
  *field.type.mutable_list()->mutable_type() = child.type;
  field.descendants = std::move(child.descendants);
  return field;
}

Field selectMap(Field field, const Mask::MapSelect& selection, bool maintain) {
  if (!field.type.has_map()) throw std::invalid_argument("Canonical map mask requires a map");
  // Canonical DFS visits key then value, including all nested key names.
  const auto keyCount = computed("key", field.type.map().key()).descendants.size();
  if (keyCount > field.descendants.size()) throw std::invalid_argument("Canonical map DFS names are missing");
  Field value{field.name, field.type.map().value(), {field.descendants.begin() + keyCount, field.descendants.end()}};
  if (selection.has_child()) value = select(std::move(value), selection.child(), maintain);
  *field.type.mutable_map()->mutable_value() = value.type;
  field.descendants.resize(keyCount);
  field.descendants.insert(field.descendants.end(), value.descendants.begin(), value.descendants.end());
  return field;
}

Field select(Field field, const Mask::Select& selection, bool maintain) {
  if (selection.has_struct_()) return selectStructField(std::move(field), selection.struct_(), maintain);
  if (selection.has_list()) return selectList(std::move(field), selection.list(), maintain);
  if (selection.has_map()) return selectMap(std::move(field), selection.map(), maintain);
  throw std::invalid_argument("Canonical mask has no supported child");
}
}  // namespace

substrait::NamedStruct mask(const substrait::NamedStruct& input, const Mask& selection) {
  return selectStruct(input, selection.select(), selection.maintain_singular_struct());
}
}  // namespace pangrova::velox::schema
