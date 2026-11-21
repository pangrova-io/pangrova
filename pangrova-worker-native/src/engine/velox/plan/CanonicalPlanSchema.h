#pragma once

#include <string>
#include <vector>

#include "substrait/algebra.pb.h"

namespace pangrova::velox::schema {

// Schema metadata only, reconstructed from the one canonical Substrait IR.
struct Field {
  std::string name;
  substrait::Type type;
  std::vector<std::string> descendants;
};

std::vector<Field> fields(const substrait::NamedStruct& schema);
substrait::NamedStruct named(const std::vector<Field>& fields,
                            const substrait::Type_Struct* root = nullptr);
Field computed(std::string name, substrait::Type type);
substrait::Type primitive(const std::string& kind, substrait::Type::Nullability nullable);
substrait::Type withNullability(substrait::Type type, substrait::Type::Nullability nullable);
bool nullable(const substrait::Type& type);
void validateValueType(const substrait::Type& type);
substrait::Type merge(const std::vector<substrait::Type>& types);
substrait::Type expression(const substrait::Expression& expression, const substrait::NamedStruct& input);
Field expressionField(const substrait::Expression& expression, const substrait::NamedStruct& input,
                      std::string computedName);
substrait::NamedStruct mask(const substrait::NamedStruct& input,
                          const substrait::Expression_MaskExpression& mask);
substrait::NamedStruct direct(const substrait::Rel& relation,
                             const std::vector<substrait::NamedStruct>& inputs);
substrait::NamedStruct emit(const substrait::NamedStruct& schema, const substrait::RelCommon& common);

}  // namespace pangrova::velox::schema
