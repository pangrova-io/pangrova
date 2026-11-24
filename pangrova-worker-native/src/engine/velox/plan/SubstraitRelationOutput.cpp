// Canonical relation output ordinals and task-local physical column identity.
// Logical names remain metadata; they are never lookup keys inside Velox tasks.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include <stdexcept>
#include <unordered_set>

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "capabilities/capability/CanonicalPhysicalTypes.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
const google::protobuf::Message& payload(const substrait::Rel& relation) {
  const auto field = relation.GetDescriptor()->FindFieldByNumber(
      static_cast<int>(relation.rel_type_case()));
  if (!field) throw std::runtime_error("Relation has no declared operator");
  return relation.GetReflection()->GetMessage(relation, field);
}

const substrait::RelCommon& common(const substrait::Rel& relation) {
  const auto& message = payload(relation);
  const auto field = message.GetDescriptor()->FindFieldByName("common");
  if (!field) throw std::runtime_error("Relation operator has no common contract");
  return static_cast<const substrait::RelCommon&>(
      message.GetReflection()->GetMessage(message, field));
}

std::vector<const substrait::Rel*> children(const substrait::Rel& relation) {
  const auto& message = payload(relation);
  std::vector<const substrait::Rel*> result;
  const auto descriptor = message.GetDescriptor();
  const auto reflection = message.GetReflection();
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const auto field = descriptor->field(i);
    if (field->message_type() != substrait::Rel::descriptor()) continue;
    if (field->is_repeated()) {
      for (int j = 0; j < reflection->FieldSize(message, field); ++j) {
        result.push_back(static_cast<const substrait::Rel*>(
            &reflection->GetRepeatedMessage(message, field, j)));
      }
    } else if (reflection->HasField(message, field)) {
      result.push_back(static_cast<const substrait::Rel*>(
          &reflection->GetMessage(message, field)));
    }
  }
  return result;
}

bool projectsDistinctNamedReadFields(const substrait::ReadRel& read) {
  if (!read.has_named_table() || read.named_table().names_size() != 3 ||
      read.named_table().names(2).find('$') != std::string::npos || !read.has_base_schema() ||
      !read.base_schema().has_struct_() || !read.common().has_emit() ||
      read.has_filter() || read.has_best_effort_filter() ||
      read.has_projection()) {
    return false;
  }
  const auto& fields = read.common().emit().output_mapping();
  if (fields.empty() || fields.size() >= read.base_schema().struct_().types_size()) return false;
  return std::unordered_set<int32_t>(fields.begin(), fields.end()).size() ==
      static_cast<size_t>(fields.size());
}

void consumeNestedNames(const TypePtr& type, const substrait::RelRoot& root,
                        int& index) {
  if (type->isRow()) {
    for (uint32_t child = 0; child < type->size(); ++child) {
      if (index >= root.names_size()) throw std::runtime_error("Root names are missing DFS fields");
      ++index;
      consumeNestedNames(type->childAt(child), root, index);
    }
  } else if (type->isArray() || type->isMap()) {
    for (uint32_t child = 0; child < type->size(); ++child) {
      consumeNestedNames(type->childAt(child), root, index);
    }
  }
}
}  // namespace

const RowTypePtr& VeloxPlanConverter::logicalNodeType(
    const std::shared_ptr<const core::PlanNode>& node) const {
  const auto found = logical_node_types_.find(node->id());
  if (found == logical_node_types_.end()) {
    throw std::runtime_error("Plan node has no canonical logical output binding");
  }
  return found->second;
}

RowTypePtr VeloxPlanConverter::directLogicalType(
    const substrait::Rel& relation, const std::shared_ptr<const core::PlanNode>& node) {
  std::vector<substrait::NamedStruct> inputs;
  for (const auto child : children(relation)) inputs.push_back(relation_output_schemas_.at(child));
  auto declared = schema::direct(relation, inputs);
  auto physical = capability::canonicalPhysicalSchema(declared);
  if (!physical->equivalent(*node->outputType())) {
    throw std::runtime_error("Native direct output differs from its canonical relation schema");
  }
  if (std::dynamic_pointer_cast<const core::ExchangeNode>(node)) {
    if (!serde_schema_bindings_.emplace(node->id(), declared).second) {
      throw std::runtime_error("Exchange node has duplicate canonical schema bindings");
    }
  }
  relation_output_schemas_[&relation] = std::move(declared);
  return physical;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertRel(
    const substrait::Rel& relation, const std::vector<split::SplitConfig>& splits) {
  if (relation.has_read() && projectsDistinctNamedReadFields(relation.read())) {
    const auto& read = relation.read();
    auto base = schema::direct(relation, {});
    auto projected = schema::emit(base, read.common());
    auto logicalType = capability::canonicalPhysicalSchema(projected);
    auto node = namedTableScan(read, capability::canonicalPhysicalSchema(base),
        logicalType, physicalRowType(logicalType), nullptr);
    relation_output_types_[&relation] = logicalType;
    relation_output_schemas_[&relation] = std::move(projected);
    logical_node_types_[node->id()] = std::move(logicalType);
    return node;
  }
  auto node = convertDirectRel(relation, splits);
  const auto logicalType = directLogicalType(relation, node);
  const auto physicalType = node->outputType();
  std::vector<std::string> logicalNames;
  std::vector<TypePtr> types;
  std::vector<std::string> aliases;
  std::vector<core::TypedExprPtr> expressions;
  auto emit = [&](int32_t index) {
    if (index < 0 || static_cast<size_t>(index) >= physicalType->size()) {
      throw std::runtime_error("Relation emit ordinal is out of bounds");
    }
    logicalNames.push_back(logicalType->nameOf(index));
    types.push_back(physicalType->childAt(index));
    aliases.push_back(physicalFieldAlias(aliases.size()));
    expressions.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        physicalType->childAt(index), physicalType->nameOf(index)));
  };
  const auto& output = common(relation);
  if (output.has_emit()) {
    for (const auto index : output.emit().output_mapping()) emit(index);
  } else {
    for (size_t i = 0; i < physicalType->size(); ++i) emit(i);
  }
  const auto logicalOutput = ROW(std::move(logicalNames), std::move(types));
  if (output.has_emit() || aliases != physicalType->names()) {
    node = std::make_shared<core::ProjectNode>(nextNodeId(),
        std::move(aliases), std::move(expressions), node);
  }
  relation_output_types_[&relation] = logicalOutput;
  relation_output_schemas_[&relation] = schema::emit(relation_output_schemas_.at(&relation), output);
  logical_node_types_[node->id()] = logicalOutput;
  return node;
}

void VeloxPlanConverter::bindResultColumnLabels(
    const substrait::RelRoot& root, const RowTypePtr& type) {
  result_output_schema_ = canonical_output_schema_;
  result_output_schema_.clear_names();
  for (const auto& name : root.names()) result_output_schema_.add_names(name);
  schema::fields(result_output_schema_);
  int index = 0;
  for (const auto& child : type->children()) {
    if (index >= root.names_size()) throw std::runtime_error("Root names are missing output fields");
    result_column_labels_.push_back(root.names(index++));
    consumeNestedNames(child, root, index);
  }
  if (index != root.names_size()) throw std::runtime_error("Root names contain extra DFS fields");
}

}  // namespace pangrova::velox
