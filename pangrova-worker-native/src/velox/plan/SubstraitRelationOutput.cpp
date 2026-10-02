// Canonical relation output ordinals and task-local physical column identity.
// Logical names remain metadata; they are never lookup keys inside Velox tasks.
#include "velox/plan/VeloxPlanConverter.h"

#include <stdexcept>

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"

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

std::string expressionName(const substrait::Expression& expression,
                           const RowTypePtr& input, std::string computed) {
  if (expression.has_selection()) {
    const auto& reference = expression.selection();
    if (reference.has_root_reference() && reference.has_direct_reference() &&
        reference.direct_reference().has_struct_field()) {
      const auto& field = reference.direct_reference().struct_field();
      if (!field.has_child() && field.field() >= 0 &&
          static_cast<size_t>(field.field()) < input->size()) {
        return input->nameOf(field.field());
      }
    }
  }
  return computed;
}

std::vector<std::string> aggregateNames(const substrait::AggregateRel& aggregate,
                                      const RowTypePtr& input) {
  std::vector<std::string> names;
  std::unordered_set<uint32_t> used;
  for (const auto& grouping : aggregate.groupings()) {
    for (const auto index : grouping.expression_references()) {
      if (index >= static_cast<uint32_t>(aggregate.grouping_expressions_size())) {
        throw std::runtime_error("Grouping expression reference is out of bounds");
      }
      used.emplace(index);
    }
  }
  if (used.size() != static_cast<size_t>(aggregate.grouping_expressions_size())) {
    throw std::runtime_error("Every grouping expression must be referenced");
  }
  for (int index = 0; index < aggregate.grouping_expressions_size(); ++index) {
    names.push_back(expressionName(aggregate.grouping_expressions(index),
        input, "group_" + std::to_string(index)));
  }
  for (int i = 0; i < aggregate.measures_size(); ++i) {
    names.push_back("measure_" + std::to_string(i));
  }
  if (aggregate.groupings_size() > 1) names.push_back("grouping_set_index");
  return names;
}

std::vector<std::string> joinNames(const substrait::Rel& relation,
                                  const std::vector<RowTypePtr>& inputs) {
  const auto& message = payload(relation);
  const auto field = message.GetDescriptor()->FindFieldByName("type");
  const auto kind = message.GetReflection()->GetEnum(message, field)->name();
  const bool right = kind.find("RIGHT") != std::string::npos;
  if (kind.ends_with("_SEMI") || kind.ends_with("_ANTI")) {
    return inputs.at(right ? 1 : 0)->names();
  }
  if (kind.ends_with("_MARK")) {
    auto names = inputs.at(right ? 1 : 0)->names();
    names.push_back("join_mark");
    return names;
  }
  auto names = inputs.at(0)->names();
  const auto& other = inputs.at(1)->names();
  names.insert(names.end(), other.begin(), other.end());
  return names;
}

const substrait::NamedStruct* extensionSchema(
    const pangrova::protocol::v2::RelationDetailEnvelope& envelope) {
  if (envelope.has_retrieval_source()) return &envelope.retrieval_source().output_contract().schema();
  if (envelope.has_retrieval_merge()) return &envelope.retrieval_merge().output_contract().schema();
  if (envelope.has_rerank()) return &envelope.rerank().output_contract().schema();
  if (envelope.has_candidate_fill()) return &envelope.candidate_fill().output_schema();
  if (envelope.has_ai_invoke()) return &envelope.ai_invoke().output_schema();
  if (envelope.has_exchange_input()) return &envelope.exchange_input().output_schema();
  if (envelope.has_table_function()) return &envelope.table_function().output_schema();
  return nullptr;
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
  std::vector<RowTypePtr> inputs;
  for (const auto child : children(relation)) inputs.push_back(relation_output_types_.at(child));
  std::vector<std::string> names;
  if (relation.has_read()) {
    const auto& read = relation.read();
    const auto base = convertNamedStruct(read.base_schema());
    if (read.has_projection()) {
      for (const auto& item : read.projection().select().struct_items()) {
        names.push_back(base->nameOf(item.field()));
      }
    } else {
      names = base->names();
    }
  } else if (relation.has_project()) {
    names = inputs.at(0)->names();
    for (int i = 0; i < relation.project().expressions_size(); ++i) {
      names.push_back(expressionName(relation.project().expressions(i),
          inputs.at(0), "proj_" + std::to_string(i)));
    }
  } else if (relation.has_aggregate()) {
    names = aggregateNames(relation.aggregate(), inputs.at(0));
  } else if (relation.has_window()) {
    names = inputs.at(0)->names();
    for (int i = 0; i < relation.window().window_functions_size(); ++i) {
      names.push_back("window_" + std::to_string(i));
    }
  } else if (relation.has_join() || relation.has_hash_join() ||
             relation.has_merge_join() || relation.has_nested_loop_join()) {
    names = joinNames(relation, inputs);
  } else if (relation.has_cross()) {
    for (const auto& input : inputs) {
      names.insert(names.end(), input->names().begin(), input->names().end());
    }
  } else if (relation.has_extension_leaf() || relation.has_extension_single() ||
             relation.has_extension_multi()) {
    const auto& message = payload(relation);
    const auto field = message.GetDescriptor()->FindFieldByName("detail");
    const auto& detail = static_cast<const google::protobuf::Any&>(
        message.GetReflection()->GetMessage(message, field));
    pangrova::protocol::v2::RelationDetailEnvelope envelope;
    if (!detail.UnpackTo(&envelope) || envelope.schema_version() != 1) {
      throw std::runtime_error("Relation requires a canonical typed detail envelope");
    }
    if (const auto schema = extensionSchema(envelope)) {
      names = convertNamedStruct(*schema)->names();
    } else if (!envelope.has_artifact_build()) {
      throw std::runtime_error("Relation extension has no declared output schema");
    }
  } else {
    names = inputs.at(0)->names();
  }
  if (names.size() != node->outputType()->size()) {
    throw std::runtime_error("Native direct output does not match canonical relation arity");
  }
  return ROW(std::move(names), node->outputType()->children());
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertRel(
    const substrait::Rel& relation, const std::vector<split::SplitConfig>& splits) {
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
  logical_node_types_[node->id()] = logicalOutput;
  return node;
}

void VeloxPlanConverter::bindResultColumnLabels(
    const substrait::RelRoot& root, const RowTypePtr& type) {
  int index = 0;
  for (const auto& child : type->children()) {
    if (index >= root.names_size()) throw std::runtime_error("Root names are missing output fields");
    result_column_labels_.push_back(root.names(index++));
    consumeNestedNames(child, root, index);
  }
  if (index != root.names_size()) throw std::runtime_error("Root names contain extra DFS fields");
}

}  // namespace pangrova::velox
