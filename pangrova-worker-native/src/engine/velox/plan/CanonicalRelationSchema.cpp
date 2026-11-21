#include "engine/velox/plan/CanonicalPlanSchema.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

#include "pangrova/protocol/v2/plan_extension.pb.h"

namespace pangrova::velox::schema {
namespace {
using Type = substrait::Type;
using NamedStruct = substrait::NamedStruct;

NamedStruct concatenate(const std::vector<NamedStruct>& inputs) {
  std::vector<Field> output;
  for (const auto& input : inputs) {
    const auto children = fields(input);
    output.insert(output.end(), children.begin(), children.end());
  }
  return named(output);
}

NamedStruct aggregate(const substrait::AggregateRel& relation, const NamedStruct& input) {
  std::vector<Field> output;
  std::unordered_set<uint32_t> used;
  for (const auto& grouping : relation.groupings()) {
    for (const auto ordinal : grouping.expression_references()) {
      if (ordinal >= static_cast<uint32_t>(relation.grouping_expressions_size())) {
        throw std::invalid_argument("Grouping reference is outside its declared expressions");
      }
      used.insert(ordinal);
    }
  }
  if (used.size() != static_cast<size_t>(relation.grouping_expressions_size())) {
    throw std::invalid_argument("Every declared grouping expression must be used");
  }
  for (int i = 0; i < relation.grouping_expressions_size(); ++i) {
    auto field = expressionField(relation.grouping_expressions(i), input, "group_" + std::to_string(i));
    for (const auto& grouping : relation.groupings()) {
      if (std::find(grouping.expression_references().begin(), grouping.expression_references().end(), i)
          == grouping.expression_references().end()) {
        field.type = withNullability(std::move(field.type), Type::NULLABILITY_NULLABLE);
      }
    }
    output.push_back(std::move(field));
  }
  for (int i = 0; i < relation.measures_size(); ++i) {
    output.push_back(computed("measure_" + std::to_string(i), relation.measures(i).measure().output_type()));
  }
  if (relation.groupings_size() > 1) {
    output.push_back(computed("grouping_set_index", primitive("i32", Type::NULLABILITY_REQUIRED)));
  }
  return named(output);
}

std::string joinKind(const substrait::Rel& relation) {
  const auto relField = relation.GetDescriptor()->FindFieldByNumber(relation.rel_type_case());
  const auto& message = relation.GetReflection()->GetMessage(relation, relField);
  const auto typeField = message.GetDescriptor()->FindFieldByName("type");
  if (!typeField) throw std::invalid_argument("Join has no canonical kind");
  return message.GetReflection()->GetEnum(message, typeField)->name();
}

NamedStruct join(const substrait::Rel& relation, const std::vector<NamedStruct>& inputs) {
  if (inputs.size() != 2) throw std::invalid_argument("Join requires two canonical input schemas");
  auto left = fields(inputs[0]);
  auto right = fields(inputs[1]);
  const auto kind = joinKind(relation);
  const bool rightSide = kind.find("RIGHT") != std::string::npos;
  if (kind.ends_with("_SEMI") || kind.ends_with("_ANTI")) return named(rightSide ? right : left);
  if (kind.ends_with("_MARK")) {
    auto output = rightSide ? right : left;
    output.push_back(computed("join_mark", primitive("bool", Type::NULLABILITY_NULLABLE)));
    return named(output);
  }
  const bool full = kind.ends_with("_OUTER");
  const bool single = kind.ends_with("_SINGLE");
  const bool leftPad = full || (rightSide && (single || kind.ends_with("_RIGHT")));
  const bool rightPad = full || (!rightSide && (single || kind.ends_with("_LEFT")));
  if (leftPad) for (auto& field : left) field.type = withNullability(std::move(field.type), Type::NULLABILITY_NULLABLE);
  if (rightPad) for (auto& field : right) field.type = withNullability(std::move(field.type), Type::NULLABILITY_NULLABLE);
  left.insert(left.end(), right.begin(), right.end());
  return named(left);
}

NamedStruct set(const substrait::SetRel& relation, const std::vector<NamedStruct>& inputs) {
  if (inputs.size() < 2 || relation.op() == substrait::SetRel::SET_OP_UNSPECIFIED) {
    throw std::invalid_argument("Set requires a canonical operation and two inputs");
  }
  auto output = fields(inputs[0]);
  for (const auto& input : inputs) {
    if (fields(input).size() != output.size()) throw std::invalid_argument("Set input arity differs");
  }
  const bool unionAll = relation.op() == substrait::SetRel::SET_OP_UNION_ALL
      || relation.op() == substrait::SetRel::SET_OP_UNION_DISTINCT;
  for (size_t i = 0; i < output.size(); ++i) {
    std::vector<Type> types;
    for (const auto& input : inputs) types.push_back(input.struct_().types(i));
    const auto merged = merge(types);
    bool allowsNull = nullable(types[0]);
    if (relation.op() == substrait::SetRel::SET_OP_INTERSECTION_PRIMARY) {
      allowsNull = allowsNull && std::any_of(types.begin() + 1, types.end(), nullable);
    } else if (relation.op() == substrait::SetRel::SET_OP_INTERSECTION_MULTISET
        || relation.op() == substrait::SetRel::SET_OP_INTERSECTION_MULTISET_ALL) {
      allowsNull = allowsNull && std::all_of(types.begin(), types.end(), nullable);
    }
    output[i].type = unionAll ? merged : withNullability(types[0], allowsNull
        ? Type::NULLABILITY_NULLABLE : Type::NULLABILITY_REQUIRED);
  }
  return named(output, &inputs[0].struct_());
}

NamedStruct extension(const substrait::Rel& relation) {
  const auto& detail = relation.has_extension_leaf() ? relation.extension_leaf().detail()
      : relation.has_extension_single() ? relation.extension_single().detail() : relation.extension_multi().detail();
  protocol::v2::RelationDetailEnvelope envelope;
  if (!detail.UnpackTo(&envelope) || envelope.schema_version() != 1) {
    throw std::invalid_argument("Relation requires its canonical typed output envelope");
  }
  NamedStruct result;
  switch (envelope.detail_case()) {
    case protocol::v2::RelationDetailEnvelope::kRetrievalSource: result = envelope.retrieval_source().output_contract().schema(); break;
    case protocol::v2::RelationDetailEnvelope::kRetrievalMerge: result = envelope.retrieval_merge().output_contract().schema(); break;
    case protocol::v2::RelationDetailEnvelope::kRerank: result = envelope.rerank().output_contract().schema(); break;
    case protocol::v2::RelationDetailEnvelope::kCandidateFill: result = envelope.candidate_fill().output_schema(); break;
    case protocol::v2::RelationDetailEnvelope::kAiInvoke: result = envelope.ai_invoke().output_schema(); break;
    case protocol::v2::RelationDetailEnvelope::kExchangeInput: result = envelope.exchange_input().output_schema(); break;
    case protocol::v2::RelationDetailEnvelope::kTableFunction: result = envelope.table_function().output_schema(); break;
    case protocol::v2::RelationDetailEnvelope::kArtifactBuild: return named({});
    default: throw std::invalid_argument("Relation envelope has no canonical output schema");
  }
  fields(result);
  return result;
}
}  // namespace

substrait::NamedStruct direct(const substrait::Rel& relation, const std::vector<NamedStruct>& inputs) {
  if (relation.has_read()) {
    const auto& read = relation.read();
    fields(read.base_schema());
    return read.has_projection() ? mask(read.base_schema(), read.projection()) : read.base_schema();
  }
  if (relation.has_project()) {
    const auto& input = inputs.at(0);
    auto output = fields(input);
    for (int i = 0; i < relation.project().expressions_size(); ++i) {
      output.push_back(expressionField(relation.project().expressions(i), input, "proj_" + std::to_string(i)));
    }
    return named(output, &input.struct_());
  }
  if (relation.has_aggregate()) return aggregate(relation.aggregate(), inputs.at(0));
  if (relation.has_window()) {
    auto output = fields(inputs.at(0));
    for (int i = 0; i < relation.window().window_functions_size(); ++i) {
      output.push_back(computed("window_" + std::to_string(i), relation.window().window_functions(i).output_type()));
    }
    return named(output, &inputs.at(0).struct_());
  }
  if (relation.has_join() || relation.has_hash_join() || relation.has_merge_join() || relation.has_nested_loop_join()) {
    return join(relation, inputs);
  }
  if (relation.has_set()) return set(relation.set(), inputs);
  if (relation.has_cross()) return concatenate(inputs);
  if (relation.has_extension_leaf() || relation.has_extension_single() || relation.has_extension_multi()) return extension(relation);
  if (relation.has_filter() || relation.has_fetch() || relation.has_sort() || relation.has_exchange()) return inputs.at(0);
  throw std::invalid_argument("Relation has no supported canonical output schema");
}
}  // namespace pangrova::velox::schema
