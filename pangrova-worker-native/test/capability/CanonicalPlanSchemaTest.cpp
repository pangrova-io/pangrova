#include <gtest/gtest.h>
#include <stdexcept>

#include "velox/plan/CanonicalPlanSchema.h"

namespace pangrova::test {
namespace {
namespace schema = pangrova::velox::schema;
using Type = substrait::Type;

substrait::NamedStruct inputSchema() {
  Type decimal;
  decimal.mutable_decimal()->set_precision(38);
  decimal.mutable_decimal()->set_scale(18);
  decimal.mutable_decimal()->set_nullability(Type::NULLABILITY_REQUIRED);
  Type row;
  row.mutable_struct_()->set_nullability(Type::NULLABILITY_NULLABLE);
  *row.mutable_struct_()->add_types() = decimal;
  return schema::named({{"record", row, {"exact_amount"}},
      schema::computed("optional_flag", schema::primitive("bool", Type::NULLABILITY_NULLABLE))});
}

substrait::Expression field(int ordinal) {
  substrait::Expression expression;
  auto* reference = expression.mutable_selection();
  reference->mutable_root_reference();
  reference->mutable_direct_reference()->mutable_struct_field()->set_field(ordinal);
  return expression;
}
}  // namespace

TEST(CanonicalPlanSchemaTest, ReadPreservesExactNestedNamesPrecisionAndNullability) {
  substrait::Rel relation;
  *relation.mutable_read()->mutable_base_schema() = inputSchema();
  const auto result = schema::direct(relation, {});
  EXPECT_EQ(result.SerializeAsString(), inputSchema().SerializeAsString());
  EXPECT_EQ(result.struct_().types(0).struct_().types(0).decimal().precision(), 38);
  EXPECT_EQ(result.struct_().types(0).struct_().types(0).decimal().scale(), 18);
  EXPECT_EQ(result.names(1), "exact_amount");
}

TEST(CanonicalPlanSchemaTest, ProjectEmitPreservesRepeatedExactNestedSourceMetadata) {
  substrait::Rel relation;
  *relation.mutable_project()->add_expressions() = field(0);
  auto direct = schema::direct(relation, {inputSchema()});
  substrait::RelCommon common;
  common.mutable_emit()->add_output_mapping(2);
  common.mutable_emit()->add_output_mapping(0);
  auto output = schema::emit(direct, common);
  EXPECT_EQ(output.struct_().types_size(), 2);
  EXPECT_EQ(output.struct_().types(0).SerializeAsString(), inputSchema().struct_().types(0).SerializeAsString());
  EXPECT_EQ(output.struct_().types(1).SerializeAsString(), output.struct_().types(0).SerializeAsString());
  EXPECT_EQ(output.names_size(), 4);
  EXPECT_EQ(output.names(1), "exact_amount");
  EXPECT_EQ(output.names(3), "exact_amount");
}

TEST(CanonicalPlanSchemaTest, DeclaredScalarOutputDoesNotInferNullabilityFromItsArguments) {
  substrait::Expression expression;
  auto* call = expression.mutable_scalar_function();
  *call->mutable_output_type() = schema::primitive("bool", Type::NULLABILITY_REQUIRED);
  *call->add_arguments()->mutable_value() = field(1);
  const auto result = schema::expression(expression, inputSchema());
  EXPECT_EQ(result.bool_().nullability(), Type::NULLABILITY_REQUIRED);
  call->mutable_output_type()->mutable_bool_()->set_nullability(Type::NULLABILITY_NULLABLE);
  EXPECT_EQ(schema::expression(expression, inputSchema()).bool_().nullability(), Type::NULLABILITY_NULLABLE);
}

TEST(CanonicalPlanSchemaTest, NullableParentPropagatesOnlyToSelectedValueNotItsStoredChildDeclaration) {
  auto selected = field(0);
  selected.mutable_selection()->mutable_direct_reference()->mutable_struct_field()
      ->mutable_child()->mutable_struct_field()->set_field(0);
  auto type = schema::expression(selected, inputSchema());
  EXPECT_EQ(type.decimal().precision(), 38);
  EXPECT_EQ(type.decimal().nullability(), Type::NULLABILITY_NULLABLE);
  EXPECT_EQ(inputSchema().struct_().types(0).struct_().types(0).decimal().nullability(), Type::NULLABILITY_REQUIRED);
}

TEST(CanonicalPlanSchemaTest, AggregatesRetainDeclaredEmptyInputCountAndSumContracts) {
  substrait::Rel relation;
  relation.mutable_aggregate()->add_groupings();
  *relation.mutable_aggregate()->add_measures()->mutable_measure()->mutable_output_type() =
      schema::primitive("i64", Type::NULLABILITY_REQUIRED);
  *relation.mutable_aggregate()->add_measures()->mutable_measure()->mutable_output_type() =
      schema::primitive("i64", Type::NULLABILITY_NULLABLE);
  const auto result = schema::direct(relation, {inputSchema()});
  EXPECT_EQ(result.names(0), "measure_0");
  EXPECT_EQ(result.struct_().types(0).i64().nullability(), Type::NULLABILITY_REQUIRED);
  EXPECT_EQ(result.struct_().types(1).i64().nullability(), Type::NULLABILITY_NULLABLE);
}

TEST(CanonicalPlanSchemaTest, OuterJoinPadsParentWhilePreservingExactNestedChildren) {
  substrait::Rel relation;
  relation.mutable_join()->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);
  const auto result = schema::direct(relation, {inputSchema(), schema::named({
      schema::computed("required_right", schema::primitive("i64", Type::NULLABILITY_REQUIRED))})});
  EXPECT_EQ(result.struct_().types(2).i64().nullability(), Type::NULLABILITY_NULLABLE);
  EXPECT_EQ(result.struct_().types(0).SerializeAsString(), inputSchema().struct_().types(0).SerializeAsString());
}

TEST(CanonicalPlanSchemaTest, CaseMergesOnlyCanonicalBranchNullabilityAndRejectsDifferentParameters) {
  substrait::Expression expression;
  auto* conditional = expression.mutable_if_then();
  *conditional->add_ifs()->mutable_if_() = field(1);
  conditional->mutable_ifs(0)->mutable_then()->mutable_literal()->set_i64(1);
  *conditional->mutable_else_()->mutable_literal()->mutable_null() =
      schema::primitive("i64", Type::NULLABILITY_NULLABLE);
  EXPECT_EQ(schema::expression(expression, inputSchema()).i64().nullability(), Type::NULLABILITY_NULLABLE);
  conditional->mutable_else_()->mutable_literal()->mutable_null()->Clear();
  *conditional->mutable_else_()->mutable_literal()->mutable_null() = schema::primitive("i32", Type::NULLABILITY_NULLABLE);
  EXPECT_THROW(schema::expression(expression, inputSchema()), std::invalid_argument);
}

TEST(CanonicalPlanSchemaTest, RequiredAndUnspecifiedLeafMetadataAreNotSilentlyRepaired) {
  auto input = inputSchema();
  input.mutable_struct_()->mutable_types(1)->mutable_bool_()->set_nullability(Type::NULLABILITY_UNSPECIFIED);
  const auto original = input.SerializeAsString();
  EXPECT_THROW(schema::expression(field(1), input), std::invalid_argument);
  EXPECT_THROW(schema::fields(input), std::invalid_argument);
  EXPECT_EQ(input.SerializeAsString(), original);
}

TEST(CanonicalPlanSchemaTest, UnspecifiedComplexParentAndChildrenCannotBeRepairedByMerge) {
  auto row = inputSchema().struct_().types(0);
  auto invalid = row;
  invalid.mutable_struct_()->set_nullability(Type::NULLABILITY_UNSPECIFIED);
  EXPECT_THROW(schema::merge({invalid, row}), std::invalid_argument);
  EXPECT_THROW(schema::computed("row", invalid), std::invalid_argument);
  invalid = row;
  invalid.mutable_struct_()->mutable_types(0)->mutable_decimal()->set_nullability(Type::NULLABILITY_UNSPECIFIED);
  EXPECT_THROW(schema::merge({row, invalid}), std::invalid_argument);
  Type list;
  list.mutable_list()->set_nullability(Type::NULLABILITY_REQUIRED);
  *list.mutable_list()->mutable_type() = invalid;
  EXPECT_THROW(schema::computed("list", list), std::invalid_argument);
  Type map;
  map.mutable_map()->set_nullability(Type::NULLABILITY_REQUIRED);
  *map.mutable_map()->mutable_key() = schema::primitive("i64", Type::NULLABILITY_REQUIRED);
  *map.mutable_map()->mutable_value() = invalid;
  EXPECT_THROW(schema::computed("map", map), std::invalid_argument);
}

TEST(CanonicalPlanSchemaTest, MissingDeclaredCallAndCaseContractsFailBeforeNullabilityPropagation) {
  substrait::Expression call;
  call.mutable_scalar_function()->mutable_output_type()->mutable_i64();
  EXPECT_THROW(schema::expression(call, inputSchema()), std::invalid_argument);
  substrait::Expression conditional;
  auto* clause = conditional.mutable_if_then()->add_ifs();
  clause->mutable_if_()->mutable_literal()->set_boolean(true);
  *clause->mutable_then() = call;
  conditional.mutable_if_then()->mutable_else_()->mutable_literal()->set_i64(7);
  EXPECT_THROW(schema::expression(conditional, inputSchema()), std::invalid_argument);
  auto container = inputSchema();
  container.mutable_struct_()->set_nullability(Type::NULLABILITY_UNSPECIFIED);
  EXPECT_NO_THROW(schema::fields(container));
  EXPECT_EQ(schema::expression(field(1), container).bool_().nullability(), Type::NULLABILITY_NULLABLE);
}

TEST(CanonicalPlanSchemaTest, EmitAndDfsErrorsFailWithoutChangingTheSource) {
  const auto input = inputSchema();
  substrait::RelCommon common;
  common.mutable_emit()->add_output_mapping(2);
  EXPECT_THROW(schema::emit(input, common), std::invalid_argument);
  auto malformed = input;
  malformed.mutable_names()->RemoveLast();
  EXPECT_THROW(schema::fields(malformed), std::exception);
  EXPECT_EQ(input.SerializeAsString(), inputSchema().SerializeAsString());
}
}  // namespace pangrova::test
