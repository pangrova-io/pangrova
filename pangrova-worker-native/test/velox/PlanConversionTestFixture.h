#pragma once

// VeloxPlanConverterTest: Verifies Substrait → Velox PlanNode conversion
// for each core operator type.
//
// Tests build Substrait Plan protobuf programmatically, call convert(),
// and verify the returned PlanNode tree has the correct type and structure.
//
#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "CanonicalOutputAssertions.h"

#include "velox/plan/VeloxPlanConverter.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"

#include "capability/NativeCapabilityCatalog.h"
#include "../TestMemoryHelper.h"
#include "udf/JvmScalarFunction.h"
#include "StructuralContractIdentities.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"

#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"

#include <google/protobuf/wrappers.pb.h>

namespace pangrova::velox::test {
class VeloxPlanConverterTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::detail::ensureRuntimeInitialized();
  }
};

// ---------------------------------------------------------------------------
// Helper: build a minimal NamedStruct with given column names and types.
// ---------------------------------------------------------------------------
inline void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  field->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

inline void addStringColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  field->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

inline void expectLogicalOutputField(const VeloxPlanConverter& converter,
    const facebook::velox::RowTypePtr& physicalType, size_t ordinal, const std::string& label) {
  expectCanonicalOutputField(converter, physicalType, ordinal, label);
}

inline void expectFieldProjection(const std::shared_ptr<const facebook::velox::core::PlanNode>& node,
    const std::vector<size_t>& ordinals) {
  auto project = std::dynamic_pointer_cast<const facebook::velox::core::ProjectNode>(node);
  ASSERT_NE(project, nullptr);
  ASSERT_EQ(project->sources().size(), 1);
  ASSERT_EQ(project->projections().size(), ordinals.size());
  ASSERT_EQ(project->outputType()->size(), ordinals.size());
  const auto& input = project->sources().front()->outputType();
  for (size_t i = 0; i < ordinals.size(); ++i) {
    ASSERT_LT(ordinals[i], input->size());
    auto field = std::dynamic_pointer_cast<const facebook::velox::core::FieldAccessTypedExpr>(
        project->projections()[i]);
    ASSERT_NE(field, nullptr);
    EXPECT_TRUE(field->isInputColumn());
    EXPECT_EQ(field->name(), input->nameOf(ordinals[i]));
    EXPECT_TRUE(field->type()->equivalent(*input->childAt(ordinals[i])));
    EXPECT_TRUE(project->outputType()->childAt(i)->equivalent(*field->type()));
  }
}

// Use the production JVM UDF implementation and registry, not a test function.
// Conversion validates signatures; these component tests do not invoke Java.
inline void registerGovernedAiSignature(
    const std::string& name, const std::string& output_type) {
  pangrova::udf::NativeUdfRegistry registry;
  for (const auto& signature : pangrova::capability::generated::runtimeCallbackSignatures(name)) {
    EXPECT_EQ(std::string(signature.returnType), output_type);
    registry.registerScalar(pangrova::udf::makeJvmScalarFunctionSpec(name,
        std::vector<std::string>(signature.arguments.begin(), signature.arguments.end()),
        signature.returnType, signature.deterministic, signature.defaultNullBehavior));
  }
  registry.registerAllWithVelox();
}

// Build a simple ReadRel (TableScan) with a named table and schema.
inline substrait::ReadRel buildReadRel(
    const std::string& table_name,
    const std::vector<std::string>& col_names) {
  substrait::ReadRel read;
  auto* named = read.mutable_named_table();
  named->add_names(table_name);
  auto* schema = read.mutable_base_schema();
  for (const auto& col : col_names) {
    addI64Column(schema, col);
  }
  return read;
}

// Wrap a Rel into a Plan and serialize to bytes.
inline std::vector<uint8_t> serializePlan(const substrait::Rel& rel,
    const std::vector<std::string>& names = {"output"}) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  for (const auto& name : names) plan_rel->mutable_root()->add_names(name);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Helper: set a field reference expression pointing to field index.
inline void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Helper: set a field reference on a FieldReference message (for join keys).
inline void setJoinKeyRef(
    substrait::Expression::FieldReference* ref, int field_index) {
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

template <typename JoinRel>
void addStandardJoinKey(JoinRel* join, int left_index, int right_index) {
  auto* key = join->add_keys();
  setJoinKeyRef(key->mutable_left(), left_index);
  setJoinKeyRef(key->mutable_right(), right_index);
  key->mutable_comparison()->set_simple(
      substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ);
}

}  // namespace pangrova::velox::test
