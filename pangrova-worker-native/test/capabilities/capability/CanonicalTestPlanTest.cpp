#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <utility>

#include "CanonicalTestPlan.h"
#include "TestMemoryHelper.h"
#include "engine/velox/CanonicalTaskExecution.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::capability::test {
namespace {
namespace proto = pangrova::protocol::v2;

substrait::Plan scalarPlan(const std::string& name) {
  auto plan = pangrova::test::canonicalTestPlan(substrait::Plan{});
  auto* declaration = plan.add_extensions()->mutable_extension_function();
  declaration->set_function_anchor(0);
  declaration->set_name(name);
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("value");
  auto* project = root->mutable_input()->mutable_project();
  project->mutable_input()->mutable_read()->mutable_base_schema()->mutable_struct_()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  project->mutable_input()->mutable_read()->mutable_virtual_table()->add_expressions();
  auto* call = project->add_expressions()->mutable_scalar_function();
  call->set_function_reference(0);
  call->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  call->add_arguments()->mutable_value()->mutable_literal()->set_i64(1);
  call->add_arguments()->mutable_value()->mutable_literal()->set_i64(2);
  return plan;
}

TEST(CanonicalTestPlanTest, DirectAliasUsesSoleDeclarationNotAnImplementationMap) {
  auto plan = pangrova::test::canonicalTestPlan(scalarPlan("plus:i64_i64"));
  const auto bound = bindPlanFunctionDeclarations(plan);
  ASSERT_EQ(bound.size(), 1);
  EXPECT_EQ(bound.at(0).ref().urn(), declaredPlanFunction(proto::FUNCTION_KIND_SCALAR, "add").ref().urn());
  EXPECT_EQ(bound.at(0).ref().name(), "add");
  EXPECT_EQ(bound.at(0).implementation_name(), "plus");
}

TEST(CanonicalTestPlanTest, SumUsesActualWindowInvocationKind) {
  auto plan = scalarPlan("sum:i64");
  auto* expression = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0);
  expression->Clear();
  expression->mutable_window_function()->set_function_reference(0);
  const auto bound = bindPlanFunctionDeclarations(pangrova::test::canonicalTestPlan(plan));
  ASSERT_EQ(bound.size(), 1);
  EXPECT_EQ(bound.at(0).ref().kind(), proto::FUNCTION_KIND_WINDOW);
  EXPECT_EQ(bound.at(0).ref().urn(), declaredPlanFunction(proto::FUNCTION_KIND_WINDOW, "sum").ref().urn());
}

TEST(CanonicalTestPlanTest, SolePredicateDagRetainsActualNullSafeExecution) {
  pangrova::test::detail::ensureRuntimeInitialized();
  auto plan = scalarPlan("is_not_distinct_from");
  auto* call = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0)->mutable_scalar_function();
  call->mutable_output_type()->Clear();
  call->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  for (auto& argument : *call->mutable_arguments()) {
    argument.mutable_value()->Clear();
    argument.mutable_value()->mutable_literal()->mutable_null()->mutable_i64()
        ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  }
  plan = pangrova::test::canonicalTestPlan(std::move(plan));
  const auto bound = bindPlanFunctionDeclarations(plan);
  ASSERT_EQ(bound.size(), 2);
  const auto bytes = plan.SerializeAsString();
  const auto registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  pangrova::velox::VeloxPlanConverter converter(registry);
  const auto node = converter.convert({bytes.begin(), bytes.end()}, {});
  const auto result = pangrova::velox::test::executeCanonicalTask(node, registry);
  ASSERT_EQ(result.batches.size(), 1);
  ASSERT_EQ(result.batches.front()->size(), 1);
  facebook::velox::DecodedVector value(*result.batches.front()->childAt(0));
  EXPECT_FALSE(value.isNullAt(0));
  EXPECT_TRUE(value.valueAt<bool>(0));
}

TEST(CanonicalTestPlanTest, ExplicitWrongAndDuplicateNamespacesAreNeverRepaired) {
  auto plan = scalarPlan("add");
  auto* urn = plan.add_extension_urns();
  urn->set_extension_urn_anchor(0);
  urn->set_urn("extension:unregistered:namespace");
  EXPECT_EQ(pangrova::test::canonicalTestPlan(plan).SerializeAsString(), plan.SerializeAsString());
  EXPECT_THROW(bindPlanFunctionDeclarations(pangrova::test::canonicalTestPlan(plan)), std::invalid_argument);
  *plan.add_extension_urns() = plan.extension_urns(0);
  EXPECT_THROW(bindPlanFunctionDeclarations(pangrova::test::canonicalTestPlan(plan)), std::invalid_argument);
}
}  // namespace
}  // namespace pangrova::capability::test
