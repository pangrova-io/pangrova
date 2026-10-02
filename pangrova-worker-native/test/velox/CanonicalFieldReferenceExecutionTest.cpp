// Actual converter/Task reference-root oracles, not public SQL/JNI E2E.
#include <gtest/gtest.h>

#include "CanonicalTaskExecution.h"
#include "substrait/plan.pb.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

substrait::Plan referencePlan() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("selected");
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->add_names("record");
  schema->add_names("part");
  schema->mutable_struct_()->add_types()->mutable_struct_()->add_types()->mutable_i64();
  read->mutable_virtual_table()->add_expressions()->add_fields()
      ->mutable_literal()->mutable_struct_()->add_fields()->set_i64(17);
  auto* reference = project->add_expressions()->mutable_selection();
  reference->mutable_root_reference();
  auto* segment = reference->mutable_direct_reference()->mutable_struct_field();
  segment->set_field(0);
  segment->mutable_child()->mutable_struct_field()->set_field(0);
  project->mutable_common()->mutable_emit()->add_output_mapping(1);
  return plan;
}

core::PlanNodePtr convertReferencePlan(const substrait::Plan& plan) {
  const auto bytes = plan.SerializeAsString();
  VeloxPlanConverter converter;
  return converter.convert({bytes.begin(), bytes.end()}, {});
}

TEST(CanonicalFieldReferenceExecutionTest, ExplicitRootWalksTheCompleteNestedPath) {
  pangrova::test::testMemoryManager();
  auto output = executeCanonicalTask(convertReferencePlan(referencePlan()));
  ASSERT_EQ(output.batches.size(), 1);
  ASSERT_EQ(output.batches.front()->size(), 1);
  ASSERT_EQ(output.batches.front()->childrenSize(), 1);
  DecodedVector selected(*output.batches.front()->childAt(0));
  EXPECT_FALSE(selected.isNullAt(0));
  EXPECT_EQ(selected.valueAt<int64_t>(0), 17);
}

TEST(CanonicalFieldReferenceExecutionTest, ExpressionRootIsNotReboundToTheInputRow) {
  pangrova::test::testMemoryManager();
  auto plan = referencePlan();
  auto* reference = plan.mutable_relations(0)->mutable_root()->mutable_input()
      ->mutable_project()->mutable_expressions(0)->mutable_selection();
  reference->mutable_expression()->mutable_literal()->mutable_struct_()
      ->add_fields()->mutable_struct_()->add_fields()->set_i64(91);
  auto output = executeCanonicalTask(convertReferencePlan(plan));
  ASSERT_EQ(output.batches.size(), 1);
  DecodedVector selected(*output.batches.front()->childAt(0));
  EXPECT_EQ(selected.valueAt<int64_t>(0), 91);
}

TEST(CanonicalFieldReferenceExecutionTest, MissingOuterAndLambdaRootsCannotImplicitlyReadInput) {
  pangrova::test::testMemoryManager();
  for (int mutation = 0; mutation < 3; ++mutation) {
    SCOPED_TRACE(mutation);
    auto plan = referencePlan();
    auto* reference = plan.mutable_relations(0)->mutable_root()->mutable_input()
        ->mutable_project()->mutable_expressions(0)->mutable_selection();
    reference->clear_root_type();
    if (mutation == 1) reference->mutable_outer_reference()->set_steps_out(1);
    if (mutation == 2) reference->mutable_lambda_parameter_reference()->set_steps_out(0);
    try {
      convertReferencePlan(plan);
      FAIL() << "Unsupported root must not read the incoming record";
    } catch (const std::runtime_error& error) {
      EXPECT_NE(std::string(error.what()).find("explicit supported root"), std::string::npos);
    }
  }
}

TEST(CanonicalFieldReferenceExecutionTest, RootMustStartWithAStructField) {
  pangrova::test::testMemoryManager();
  auto plan = referencePlan();
  auto* reference = plan.mutable_relations(0)->mutable_root()->mutable_input()
      ->mutable_project()->mutable_expressions(0)->mutable_selection();
  reference->mutable_direct_reference()->mutable_list_element()->set_offset(0);
  try {
    convertReferencePlan(plan);
    FAIL() << "RootReference with a ListElement is malformed";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("requires a struct_field"), std::string::npos);
  }
}
}  // namespace
}  // namespace pangrova::velox::test
