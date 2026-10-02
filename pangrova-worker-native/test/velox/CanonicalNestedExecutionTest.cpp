// Canonical Nested expressions -> production converter -> actual Velox Task value oracle.
// This is not public SQL/JNI/provider end-to-end coverage.
#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

substrait::Expression field() {
  substrait::Expression expression;
  expression.mutable_selection()->mutable_root_reference();
  expression.mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(0);
  return expression;
}

substrait::Plan nestedRows() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("result");
  root->add_names("field_0");
  root->add_names("field_1");
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("x");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
      substrait::Type::NULLABILITY_NULLABLE);
  for (int64_t value : {7, 9}) {
    read->mutable_virtual_table()->add_expressions()->add_fields()->mutable_literal()->set_i64(value);
  }
  auto* list = project->add_expressions()->mutable_nested()->mutable_list();
  for (bool nullDecimal : {false, true}) {
    auto* row = list->add_values()->mutable_nested()->mutable_struct_();
    row->add_fields()->CopyFrom(field());
    auto* literal = row->add_fields()->mutable_literal();
    if (nullDecimal) {
      auto* decimal = literal->mutable_null()->mutable_decimal();
      decimal->set_precision(12);
      decimal->set_scale(2);
      decimal->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    } else {
      auto* decimal = literal->mutable_decimal();
      decimal->set_precision(12);
      decimal->set_scale(2);
      decimal->set_value(std::string(1, char{123}) + std::string(15, '\0'));
      literal->set_nullable(true);
    }
  }
  project->mutable_common()->mutable_emit()->add_output_mapping(1);
  return plan;
}

TEST(CanonicalNestedExecutionTest, DynamicRowArrayPreservesOrdinalValuesDecimalAndTypedNull) {
  auto pool = pangrova::test::testLeafPool("canonical-nested-values");
  ASSERT_NE(pool, nullptr);
  VeloxPlanConverter converter;
  const auto bytes = nestedRows().SerializeAsString();
  auto node = converter.convert({bytes.begin(), bytes.end()}, {});
  ASSERT_EQ(node->outputType()->size(), 1);
  const auto& rowType = node->outputType()->childAt(0)->asArray().elementType()->asRow();
  EXPECT_EQ(rowType.nameOf(0), "field_0");
  EXPECT_EQ(rowType.nameOf(1), "field_1");
  EXPECT_TRUE(rowType.childAt(1)->equivalent(*DECIMAL(12, 2)));
  auto task = exec::Task::create("canonical-nested-oracle", core::PlanFragment{node}, 0,
      core::QueryCtx::create(pangrova::jni::globalExecutor().get()), exec::Task::ExecutionMode::kSerial);
  ASSERT_TRUE(task->supportSerialExecutionMode());
  int rows = 0;
  while (auto batch = task->next()) {
    auto array = std::dynamic_pointer_cast<ArrayVector>(batch->childAt(0));
    ASSERT_NE(array, nullptr);
    auto elements = std::dynamic_pointer_cast<RowVector>(array->elements());
    ASSERT_NE(elements, nullptr);
    DecodedVector first(*elements->childAt(0));
    DecodedVector decimal(*elements->childAt(1));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      ASSERT_EQ(array->sizeAt(row), 2);
      const auto offset = array->offsetAt(row);
      EXPECT_EQ(first.valueAt<int64_t>(offset), rows == 0 ? 7 : 9);
      EXPECT_EQ(first.valueAt<int64_t>(offset + 1), rows == 0 ? 7 : 9);
      EXPECT_FALSE(decimal.isNullAt(offset));
      EXPECT_EQ(decimal.valueAt<int64_t>(offset), 123);
      EXPECT_TRUE(decimal.isNullAt(offset + 1));
      ++rows;
    }
  }
  EXPECT_EQ(rows, 2);
  EXPECT_FALSE(task->error()) << task->errorMessage();
  EXPECT_EQ(task->state(), exec::TaskState::kFinished);
  EXPECT_EQ(task->numRunningDrivers(), 0);
}

TEST(CanonicalNestedExecutionTest, HeterogeneousRuntimeElementsRequireAnExplicitCast) {
  auto pool = pangrova::test::testLeafPool("canonical-nested-rejection");
  ASSERT_NE(pool, nullptr);
  auto plan = nestedRows();
  auto* value = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0)->mutable_nested()->mutable_list()->mutable_values(1);
  value->Clear();
  value->mutable_literal()->set_i64(1);
  const auto bytes = plan.SerializeAsString();
  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert({bytes.begin(), bytes.end()}, {}), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox::test
