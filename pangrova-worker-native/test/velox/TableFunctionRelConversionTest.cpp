// TableFunctionRelConversionTest: Parameterized tests for TableFunctionRel
// plan conversion verification.
//
// Property 12: TableFunctionRel Plan Conversion
//
// TEST_P over function names × argument counts (1, 2, 3) × output schema
// widths (1, 2, 4).
// Verifies:
//   - convertExtensionMultiRel creates a TableFunctionNode with matching
//     function name, arguments, and output schema.
//   - Unregistered function names throw std::runtime_error.
//
// **Validates: Requirements 5.1**

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include <google/protobuf/any.pb.h>

namespace pangrova::velox::test {
namespace {

// ===========================================================================
// Test parameters: function name × argument count × output schema width
// ===========================================================================

struct TableFunctionRelParam {
  std::string name;
  std::string function_name;
  int argument_count;
  int output_schema_width;
};

// ===========================================================================
// Helpers
// ===========================================================================

void addI64Column(substrait::NamedStruct* schema, const std::string& col_name) {
  schema->add_names(col_name);
  schema->mutable_struct_()->add_types()->mutable_i64();
}

// Build a Substrait Plan containing an ExtensionMultiRel encoding a
// TableFunctionDetail with the given parameters. No input (standalone).
std::vector<uint8_t> buildTableFunctionPlan(
    const std::string& function_name,
    int argument_count,
    int output_schema_width) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  // Add output names matching the output schema width.
  for (int i = 0; i < output_schema_width; ++i) {
    root->add_names("out_" + std::to_string(i));
  }

  auto* ext_multi = root->mutable_input()->mutable_extension_multi();

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_table_function();
  detail->set_plan_node_id("table-function-0");
  detail->mutable_invocation()->mutable_capability()->set_capability_id(
      function_name);

  // Add literal integer arguments.
  for (int i = 0; i < argument_count; ++i) {
    substrait::Expression arg;
    arg.mutable_literal()->set_i64(i + 1);
    auto* named = detail->mutable_invocation()->add_arguments();
    named->set_name("arg_" + std::to_string(i));
    named->mutable_expression()->CopyFrom(arg);
  }

  // Build output schema.
  substrait::NamedStruct output_schema;
  for (int i = 0; i < output_schema_width; ++i) {
    addI64Column(&output_schema, "out_" + std::to_string(i));
  }
  detail->mutable_output_schema()->CopyFrom(output_schema);

  // Pack detail into the ExtensionMultiRel's detail field.
  ext_multi->mutable_detail()->PackFrom(envelope);

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Build a Substrait Plan containing an ExtensionMultiRel with a lateral join
// pattern (one input source).
std::vector<uint8_t> buildTableFunctionWithInputPlan(
    const std::string& function_name,
    int argument_count,
    int output_schema_width,
    int input_schema_width) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  // Output names = input columns + function output columns.
  for (int i = 0; i < input_schema_width + output_schema_width; ++i) {
    root->add_names("col_" + std::to_string(i));
  }

  auto* ext_multi = root->mutable_input()->mutable_extension_multi();

  // Add input: a ReadRel (TableScan).
  auto* input_read = ext_multi->add_inputs()->mutable_read();
  input_read->mutable_named_table()->add_names("input_table");
  auto* input_schema = input_read->mutable_base_schema();
  for (int i = 0; i < input_schema_width; ++i) {
    addI64Column(input_schema, "in_" + std::to_string(i));
  }

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_table_function();
  detail->set_plan_node_id("table-function-0");
  detail->mutable_invocation()->mutable_capability()->set_capability_id(
      function_name);

  // Add field reference arguments pointing to input columns.
  for (int i = 0; i < argument_count; ++i) {
    substrait::Expression arg;
    auto* ref = arg.mutable_selection();
    ref->mutable_root_reference();
    ref->mutable_direct_reference()->mutable_struct_field()->set_field(
        i % input_schema_width);
    auto* named = detail->mutable_invocation()->add_arguments();
    named->set_name("arg_" + std::to_string(i));
    named->mutable_expression()->CopyFrom(arg);
  }

  // The canonical schema is complete, including the exact input prefix.
  substrait::NamedStruct output_schema;
  for (int i = 0; i < input_schema_width; ++i) {
    addI64Column(&output_schema, "in_" + std::to_string(i));
  }
  for (int i = 0; i < output_schema_width; ++i) {
    addI64Column(&output_schema, "func_out_" + std::to_string(i));
  }
  detail->mutable_output_schema()->CopyFrom(output_schema);

  ext_multi->mutable_detail()->PackFrom(envelope);

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// ===========================================================================
// Test fixture
// ===========================================================================

class TableFunctionRelConversionTest
    : public ::testing::TestWithParam<TableFunctionRelParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 12.1: Standalone TableFunctionNode Creation
//
// For any registered function name, convertExtensionMultiRel SHALL create a
// TableFunctionNode with matching function name, argument count, and output
// schema width.
//
// **Validates: Requirements 5.1**
// ===========================================================================

TEST_P(TableFunctionRelConversionTest, StandaloneTableFunctionNodeCreated) {
  const auto& param = GetParam();
  auto plan_bytes = buildTableFunctionPlan(
      param.function_name, param.argument_count, param.output_schema_width);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr)
      << "Plan conversion failed for " << param.name;
  EXPECT_EQ(result->name(), "TableFunction")
      << "Expected TableFunction node for " << param.name;

  // Downcast to TableFunctionNode to verify properties.
  auto tf_node = std::dynamic_pointer_cast<const TableFunctionNode>(result);
  ASSERT_NE(tf_node, nullptr)
      << "Failed to cast to TableFunctionNode for " << param.name;

  // Verify function name matches.
  EXPECT_EQ(tf_node->functionName(), param.function_name)
      << "Function name mismatch for " << param.name;

  // Verify argument count matches.
  EXPECT_EQ(
      static_cast<int>(tf_node->arguments().size()), param.argument_count)
      << "Argument count mismatch for " << param.name;

  // Verify output schema width matches.
  auto output_type = tf_node->outputType();
  ASSERT_NE(output_type, nullptr);
  EXPECT_EQ(static_cast<int>(output_type->size()), param.output_schema_width)
      << "Output schema width mismatch for " << param.name;

  // Verify output column names.
  for (int i = 0; i < param.output_schema_width; ++i) {
    expectCanonicalOutputField(converter, output_type, i, "out_" + std::to_string(i));
  }

  // Standalone: no input sources.
  EXPECT_TRUE(tf_node->sources().empty())
      << "Standalone TableFunctionNode should have no sources for "
      << param.name;
}

// ===========================================================================
// Property 12.2: Lateral Join TableFunctionNode Creation
//
// When an input source is present (lateral join pattern), the
// TableFunctionNode SHALL have the input as its source and the output schema
// SHALL be the combined input + function output columns.
//
// **Validates: Requirements 5.1**
// ===========================================================================

TEST_P(TableFunctionRelConversionTest, LateralJoinTableFunctionNodeCreated) {
  const auto& param = GetParam();
  const int input_schema_width = 2;  // Fixed input width for lateral tests.

  auto plan_bytes = buildTableFunctionWithInputPlan(
      param.function_name, param.argument_count, param.output_schema_width,
      input_schema_width);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr)
      << "Plan conversion failed for lateral " << param.name;

  // Root.names may cause VeloxPlanConverter to wrap the plan with a
  // ProjectNode for column renaming. Unwrap it to find the TableFunctionNode.
  auto actual_node = result;
  if (actual_node->name() == "Project" && !actual_node->sources().empty()) {
    actual_node = actual_node->sources()[0];
  }
  EXPECT_EQ(actual_node->name(), "TableFunction")
      << "Expected TableFunction node for lateral " << param.name;

  auto tf_node = std::dynamic_pointer_cast<const TableFunctionNode>(actual_node);
  ASSERT_NE(tf_node, nullptr)
      << "Failed to cast to TableFunctionNode for lateral " << param.name;

  // Verify function name matches.
  EXPECT_EQ(tf_node->functionName(), param.function_name)
      << "Function name mismatch for lateral " << param.name;

  // Verify argument count matches.
  EXPECT_EQ(
      static_cast<int>(tf_node->arguments().size()), param.argument_count)
      << "Argument count mismatch for lateral " << param.name;

  // Verify combined output schema: input_cols + function_output_cols.
  auto output_type = tf_node->outputType();
  ASSERT_NE(output_type, nullptr);
  int expected_total_cols = input_schema_width + param.output_schema_width;
  EXPECT_EQ(static_cast<int>(output_type->size()), expected_total_cols)
      << "Combined output schema width mismatch for lateral " << param.name;

  // Verify input column names come first.
  for (int i = 0; i < input_schema_width; ++i) {
    expectCanonicalOutputField(converter, output_type, i, "col_" + std::to_string(i));
    EXPECT_EQ(tf_node->sources()[0]->outputType()->nameOf(i), physicalFieldAlias(i));
  }

  // Verify function output column names follow.
  for (int i = 0; i < param.output_schema_width; ++i) {
    expectCanonicalOutputField(converter, output_type, input_schema_width + i,
        "col_" + std::to_string(input_schema_width + i));
  }

  // Lateral join: one input source (TableScan).
  ASSERT_EQ(tf_node->sources().size(), 1)
      << "Lateral TableFunctionNode should have one source for "
      << param.name;
  EXPECT_EQ(tf_node->sources()[0]->name(), "TableScan");
  const auto declaredInput = converter.getScanNodeOutputType(tf_node->sources()[0]->id());
  ASSERT_TRUE(declaredInput.has_value());
  for (int i = 0; i < input_schema_width; ++i) {
    EXPECT_EQ(declaredInput.value()->nameOf(i), "in_" + std::to_string(i));
  }
}

TEST(TableFunctionOutputContractTest, RejectsMissingOrChangedInputPrefix) {
  pangrova::test::testMemoryManager();
  for (int mutation = 0; mutation < 3; ++mutation) {
    SCOPED_TRACE(mutation);
    const auto bytes = buildTableFunctionWithInputPlan("generate_series", 2, 1, 2);
    substrait::Plan plan;
    ASSERT_TRUE(plan.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
    auto* extension = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_extension_multi();
    pangrova::protocol::v2::RelationDetailEnvelope envelope;
    ASSERT_TRUE(extension->detail().UnpackTo(&envelope));
    auto* schema = envelope.mutable_table_function()->mutable_output_schema();
    if (mutation == 0) schema->set_names(0, "wrong_input_label");
    if (mutation == 1) schema->mutable_struct_()->mutable_types(0)->mutable_string();
    if (mutation == 2) {
      schema->mutable_names()->DeleteSubrange(0, 2);
      schema->mutable_struct_()->mutable_types()->DeleteSubrange(0, 2);
    }
    extension->mutable_detail()->PackFrom(envelope);
    const auto mutated = plan.SerializeAsString();
    VeloxPlanConverter converter;
    try {
      converter.convert({mutated.begin(), mutated.end()}, {});
      FAIL() << "Invalid table-function prefix must fail before execution";
    } catch (const std::runtime_error& error) {
      EXPECT_NE(std::string(error.what()).find("output"), std::string::npos);
      EXPECT_NE(std::string(error.what()).find("prefix"), std::string::npos);
    }
  }
}

// ===========================================================================
// Property 12.3: Unregistered Function Name Throws
//
// For any unregistered function name, convertExtensionMultiRel SHALL throw
// std::runtime_error with a descriptive message.
//
// **Validates: Requirements 5.5**
// ===========================================================================

TEST_P(TableFunctionRelConversionTest, UnregisteredFunctionThrows) {
  const auto& param = GetParam();
  const std::string unregistered_name =
      "unregistered_" + param.function_name + "_xyz";

  auto plan_bytes = buildTableFunctionPlan(
      unregistered_name, param.argument_count, param.output_schema_width);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  try {
    converter.convert(plan_bytes, {});
    FAIL() << "Expected std::runtime_error for unregistered function: "
           << unregistered_name;
  } catch (const std::runtime_error& e) {
    std::string msg = e.what();
    EXPECT_TRUE(msg.find(unregistered_name) != std::string::npos)
        << "Error message should contain the function name '"
        << unregistered_name << "': " << msg;
  }
}

// ===========================================================================
// Parameterization: function names × argument counts × output schema widths
// ===========================================================================

INSTANTIATE_TEST_SUITE_P(
    TableFunctionRelConversion,
    TableFunctionRelConversionTest,
    ::testing::Values(
        // generate_series × argument counts × output widths
        TableFunctionRelParam{
            "GenSeries_1arg_1out", "generate_series", 1, 1},
        TableFunctionRelParam{
            "GenSeries_2arg_1out", "generate_series", 2, 1},
        TableFunctionRelParam{
            "GenSeries_3arg_1out", "generate_series", 3, 1},
        TableFunctionRelParam{
            "GenSeries_1arg_2out", "generate_series", 1, 2},
        TableFunctionRelParam{
            "GenSeries_2arg_2out", "generate_series", 2, 2},
        TableFunctionRelParam{
            "GenSeries_3arg_2out", "generate_series", 3, 2},
        TableFunctionRelParam{
            "GenSeries_1arg_4out", "generate_series", 1, 4},
        TableFunctionRelParam{
            "GenSeries_2arg_4out", "generate_series", 2, 4},
        TableFunctionRelParam{
            "GenSeries_3arg_4out", "generate_series", 3, 4}),
    [](const ::testing::TestParamInfo<TableFunctionRelParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
