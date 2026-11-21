// TableFunctionRelConversionTest: Parameterized tests for TableFunctionRel
// plan conversion verification.
//
// Property 12: TableFunctionRel Plan Conversion
//
// TEST_P over the declared 2/3-argument source and rejected lateral forms.
// Verifies:
//   - canonical extension relations create a TableFunctionNode with matching
//     function name, arguments, and output schema.
//   - Unregistered function names throw std::runtime_error.
//
// **Validates: Requirements 5.1**

#include <gtest/gtest.h>
#include "StructuralContractIdentities.h"
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "engine/velox/plan/TableFunctionNode.h"
#include "engine/velox/plan/VeloxPlanConverter.h"

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

constexpr auto& kSeries = pangrova::capability::generated::pangrova_table_generate_series;

void setCapability(pangrova::protocol::v2::CapabilityRef* reference,
                   const std::string& function_name) {
  reference->set_capability_id(function_name);
  if (function_name == kSeries.id) {
    reference->set_semantic_version(kSeries.version);
    reference->set_signature_sha256(std::string(
        reinterpret_cast<const char*>(kSeries.signature.data()),
        kSeries.signature.size()));
  }
}

// ===========================================================================
// Helpers
// ===========================================================================

void addI64Column(substrait::NamedStruct* schema, const std::string& col_name) {
  schema->add_names(col_name);
  schema->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

// Build a Substrait Plan containing an ExtensionLeafRel encoding a
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
    root->add_names(i == 0 ? "val" : "extra_" + std::to_string(i));
  }

  auto* ext_leaf = root->mutable_input()->mutable_extension_leaf();

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_table_function();
  detail->set_plan_node_id("table-function-0");
  setCapability(detail->mutable_invocation()->mutable_capability(), function_name);

  // Add literal integer arguments.
  for (int i = 0; i < argument_count; ++i) {
    substrait::Expression arg;
    arg.mutable_literal()->set_i64(i + 1);
    auto* named = detail->mutable_invocation()->add_arguments();
    named->set_name(i == 0 ? "start" : i == 1 ? "stop" : "step");
    named->mutable_expression()->CopyFrom(arg);
  }

  // Build output schema.
  substrait::NamedStruct output_schema;
  for (int i = 0; i < output_schema_width; ++i) {
    addI64Column(&output_schema, i == 0 ? "val" : "extra_" + std::to_string(i));
  }
  detail->mutable_output_schema()->CopyFrom(output_schema);

  // A source relation has no inputs in Substrait.
  ext_leaf->mutable_detail()->PackFrom(envelope);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Build a Substrait Plan containing an ExtensionSingleRel with a lateral join
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

  auto* ext_single = root->mutable_input()->mutable_extension_single();

  // Add input: a ReadRel (TableScan).
  auto* input_read = ext_single->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("input_table");
  auto* input_schema = input_read->mutable_base_schema();
  for (int i = 0; i < input_schema_width; ++i) {
    addI64Column(input_schema, "in_" + std::to_string(i));
  }

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_table_function();
  detail->set_plan_node_id("table-function-0");
  setCapability(detail->mutable_invocation()->mutable_capability(), function_name);

  // Add field reference arguments pointing to input columns.
  for (int i = 0; i < argument_count; ++i) {
    substrait::Expression arg;
    auto* ref = arg.mutable_selection();
    ref->mutable_root_reference();
    ref->mutable_direct_reference()->mutable_struct_field()->set_field(
        i % input_schema_width);
    auto* named = detail->mutable_invocation()->add_arguments();
    named->set_name(i == 0 ? "start" : i == 1 ? "stop" : "step");
    named->mutable_expression()->CopyFrom(arg);
  }

  // The canonical schema is complete, including the exact input prefix.
  substrait::NamedStruct output_schema;
  for (int i = 0; i < input_schema_width; ++i) {
    addI64Column(&output_schema, "in_" + std::to_string(i));
  }
  for (int i = 0; i < output_schema_width; ++i) {
    addI64Column(&output_schema, i == 0 ? "val" : "extra_" + std::to_string(i));
  }
  detail->mutable_output_schema()->CopyFrom(output_schema);

  ext_single->mutable_detail()->PackFrom(envelope);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
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
// For the declared GENERATE_SERIES capability, conversion SHALL create a
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
    expectCanonicalOutputField(converter, output_type, i, i == 0 ? "val" : "extra_" + std::to_string(i));
  }

  // Standalone: no input sources.
  EXPECT_TRUE(tf_node->sources().empty())
      << "Standalone TableFunctionNode should have no sources for "
      << param.name;
}

// ===========================================================================
// Property 12.2: Undeclared Lateral GENERATE_SERIES Is Rejected
//
// The SQL planner only emits a zero-input source. ExtensionSingleRel must not
// silently turn a field-dependent invocation into a production capability.
// ===========================================================================

TEST_P(TableFunctionRelConversionTest, LateralSeriesIsRejected) {
  const auto& param = GetParam();
  const auto bytes = buildTableFunctionWithInputPlan(
      param.function_name, param.argument_count, param.output_schema_width, 2);
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(bytes, {}), std::runtime_error);
}

TEST(TableFunctionOutputContractTest, RejectsUndeclaredIdentityAndMalformedSeriesShape) {
  pangrova::test::testMemoryManager();
  for (int mutation = 0; mutation < 4; ++mutation) {
    SCOPED_TRACE(mutation);
    const auto bytes = buildTableFunctionPlan(kSeries.id, 2, 1);
    substrait::Plan plan;
    ASSERT_TRUE(plan.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
    auto* extension = plan.mutable_relations(0)->mutable_root()->mutable_input()
                          ->mutable_extension_leaf();
    pangrova::protocol::v2::RelationDetailEnvelope envelope;
    ASSERT_TRUE(extension->detail().UnpackTo(&envelope));
    auto* detail = envelope.mutable_table_function();
    if (mutation == 0) {
      detail->mutable_invocation()->mutable_capability()->set_signature_sha256("invalid");
    } else if (mutation == 1) {
      detail->mutable_invocation()->mutable_capability()->set_capability_id("generate_series");
    } else if (mutation == 2) {
      detail->mutable_invocation()->mutable_arguments()->RemoveLast();
    } else {
      addI64Column(detail->mutable_output_schema(), "extra");
    }
    extension->mutable_detail()->PackFrom(envelope);
    const auto mutated = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    EXPECT_THROW(converter.convert({mutated.begin(), mutated.end()}, {}),
                 std::runtime_error);
  }
}

// ===========================================================================
// Property 12.3: Undeclared Capability Name Throws
//
// For an undeclared capability name, conversion SHALL throw
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
// Parameterization: declared argument counts
// ===========================================================================

INSTANTIATE_TEST_SUITE_P(
    TableFunctionRelConversion,
    TableFunctionRelConversionTest,
    ::testing::Values(
        TableFunctionRelParam{"GenSeries_2arg_1out", kSeries.id, 2, 1},
        TableFunctionRelParam{"GenSeries_3arg_1out", kSeries.id, 3, 1}),
    [](const ::testing::TestParamInfo<TableFunctionRelParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
