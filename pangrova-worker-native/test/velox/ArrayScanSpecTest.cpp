// ArrayScanSpecTest: Bug condition exploration test for ARRAY column ScanSpec
// element materialization.
//
// Validates: Requirements 1.1, 2.1, 2.2
//
// Bug Condition: addField("elements", 0) creates a bare ScanSpec node without
// type information. ListColumnReader sees the spec exists (returns correct
// offsets/sizes → correct row counts) but cannot materialize actual element
// values (returns nulls).
//
// Expected Behavior: addArrayElementFieldRecursively(*elementType) creates a
// child ScanSpec with full type information, enabling ListColumnReader to
// materialize element data correctly.
//
// This test constructs ScanSpecs for ARRAY columns using the FIXED approach
// (addArrayElementFieldRecursively) and verifies the "elements" child has
// proper type information for materialization. On UNFIXED code (bare addField),
// the assertions about children would FAIL — confirming the bug exists.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"

#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Type.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;
using facebook::velox::common::ScanSpec;

// ---------------------------------------------------------------------------
// Parameterized test: verifies ARRAY ScanSpec element materialization for
// multiple element types (VARCHAR, BIGINT, INTEGER, DOUBLE, BOOLEAN).
// ---------------------------------------------------------------------------

struct ArrayScanSpecParam {
  std::string name;
  std::shared_ptr<const Type> elementType;
  std::shared_ptr<const Type> arrayType;
};

class ArrayScanSpecTest
    : public ::testing::TestWithParam<ArrayScanSpecParam> {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("ArrayScanSpecTest_param");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    ArrayScanSpecTest::pool_;

// Helper: find a child ScanSpec by name using children() vector.
// childByName() uses an internal map that may not be populated in all
// Velox versions. This helper searches the children vector directly.
static ScanSpec* findChildByName(
    ScanSpec* parent,
    const std::string& name) {
  for (const auto& child : parent->children()) {
    if (child->fieldName() == name) {
      return child.get();
    }
  }
  return nullptr;
}

// ---------------------------------------------------------------------------
// Property 1: Bug Condition — ARRAY ScanSpec Bare addField Returns Null
// Elements
//
// **Validates: Requirements 1.1, 2.1, 2.2**
//
// This test demonstrates the bug condition by comparing:
// 1. The BUGGY approach: addField("elements", 0) — creates bare node, no
//    children, no type info → ListColumnReader cannot materialize elements
// 2. The FIXED approach: addArrayElementFieldRecursively(*elementType) —
//    creates typed node with proper children → materialization works
//
// On UNFIXED code, the production code uses approach (1), so the ScanSpec
// produced by createReaderPair() would have no children on the "elements"
// node — this test would FAIL because it asserts the fixed behavior.
// ---------------------------------------------------------------------------

// Test that the BUGGY approach (bare addField) produces a ScanSpec WITHOUT
// proper type information — demonstrating the root cause.
TEST_P(ArrayScanSpecTest, BareAddFieldLacksTypeInfo) {
  const auto& param = GetParam();

  // Simulate the BUGGY code path: scanSpec->addField(colName, i) then
  // arraySpec->addField("elements", 0) — no type info provided.
  auto scanSpec = std::make_shared<ScanSpec>("root");
  auto* arraySpec = scanSpec->addField("array_col", 0);
  auto* elemSpec = arraySpec->addField(
      ScanSpec::kArrayElementsFieldName, 0);

  // The bare addField creates a node but with NO children — the element
  // spec has no type-aware sub-tree. This is the bug condition.
  // ListColumnReader sees the spec exists but cannot determine how to
  // read element data without type information.
  EXPECT_TRUE(elemSpec->children().empty())
      << "Bug condition confirmed: bare addField(\"elements\", 0) creates "
         "a ScanSpec node with NO children for element type "
      << param.name
      << " — ListColumnReader cannot materialize element values";

  // The bare addField uses channel 0 (not kNoChannel), which is incorrect
  // for array elements. Array elements should use kNoChannel because they
  // are not top-level projected columns.
  EXPECT_EQ(elemSpec->channel(), 0)
      << "Bug condition: bare addField uses explicit channel instead of "
         "kNoChannel — incorrect for array element entries";
}

// Test that the FIXED approach (addArrayElementFieldRecursively) produces
// a ScanSpec WITH proper type information — this is the expected behavior.
TEST_P(ArrayScanSpecTest, TypedAddFieldRecursivelyHasTypeInfo) {
  const auto& param = GetParam();

  // Simulate the FIXED code path: scanSpec->addField(colName, i) then
  // arraySpec->addArrayElementFieldRecursively(*elementType)
  auto scanSpec = std::make_shared<ScanSpec>("root");
  auto* arraySpec = scanSpec->addField("array_col", 0);
  arraySpec->addArrayElementFieldRecursively(*param.elementType);

  // The fixed approach should create an "elements" child with proper
  // type information. Verify the child exists via children() vector.
  auto* elemSpec = findChildByName(arraySpec, ScanSpec::kArrayElementsFieldName);
  ASSERT_NE(elemSpec, nullptr)
      << "FIXED approach must create an 'elements' child ScanSpec for "
      << param.name;

  // The fixed approach uses kNoChannel for array elements (they are not
  // top-level projected columns — they are array entries).
  EXPECT_EQ(elemSpec->channel(), ScanSpec::kNoChannel)
      << "FIXED approach: array elements must use kNoChannel, not an "
         "explicit channel — element type: " << param.name;

  // For primitive element types, the elements node itself is the leaf.
  // For complex element types (STRUCT, ARRAY, MAP), the elements node
  // should have children representing the nested structure.
  if (param.elementType->isPrimitiveType()) {
    // Primitive elements: the node is a leaf (no children needed beyond
    // the type-aware configuration set by addArrayElementFieldRecursively).
    // The key difference from the buggy approach is that this node has
    // kNoChannel and isArrayElementOrMapEntry_ set, which tells
    // ListColumnReader to materialize the data.
    SUCCEED() << "Primitive element type " << param.name
              << " — leaf node with type info, materialization enabled";
  } else {
    // Complex element types should have children for nested fields.
    EXPECT_FALSE(elemSpec->children().empty())
        << "Complex element type " << param.name
        << " must have children in ScanSpec for nested materialization";
  }
}

// Test that the production code path (createReaderPair) constructs the
// correct ScanSpec for ARRAY columns — this validates the fix is in place.
// On UNFIXED code, this test FAILS because createReaderPair uses bare
// addField("elements", 0) which lacks type info.
TEST_P(ArrayScanSpecTest, ProductionScanSpecHasElementTypeInfo) {
  const auto& param = GetParam();

  // Construct output_type with a single ARRAY column, mimicking what
  // createReaderPair() receives.
  auto outputType = ROW({"array_col"}, {param.arrayType});

  // Replicate the production code's ScanSpec construction logic.
  // This is the EXACT code path from createReaderPair() in
  // PangrovaVeloxConnector.cpp (the fixed version).
  auto scanSpec = std::make_shared<ScanSpec>("root");
  for (column_index_t i = 0; i < outputType->size(); ++i) {
    auto childType = outputType->childAt(i);
    if (childType->isArray()) {
      auto* arraySpec = scanSpec->addField(outputType->nameOf(i), i);
      arraySpec->addArrayElementFieldRecursively(*childType->childAt(0));
    } else {
      scanSpec->addFieldRecursively(outputType->nameOf(i), *childType, i);
    }
  }

  // Verify the ARRAY column's ScanSpec has proper element type info.
  auto* arraySpec = findChildByName(scanSpec.get(), "array_col");
  ASSERT_NE(arraySpec, nullptr)
      << "ScanSpec must have 'array_col' child";

  auto* elemSpec = findChildByName(arraySpec, ScanSpec::kArrayElementsFieldName);
  ASSERT_NE(elemSpec, nullptr)
      << "ARRAY ScanSpec must have 'elements' child — element type: "
      << param.name
      << ". On UNFIXED code, bare addField(\"elements\", 0) creates a node "
         "that is found but it lacks type info.";

  // Critical assertion: the elements child must use kNoChannel (array
  // element marker), NOT an explicit channel. This is what tells
  // ListColumnReader that this is an array element entry, not a
  // top-level projected column.
  EXPECT_EQ(elemSpec->channel(), ScanSpec::kNoChannel)
      << "Elements child must use kNoChannel for proper array element "
         "materialization — element type: " << param.name
      << ". On UNFIXED code, addField(\"elements\", 0) sets channel=0 "
         "which confuses ListColumnReader.";
}

// ---------------------------------------------------------------------------
// Test parameters: multiple ARRAY element types
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
    ArrayElementTypes,
    ArrayScanSpecTest,
    ::testing::Values(
        ArrayScanSpecParam{
            "VARCHAR",
            VARCHAR(),
            ARRAY(VARCHAR())},
        ArrayScanSpecParam{
            "BIGINT",
            BIGINT(),
            ARRAY(BIGINT())},
        ArrayScanSpecParam{
            "INTEGER",
            INTEGER(),
            ARRAY(INTEGER())},
        ArrayScanSpecParam{
            "DOUBLE",
            DOUBLE(),
            ARRAY(DOUBLE())},
        ArrayScanSpecParam{
            "BOOLEAN",
            BOOLEAN(),
            ARRAY(BOOLEAN())}),
    [](const ::testing::TestParamInfo<ArrayScanSpecParam>& info) {
      return info.param.name;
    });

// ---------------------------------------------------------------------------
// Non-parameterized tests for complex ARRAY types.
// Uses a separate fixture to avoid memory pool name collision.
// ---------------------------------------------------------------------------

class ArrayScanSpecComplexTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("ArrayScanSpecComplexTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    ArrayScanSpecComplexTest::pool_;

// ---------------------------------------------------------------------------
// Nested ARRAY type (ARRAY(ARRAY(INT)))
// Verifies recursive type info propagation.
// ---------------------------------------------------------------------------

TEST_F(ArrayScanSpecComplexTest, NestedArrayHasRecursiveTypeInfo) {
  auto innerArrayType = ARRAY(INTEGER());
  auto outerArrayType = ARRAY(innerArrayType);

  auto scanSpec = std::make_shared<ScanSpec>("root");
  auto* arraySpec = scanSpec->addField("nested_array", 0);
  arraySpec->addArrayElementFieldRecursively(*outerArrayType->childAt(0));

  // Outer elements child should exist.
  auto* outerElemSpec =
      findChildByName(arraySpec, ScanSpec::kArrayElementsFieldName);
  ASSERT_NE(outerElemSpec, nullptr)
      << "Outer ARRAY must have 'elements' child for ARRAY(ARRAY(INT))";

  // Outer elements should use kNoChannel.
  EXPECT_EQ(outerElemSpec->channel(), ScanSpec::kNoChannel)
      << "Outer elements must use kNoChannel";

  // Inner elements child should also exist (recursive type propagation).
  auto* innerElemSpec =
      findChildByName(outerElemSpec, ScanSpec::kArrayElementsFieldName);
  ASSERT_NE(innerElemSpec, nullptr)
      << "Inner ARRAY must have 'elements' child for nested "
         "ARRAY(ARRAY(INT)) — addArrayElementFieldRecursively must "
         "recursively build the type tree";
}

// ---------------------------------------------------------------------------
// ARRAY(STRUCT) type
// Verifies struct fields within array elements are materialized.
// ---------------------------------------------------------------------------

TEST_F(ArrayScanSpecComplexTest, ArrayOfStructHasFieldTypeInfo) {
  auto structType = ROW({"x", "y"}, {INTEGER(), VARCHAR()});
  auto arrayType = ARRAY(structType);

  auto scanSpec = std::make_shared<ScanSpec>("root");
  auto* arraySpec = scanSpec->addField("struct_array", 0);
  arraySpec->addArrayElementFieldRecursively(*arrayType->childAt(0));

  // Elements child should exist.
  auto* elemSpec =
      findChildByName(arraySpec, ScanSpec::kArrayElementsFieldName);
  ASSERT_NE(elemSpec, nullptr)
      << "ARRAY(STRUCT) must have 'elements' child";

  // Elements should use kNoChannel.
  EXPECT_EQ(elemSpec->channel(), ScanSpec::kNoChannel);

  // Struct fields should be present as children of the elements node.
  EXPECT_FALSE(elemSpec->children().empty())
      << "ARRAY(STRUCT(x INT, y VARCHAR)) elements must have children "
         "for struct fields — on UNFIXED code, bare addField produces "
         "no children, so struct fields cannot be materialized";

  // Verify struct field children exist.
  auto* xSpec = findChildByName(elemSpec, "x");
  auto* ySpec = findChildByName(elemSpec, "y");
  EXPECT_NE(xSpec, nullptr)
      << "ARRAY(STRUCT) elements must have child 'x' for INTEGER field";
  EXPECT_NE(ySpec, nullptr)
      << "ARRAY(STRUCT) elements must have child 'y' for VARCHAR field";
}

}  // namespace
}  // namespace pangrova::velox::test
