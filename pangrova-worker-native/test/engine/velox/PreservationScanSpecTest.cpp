// PreservationScanSpecTest: Preservation property tests for non-ARRAY/MAP
// column ScanSpec construction.
//
// Validates: Requirements 3.1, 3.3, 3.4
//
// Property 2: Preservation — Primitive and STRUCT Column ScanSpec
// Construction Unchanged
//
// These tests verify that the ARRAY/MAP fix does NOT break existing behavior
// for non-ARRAY/MAP columns:
//   - Primitive columns (BIGINT, VARCHAR, etc.) use addFieldRecursively at
//     root level and produce valid ScanSpec nodes with correct channels.
//   - STRUCT columns use addFieldRecursively at root level and produce
//     ScanSpec nodes with all struct children present.
//   - Subfield pruning (current_required_subfields_ non-empty) adds only
//     specified subfields, not all children.
//
// Non-Bug Condition:
//   !isBugCondition(column) := column.type.isPrimitive()
//                           OR column.type.isStruct()
//                           OR (column has current_required_subfields_ non-empty)
//
// EXPECTED OUTCOME: All tests PASS on both fixed and unfixed code.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Type.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;
using facebook::velox::common::ScanSpec;

// Helper: find a child ScanSpec by name using children() vector.
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

// ===========================================================================
// Property 2.1: Primitive Column Preservation
//
// For all primitive column types, scanSpec->addFieldRecursively(colName,
// *childType, i) produces a valid ScanSpec node with the correct channel.
//
// **Validates: Requirements 3.1**
// ===========================================================================

struct PrimitivePreservationParam {
  std::string name;
  std::shared_ptr<const Type> type;
};

class PrimitivePreservationTest
    : public ::testing::TestWithParam<PrimitivePreservationParam> {};

// Verify that addFieldRecursively on root ScanSpec produces a valid child
// node for primitive types. This is the code path used by createReaderPair()
// for all primitive columns.
TEST_P(PrimitivePreservationTest, AddFieldRecursivelyProducesValidNode) {
  const auto& param = GetParam();

  // Simulate createReaderPair() for a primitive column:
  //   scanSpec->addFieldRecursively(colName, *childType, i)
  auto scanSpec = std::make_shared<ScanSpec>("root");
  scanSpec->addFieldRecursively("col_0", *param.type, 0);

  // The root ScanSpec must have exactly one child for the column.
  ASSERT_EQ(scanSpec->children().size(), 1u)
      << "addFieldRecursively must create exactly one child for primitive "
         "type " << param.name;

  auto* childSpec = scanSpec->children()[0].get();
  ASSERT_NE(childSpec, nullptr);

  // The child must have the correct field name.
  EXPECT_EQ(childSpec->fieldName(), "col_0")
      << "Child field name must match column name for " << param.name;

  // The child must have the correct channel (column index).
  EXPECT_EQ(childSpec->channel(), 0)
      << "Primitive column " << param.name
      << " must have channel=0 (its column index)";

  // Primitive types are leaf nodes — no children expected.
  EXPECT_TRUE(childSpec->children().empty())
      << "Primitive type " << param.name
      << " must be a leaf node (no children in ScanSpec)";
}

// Verify that multiple primitive columns can coexist in the same ScanSpec
// with distinct channels. This mirrors createReaderPair() iterating over
// output_type_ columns.
TEST_P(PrimitivePreservationTest, MultipleColumnsHaveDistinctChannels) {
  const auto& param = GetParam();

  auto scanSpec = std::make_shared<ScanSpec>("root");
  // Add three columns of the same type at different channel indices.
  scanSpec->addFieldRecursively("col_0", *param.type, 0);
  scanSpec->addFieldRecursively("col_1", *param.type, 1);
  scanSpec->addFieldRecursively("col_2", *param.type, 2);

  ASSERT_EQ(scanSpec->children().size(), 3u)
      << "Three addFieldRecursively calls must produce three children for "
      << param.name;

  // Verify each child has the correct channel.
  auto* c0 = findChildByName(scanSpec.get(), "col_0");
  auto* c1 = findChildByName(scanSpec.get(), "col_1");
  auto* c2 = findChildByName(scanSpec.get(), "col_2");

  ASSERT_NE(c0, nullptr);
  ASSERT_NE(c1, nullptr);
  ASSERT_NE(c2, nullptr);

  EXPECT_EQ(c0->channel(), 0);
  EXPECT_EQ(c1->channel(), 1);
  EXPECT_EQ(c2->channel(), 2);
}

// Verify the production code path: construct output_type with primitive
// columns and replicate createReaderPair() logic.
TEST_P(PrimitivePreservationTest, ProductionCodePathProducesCorrectSpec) {
  const auto& param = GetParam();

  // Simulate output_type_ with two primitive columns.
  auto outputType = ROW({"prim_col", "other_col"}, {param.type, BIGINT()});

  auto scanSpec = std::make_shared<ScanSpec>("root");
  for (column_index_t i = 0; i < outputType->size(); ++i) {
    auto childType = outputType->childAt(i);
    if (childType->isArray()) {
      auto* arraySpec = scanSpec->addField(outputType->nameOf(i), i);
      arraySpec->addArrayElementFieldRecursively(*childType->childAt(0));
    } else if (childType->isMap()) {
      auto* mapSpec = scanSpec->addField(outputType->nameOf(i), i);
      mapSpec->addMapKeyFieldRecursively(*childType->childAt(0));
      mapSpec->addMapValueFieldRecursively(*childType->childAt(1));
    } else {
      scanSpec->addFieldRecursively(outputType->nameOf(i), *childType, i);
    }
  }

  // Both columns must be present.
  ASSERT_EQ(scanSpec->children().size(), 2u);

  auto* primSpec = findChildByName(scanSpec.get(), "prim_col");
  ASSERT_NE(primSpec, nullptr)
      << "Primitive column 'prim_col' of type " << param.name
      << " must be present in ScanSpec";
  EXPECT_EQ(primSpec->channel(), 0);
  EXPECT_TRUE(primSpec->children().empty())
      << "Primitive column must be a leaf node";

  auto* otherSpec = findChildByName(scanSpec.get(), "other_col");
  ASSERT_NE(otherSpec, nullptr);
  EXPECT_EQ(otherSpec->channel(), 1);
}

INSTANTIATE_TEST_SUITE_P(
    PrimitiveTypes,
    PrimitivePreservationTest,
    ::testing::Values(
        PrimitivePreservationParam{"BIGINT", BIGINT()},
        PrimitivePreservationParam{"INTEGER", INTEGER()},
        PrimitivePreservationParam{"SMALLINT", SMALLINT()},
        PrimitivePreservationParam{"TINYINT", TINYINT()},
        PrimitivePreservationParam{"DOUBLE", DOUBLE()},
        PrimitivePreservationParam{"REAL", REAL()},
        PrimitivePreservationParam{"BOOLEAN", BOOLEAN()},
        PrimitivePreservationParam{"VARCHAR", VARCHAR()},
        PrimitivePreservationParam{"VARBINARY", VARBINARY()},
        PrimitivePreservationParam{"TIMESTAMP", TIMESTAMP()}),
    [](const ::testing::TestParamInfo<PrimitivePreservationParam>& info) {
      return info.param.name;
    });

// ===========================================================================
// Property 2.2: STRUCT Column Preservation
//
// For STRUCT columns, scanSpec->addFieldRecursively(colName, *childType, i)
// produces a ScanSpec with all struct children present.
//
// **Validates: Requirements 3.3**
// ===========================================================================

struct StructPreservationParam {
  std::string name;
  std::shared_ptr<const Type> structType;
  std::vector<std::string> expectedChildren;
};

class StructPreservationTest
    : public ::testing::TestWithParam<StructPreservationParam> {};

// Verify that addFieldRecursively on root ScanSpec produces a node with
// all struct children for STRUCT types.
TEST_P(StructPreservationTest, AddFieldRecursivelyProducesAllChildren) {
  const auto& param = GetParam();

  auto scanSpec = std::make_shared<ScanSpec>("root");
  scanSpec->addFieldRecursively("struct_col", *param.structType, 0);

  // Root must have one child for the struct column.
  ASSERT_EQ(scanSpec->children().size(), 1u)
      << "addFieldRecursively must create one child for STRUCT type "
      << param.name;

  auto* structSpec = scanSpec->children()[0].get();
  ASSERT_NE(structSpec, nullptr);

  EXPECT_EQ(structSpec->fieldName(), "struct_col");
  EXPECT_EQ(structSpec->channel(), 0)
      << "STRUCT column must have channel=0 (its column index)";

  // STRUCT node must have children for each struct field.
  EXPECT_EQ(structSpec->children().size(), param.expectedChildren.size())
      << "STRUCT type " << param.name
      << " must have " << param.expectedChildren.size()
      << " children in ScanSpec";

  // Verify each expected child field is present.
  for (const auto& fieldName : param.expectedChildren) {
    auto* fieldSpec = findChildByName(structSpec, fieldName);
    EXPECT_NE(fieldSpec, nullptr)
        << "STRUCT type " << param.name
        << " must have child '" << fieldName << "' in ScanSpec";
  }
}

// Verify the production code path for STRUCT columns.
TEST_P(StructPreservationTest, ProductionCodePathProducesCorrectSpec) {
  const auto& param = GetParam();

  // Simulate output_type_ with a STRUCT column and a primitive column.
  auto outputType =
      ROW({"struct_col", "id_col"}, {param.structType, BIGINT()});

  auto scanSpec = std::make_shared<ScanSpec>("root");
  for (column_index_t i = 0; i < outputType->size(); ++i) {
    auto childType = outputType->childAt(i);
    if (childType->isArray()) {
      auto* arraySpec = scanSpec->addField(outputType->nameOf(i), i);
      arraySpec->addArrayElementFieldRecursively(*childType->childAt(0));
    } else if (childType->isMap()) {
      auto* mapSpec = scanSpec->addField(outputType->nameOf(i), i);
      mapSpec->addMapKeyFieldRecursively(*childType->childAt(0));
      mapSpec->addMapValueFieldRecursively(*childType->childAt(1));
    } else {
      scanSpec->addFieldRecursively(outputType->nameOf(i), *childType, i);
    }
  }

  // Both columns must be present.
  ASSERT_EQ(scanSpec->children().size(), 2u);

  auto* structSpec = findChildByName(scanSpec.get(), "struct_col");
  ASSERT_NE(structSpec, nullptr)
      << "STRUCT column must be present in ScanSpec";
  EXPECT_EQ(structSpec->channel(), 0);

  // All struct children must be present.
  for (const auto& fieldName : param.expectedChildren) {
    auto* fieldSpec = findChildByName(structSpec, fieldName);
    EXPECT_NE(fieldSpec, nullptr)
        << "STRUCT child '" << fieldName << "' must be present after "
           "addFieldRecursively for " << param.name;
  }

  // Primitive column must also be present and correct.
  auto* idSpec = findChildByName(scanSpec.get(), "id_col");
  ASSERT_NE(idSpec, nullptr);
  EXPECT_EQ(idSpec->channel(), 1);
  EXPECT_TRUE(idSpec->children().empty());
}

INSTANTIATE_TEST_SUITE_P(
    StructTypes,
    StructPreservationTest,
    ::testing::Values(
        StructPreservationParam{
            "SimpleStruct",
            ROW({"x", "y"}, {INTEGER(), VARCHAR()}),
            {"x", "y"}},
        StructPreservationParam{
            "ThreeFieldStruct",
            ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), BOOLEAN()}),
            {"a", "b", "c"}},
        StructPreservationParam{
            "NestedStruct",
            ROW({"outer", "val"},
                {ROW({"inner_x", "inner_y"}, {INTEGER(), VARCHAR()}),
                 BIGINT()}),
            {"outer", "val"}},
        StructPreservationParam{
            "SingleFieldStruct",
            ROW({"only_field"}, {VARCHAR()}),
            {"only_field"}}),
    [](const ::testing::TestParamInfo<StructPreservationParam>& info) {
      return info.param.name;
    });

// ===========================================================================
// Property 2.3: Subfield Pruning Preservation
//
// For columns with subfield pruning (current_required_subfields_ non-empty),
// only specified subfields are added — the pruned path remains unchanged.
//
// **Validates: Requirements 3.4**
// ===========================================================================

struct SubfieldPruningParam {
  std::string name;
  std::shared_ptr<const Type> columnType;
  std::vector<std::string> requiredSubfields;
  std::vector<std::string> allSubfields;
};

class SubfieldPruningPreservationTest
    : public ::testing::TestWithParam<SubfieldPruningParam> {};

// Verify that the pruned path (addField + individual subfield addField)
// adds ONLY the specified subfields, not all children.
TEST_P(SubfieldPruningPreservationTest, OnlySpecifiedSubfieldsAdded) {
  const auto& param = GetParam();

  // Simulate the pruned code path from createReaderPair():
  //   auto* fieldSpec = scanSpec->addField(colName, i);
  //   for (const auto& subfield : it->second) {
  //     fieldSpec->addField(subfield, 0);
  //   }
  auto scanSpec = std::make_shared<ScanSpec>("root");
  auto* fieldSpec = scanSpec->addField("pruned_col", 0);
  for (const auto& subfield : param.requiredSubfields) {
    fieldSpec->addField(subfield, 0);
  }

  // The column node must exist.
  ASSERT_EQ(scanSpec->children().size(), 1u);
  auto* colSpec = scanSpec->children()[0].get();
  ASSERT_NE(colSpec, nullptr);
  EXPECT_EQ(colSpec->fieldName(), "pruned_col");
  EXPECT_EQ(colSpec->channel(), 0);

  // Only the required subfields must be present as children.
  EXPECT_EQ(colSpec->children().size(), param.requiredSubfields.size())
      << "Pruned path for " << param.name
      << " must add exactly " << param.requiredSubfields.size()
      << " subfields, not all " << param.allSubfields.size();

  // Verify each required subfield is present.
  for (const auto& subfield : param.requiredSubfields) {
    auto* subfieldSpec = findChildByName(colSpec, subfield);
    EXPECT_NE(subfieldSpec, nullptr)
        << "Required subfield '" << subfield
        << "' must be present in pruned ScanSpec for " << param.name;
  }

  // Verify non-required subfields are NOT present.
  for (const auto& subfield : param.allSubfields) {
    bool isRequired = false;
    for (const auto& req : param.requiredSubfields) {
      if (req == subfield) {
        isRequired = true;
        break;
      }
    }
    if (!isRequired) {
      auto* subfieldSpec = findChildByName(colSpec, subfield);
      EXPECT_EQ(subfieldSpec, nullptr)
          << "Non-required subfield '" << subfield
          << "' must NOT be present in pruned ScanSpec for " << param.name;
    }
  }
}

// Verify that pruned path produces different result than full recursive path.
// This confirms the pruning is actually effective.
TEST_P(SubfieldPruningPreservationTest, PrunedPathDiffersFromFullPath) {
  const auto& param = GetParam();

  // Full path: addFieldRecursively adds ALL children.
  auto fullSpec = std::make_shared<ScanSpec>("root");
  fullSpec->addFieldRecursively("col", *param.columnType, 0);

  auto* fullColSpec = findChildByName(fullSpec.get(), "col");
  ASSERT_NE(fullColSpec, nullptr);

  // Pruned path: only required subfields.
  auto prunedSpec = std::make_shared<ScanSpec>("root");
  auto* prunedColSpec = prunedSpec->addField("col", 0);
  for (const auto& subfield : param.requiredSubfields) {
    prunedColSpec->addField(subfield, 0);
  }

  // If we're pruning (not requesting all subfields), the pruned path
  // must have fewer children than the full path.
  if (param.requiredSubfields.size() < param.allSubfields.size()) {
    EXPECT_LT(prunedColSpec->children().size(),
              fullColSpec->children().size())
        << "Pruned path for " << param.name
        << " must have fewer children than full recursive path";
  }
}

// Verify the production code path with subfield pruning.
TEST_P(SubfieldPruningPreservationTest, ProductionPrunedPathCorrect) {
  const auto& param = GetParam();

  // Simulate output_type_ and current_required_subfields_.
  auto outputType = ROW({"pruned_col"}, {param.columnType});
  std::unordered_map<std::string, std::vector<std::string>>
      requiredSubfields;
  requiredSubfields["pruned_col"] = param.requiredSubfields;

  // Replicate createReaderPair() logic with subfield pruning.
  auto scanSpec = std::make_shared<ScanSpec>("root");
  for (column_index_t i = 0; i < outputType->size(); ++i) {
    const auto& colName = outputType->nameOf(i);
    auto it = requiredSubfields.find(colName);
    if (it != requiredSubfields.end() && !it->second.empty()) {
      // Pruned path.
      auto* fSpec = scanSpec->addField(colName, i);
      for (const auto& subfield : it->second) {
        fSpec->addField(subfield, 0);
      }
    } else {
      // Full path.
      auto childType = outputType->childAt(i);
      if (childType->isArray()) {
        auto* arraySpec = scanSpec->addField(colName, i);
        arraySpec->addArrayElementFieldRecursively(*childType->childAt(0));
      } else if (childType->isMap()) {
        auto* mapSpec = scanSpec->addField(colName, i);
        mapSpec->addMapKeyFieldRecursively(*childType->childAt(0));
        mapSpec->addMapValueFieldRecursively(*childType->childAt(1));
      } else {
        scanSpec->addFieldRecursively(colName, *childType, i);
      }
    }
  }

  // Verify pruned column has only required subfields.
  auto* colSpec = findChildByName(scanSpec.get(), "pruned_col");
  ASSERT_NE(colSpec, nullptr);
  EXPECT_EQ(colSpec->children().size(), param.requiredSubfields.size())
      << "Production pruned path must add exactly the required subfields";
}

INSTANTIATE_TEST_SUITE_P(
    SubfieldPruning,
    SubfieldPruningPreservationTest,
    ::testing::Values(
        SubfieldPruningParam{
            "StructPruneOneOfTwo",
            ROW({"x", "y"}, {INTEGER(), VARCHAR()}),
            {"x"},
            {"x", "y"}},
        SubfieldPruningParam{
            "StructPruneTwoOfThree",
            ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), BOOLEAN()}),
            {"a", "c"},
            {"a", "b", "c"}},
        SubfieldPruningParam{
            "StructPruneOneOfFour",
            ROW({"w", "x", "y", "z"},
                {INTEGER(), VARCHAR(), DOUBLE(), BOOLEAN()}),
            {"y"},
            {"w", "x", "y", "z"}},
        SubfieldPruningParam{
            "MapPruneKeys",
            MAP(VARCHAR(), INTEGER()),
            {"keys"},
            {"keys", "values"}}),
    [](const ::testing::TestParamInfo<SubfieldPruningParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
