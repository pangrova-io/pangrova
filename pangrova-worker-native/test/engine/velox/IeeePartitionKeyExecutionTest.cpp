#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "CanonicalTaskExecution.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

template <typename Float, typename Bits>
void checkKeys(const std::string& function, TypePtr type, const std::vector<Bits>& bits,
    const std::vector<Bits>& canonical) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto pool = pangrova::test::testLeafPool("ieee-partition-key");
  auto input = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(ROW({"v"}, {type}), bits.size() + 1, pool.get()));
  auto* values = input->childAt(0)->template asFlatVector<Float>();
  for (size_t i = 0; i < bits.size(); ++i) values->set(i, std::bit_cast<Float>(bits[i]));
  values->setNull(bits.size(), true);
  auto source = std::make_shared<core::ValuesNode>("ieee-source", std::vector<RowVectorPtr>{input});
  auto project = std::make_shared<core::ProjectNode>("ieee-key", std::vector<std::string>{"k"},
      std::vector<core::TypedExprPtr>{std::make_shared<core::CallTypedExpr>(VARBINARY(),
          std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(type, "v")}, function)},
      source);
  const auto output = executeCanonicalTask(project, registry);
  ASSERT_EQ(output.batches.size(), 1);
  auto* encoded = output.batches[0]->childAt(0)->asFlatVector<StringView>();
  for (size_t i = 0; i < bits.size(); ++i) {
    const auto key = encoded->valueAt(i).str();
    ASSERT_EQ(key.size(), sizeof(Bits));
    Bits decoded = 0;
    for (unsigned char byte : key) decoded = (decoded << 8) | byte;
    EXPECT_EQ(decoded, canonical[i]);
  }
  EXPECT_TRUE(encoded->isNullAt(bits.size()));
  auto distinct = std::make_shared<core::AggregationNode>("ieee-groups", core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(VARBINARY(), "k")},
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<std::string>{},
      std::vector<core::AggregationNode::Aggregate>{}, false, false, project);
  const auto groups = executeCanonicalTask(distinct, registry);
  size_t count = 0;
  for (const auto& batch : groups.batches) count += batch->size();
  // Two NaN payloads merge; signed zeros, infinities, subnormals and NULL remain distinct.
  EXPECT_EQ(count, 8);
}

TEST(IeeePartitionKeyExecutionTest, FloatKeysPreserveSignedZeroAndNormalizeNanPayloads) {
  checkKeys<float, uint32_t>("to_ieee754_32", REAL(),
      {0, 0x80000000U, 0x7fc00001U, 0xffc01234U, 0x7f800000U, 0xff800000U, 1, 0x80000001U},
      {0, 0x80000000U, 0x7fc00000U, 0x7fc00000U, 0x7f800000U, 0xff800000U, 1, 0x80000001U});
}

TEST(IeeePartitionKeyExecutionTest, DoubleKeysPreserveSignedZeroAndNormalizeNanPayloads) {
  checkKeys<double, uint64_t>("to_ieee754_64", DOUBLE(),
      {0, 0x8000000000000000ULL, 0x7ff8000000000001ULL, 0xfff8000000001234ULL,
       0x7ff0000000000000ULL, 0xfff0000000000000ULL, 1, 0x8000000000000001ULL},
      {0, 0x8000000000000000ULL, 0x7ff8000000000000ULL, 0x7ff8000000000000ULL,
       0x7ff0000000000000ULL, 0xfff0000000000000ULL, 1, 0x8000000000000001ULL});
}
}  // namespace
}  // namespace pangrova::velox::test
