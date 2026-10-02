#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/FunctionRegistry.h"

namespace pangrova::capability::test {
namespace {
using namespace facebook::velox;

TEST(FunctionMetadataSnapshotTest, ActualEntriesRetainPriorityAndPhysicalImplementationFilters) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto entries = exec::simpleFunctions().getFunctionMetadataSnapshot("round");
  ASSERT_FALSE(entries.empty());
  bool shortDecimal = false;
  bool longDecimal = false;
  for (const auto& entry : entries) {
    ASSERT_NE(entry, nullptr);
    ASSERT_NE(entry->signature(), nullptr);
    EXPECT_EQ(entry->argPhysicalTypes().size(), entry->signature()->argumentTypes().size());
    EXPECT_GT(entry->priority(), 0);
    for (const auto& argument : entry->argPhysicalTypes()) {
      shortDecimal |= argument->kind() == TypeKind::BIGINT;
      longDecimal |= argument->kind() == TypeKind::HUGEINT;
    }
  }
  EXPECT_TRUE(shortDecimal);
  EXPECT_TRUE(longDecimal);
  EXPECT_TRUE(resolveFunctionOrCallableSpecialForm("round", {DECIMAL(12, 3)})->equivalent(*DECIMAL(10, 0)));
}

TEST(FunctionMetadataSnapshotTest, SnapshotOwnsActualMetadataAfterRegistrationIsRemoved) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string alias = "pangrova_metadata_snapshot_test_plus";
  const bool registered = facebook::velox::registerFunction<
      functions::PlusFunction, int64_t, int64_t, int64_t>({alias});
  ASSERT_TRUE(registered);
  const auto entries = exec::simpleFunctions().getFunctionMetadataSnapshot(alias);
  exec::mutableSimpleFunctions().removeFunction(alias);
  EXPECT_TRUE(exec::simpleFunctions().getFunctionMetadataSnapshot(alias).empty());
  ASSERT_EQ(entries.size(), 1);
  EXPECT_TRUE(entries.front()->argPhysicalTypes().front()->equivalent(*BIGINT()));
  EXPECT_TRUE(entries.front()->resultPhysicalType()->equivalent(*BIGINT()));
  EXPECT_EQ(entries.front()->signature()->argumentTypes().size(), 2);
  EXPECT_TRUE(entries.front()->isDeterministic());
}

TEST(FunctionMetadataSnapshotTest, UnknownIdentityProducesNoInventedEntry) {
  pangrova::test::detail::ensureRuntimeInitialized();
  EXPECT_TRUE(exec::simpleFunctions().getFunctionMetadataSnapshot("pangrova_unregistered_snapshot_test").empty());
}
}  // namespace
}  // namespace pangrova::capability::test
