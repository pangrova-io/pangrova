#include <gtest/gtest.h>

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include "result/FlightResultServer.h"

namespace pangrova::result::test {
namespace {

TEST(FlightResultColumnTypeTest, AnnotatesOnlyExactIndexedVectorColumns) {
  auto storage = arrow::schema({
      arrow::field("first_embedding", arrow::list(arrow::float32())),
      arrow::field("ordinary_array", arrow::list(arrow::float32())),
      arrow::field("second_embedding", arrow::list(arrow::float32()))});

  auto result = detail::withResultColumnTypes(storage, {{0, 3}, {2, 5}});

  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ((*result)->field(0)->metadata()->Get("ARROW:extension:name").ValueOrDie(),
            "pangrova.vector");
  EXPECT_EQ((*result)->field(0)->metadata()->Get("ARROW:extension:metadata").ValueOrDie(),
            "3");
  EXPECT_EQ((*result)->field(1)->metadata(), nullptr);
  EXPECT_EQ((*result)->field(2)->metadata()->Get("ARROW:extension:metadata").ValueOrDie(),
            "5");
}

TEST(FlightResultColumnTypeTest, RejectsSchemaAndPhysicalTypeMismatches) {
  auto storage = arrow::schema({
      arrow::field("embedding", arrow::list(arrow::float32())),
      arrow::field("ordinary", arrow::list(arrow::float64()))});

  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{2, 3}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{1, 3}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{0, 0}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{0, 3}, {0, 5}}).ok());
}

}  // namespace
}  // namespace pangrova::result::test
