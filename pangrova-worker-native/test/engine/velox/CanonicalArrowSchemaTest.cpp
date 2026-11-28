#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <stdexcept>
#include <vector>

#include "TestMemoryHelper.h"
#include "engine/velox/serde/CanonicalArrowBatchBridge.h"
#include "engine/velox/serde/CanonicalArrowSchema.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

substrait::Type integer(bool nullable) {
  substrait::Type type;
  type.mutable_i64()->set_nullability(nullable ? substrait::Type::NULLABILITY_NULLABLE
                                             : substrait::Type::NULLABILITY_REQUIRED);
  return type;
}

substrait::NamedStruct declared(substrait::Type type) {
  substrait::NamedStruct schema;
  schema.add_names("logical");
  *schema.mutable_struct_()->add_types() = std::move(type);
  return schema;
}

TEST(CanonicalArrowSchemaTest, ActualBridgeSchemaKeepsRequiredAndNestedDfsNames) {
  const auto pool = pangrova::test::testLeafPool("canonical-arrow-nested-schema");
  substrait::Type row;
  row.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *row.mutable_struct_()->add_types() = integer(false);
  auto schema = declared(row);
  schema.add_names("nested");
  const auto wire = canonicalArrowSchema(schema, pool.get());
  EXPECT_EQ(wire->field(0)->name(), "logical");
  EXPECT_TRUE(wire->field(0)->nullable());
  EXPECT_EQ(wire->field(0)->type()->field(0)->name(), "nested");
  EXPECT_FALSE(wire->field(0)->type()->field(0)->nullable());
  const auto component = facebook::velox::ROW({"task-alias"},
      {facebook::velox::ROW({"nested"}, {facebook::velox::BIGINT()})});
  EXPECT_EQ(physicalArrowSchema(wire, schema, component)->field(0)->name(), "task-alias");
  const auto drift = facebook::velox::ROW({"task-alias"},
      {facebook::velox::ROW({"wrong"}, {facebook::velox::BIGINT()})});
  EXPECT_THROW(physicalArrowSchema(wire, schema, drift), std::invalid_argument);
}

TEST(CanonicalArrowSchemaTest, MapEntryAndKeyAreRequiredEvenWhenCanonicalKeyIsNullable) {
  const auto pool = pangrova::test::testLeafPool("canonical-arrow-map-schema");
  substrait::Type map;
  map.mutable_map()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *map.mutable_map()->mutable_key() = integer(true);
  *map.mutable_map()->mutable_value() = integer(true);
  const auto wire = canonicalArrowSchema(declared(map), pool.get());
  const auto entries = wire->field(0)->type()->field(0);
  EXPECT_TRUE(wire->field(0)->nullable());
  EXPECT_FALSE(entries->nullable());
  EXPECT_FALSE(entries->type()->field(0)->nullable());
  EXPECT_TRUE(entries->type()->field(1)->nullable());
}

TEST(CanonicalArrowSchemaTest, MetadataRebindingRetainsActualPayloadBuffersAndCardinality) {
  const auto pool = pangrova::test::testLeafPool("canonical-arrow-buffer-schema");
  arrow::Int64Builder builder;
  ASSERT_TRUE(builder.Append(17).ok());
  ASSERT_TRUE(builder.Append(23).ok());
  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(builder.Finish(&values).ok());
  const auto source = arrow::RecordBatch::Make(arrow::schema({arrow::field("physical", arrow::int64())}),
      2, std::vector<std::shared_ptr<arrow::Array>>{values});
  const auto wire = canonicalArrowSchema(declared(integer(false)), pool.get());
  const auto rebound = bindCanonicalArrowSchema(source, wire);
  EXPECT_EQ(rebound->num_rows(), 2);
  EXPECT_EQ(rebound->column(0).get(), values.get());
  EXPECT_EQ(rebound->column(0)->data()->buffers[1].get(), values->data()->buffers[1].get());
  EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(rebound->column(0))->Value(1), 23);
  EXPECT_FALSE(rebound->schema()->field(0)->nullable());
  EXPECT_EQ(source->schema()->field(0)->name(), "physical");
}

TEST(CanonicalArrowSchemaTest, ZeroColumnSchemaPreservesRealBatchCardinality) {
  const auto pool = pangrova::test::testLeafPool("canonical-arrow-empty-schema");
  substrait::NamedStruct declared;
  declared.mutable_struct_();
  const auto schema = canonicalArrowSchema(declared, pool.get());
  const auto batch = arrow::RecordBatch::Make(schema, 3, std::vector<std::shared_ptr<arrow::Array>>{});
  const auto rebound = bindCanonicalArrowSchema(batch, schema);
  EXPECT_EQ(rebound->num_rows(), 3);
  EXPECT_EQ(rebound->num_columns(), 0);
}

TEST(CanonicalArrowSchemaTest, NullParentMasksUnrepresentableTimestampBeforeArrowConversion) {
  using namespace facebook::velox;
  const auto pool = pangrova::test::testLeafPool("canonical-arrow-null-timestamp");
  facebook::velox::test::VectorMaker maker(pool.get());
  const auto values = maker.flatVector<Timestamp>(
      {Timestamp(1, 234), Timestamp(10'000'000'000, 0), Timestamp(2, 345)});
  auto nested = maker.rowVector({"created_at"}, {values});
  const auto rows = maker.rowVector({"file"}, {nested});
  EXPECT_THROW(exportArrowRecordBatch(rows, pool.get()), VeloxUserError);

  nested->setNull(1, true);
  const auto batch = exportArrowRecordBatch(rows, pool.get());
  const auto files = std::static_pointer_cast<arrow::StructArray>(batch->column(0));
  const auto timestamps = std::static_pointer_cast<arrow::TimestampArray>(files->field(0));
  EXPECT_EQ(timestamps->Value(0), 1'000'000'234);
  EXPECT_TRUE(files->IsNull(1));
  EXPECT_TRUE(timestamps->IsNull(1));
  EXPECT_EQ(timestamps->Value(2), 2'000'000'345);
}

}  // namespace
}  // namespace pangrova::velox::test
