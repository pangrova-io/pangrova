#include <gtest/gtest.h>
#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>

#include "../TestMemoryHelper.h"
#include "memory/ColumnarMemoryPool.h"
#include "result/ResultOwnershipState.h"

namespace pangrova::result::test {
namespace {
class ResultOwnershipStateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    leaf_ = pangrova::test::testLeafPool("result-ownership");
    pool_ = std::make_shared<memory::ColumnarMemoryPool>(leaf_);
    state_ = std::make_shared<ResultOwnershipState>();
  }
  std::shared_ptr<arrow::RecordBatch> values() {
    arrow::Int64Builder builder(pool_.get());
    EXPECT_TRUE(builder.AppendValues({41, 42, 43}).ok());
    auto built = builder.Finish();
    EXPECT_TRUE(built.ok());
    return memory::retainColumnarBatch(arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int64())}), 3, {*built}), pool_);
  }
  std::shared_ptr<facebook::velox::memory::MemoryPool> leaf_;
  std::shared_ptr<memory::ColumnarMemoryPool> pool_;
  std::shared_ptr<ResultOwnershipState> state_;
};

TEST_F(ResultOwnershipStateTest, NoReadersDoesNotProveUnclaimedProducerOrQueueDrained) {
  EXPECT_FALSE(state_->evidence().drained());
  state_->stopProducer();
  EXPECT_FALSE(state_->evidence().drained());
  state_->revokeAndClearQueue();
  EXPECT_TRUE(state_->evidence().drained());
  EXPECT_THROW(state_->acquireReader(), std::runtime_error);
}

TEST_F(ResultOwnershipStateTest, ReaderLifetimeRemainsChargedAfterProducerEofAndRevocation) {
  state_->acquireReader();
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_EQ(state_->evidence().active_readers, 1);
  EXPECT_FALSE(state_->evidence().drained());
  state_->releaseReader();
  EXPECT_TRUE(state_->evidence().drained());
}

TEST_F(ResultOwnershipStateTest, LastColumnSliceRetainsRealBufferAndDrainLeaseAfterRootRelease) {
  auto retained = retainResultBatch(values(), state_);
  auto slice = std::static_pointer_cast<arrow::Int64Array>(retained->column(0)->Slice(1, 1));
  retained.reset();
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_EQ(state_->evidence().retained_batch_owners, 1);
  EXPECT_FALSE(state_->evidence().drained());
  EXPECT_EQ(slice->Value(0), 42);
  EXPECT_GT(pool_->bytes_allocated(), 0);
  slice.reset();
  EXPECT_TRUE(state_->evidence().drained());
  EXPECT_EQ(pool_->bytes_allocated(), 0);
  EXPECT_EQ(leaf_->usedBytes(), 0);
}

TEST_F(ResultOwnershipStateTest, ZeroColumnNonzeroRowsRequireActualRootRelease) {
  auto retained = retainResultBatch(
      arrow::RecordBatch::Make(arrow::schema({}), 7, arrow::ArrayVector{}), state_);
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_EQ(retained->num_rows(), 7);
  EXPECT_EQ(retained->num_columns(), 0);
  EXPECT_FALSE(state_->evidence().drained());
  retained.reset();
  EXPECT_TRUE(state_->evidence().drained());
}

TEST_F(ResultOwnershipStateTest, DictionaryValueBufferRetainsLeaseIndependentlyOfArrayAndBatch) {
  arrow::Int8Builder indices(pool_.get());
  ASSERT_TRUE(indices.AppendValues({0, 1, 0}).ok());
  auto indexArray = indices.Finish();
  ASSERT_TRUE(indexArray.ok());
  auto dictionary = values()->column(0);
  auto encoded = arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int8(), arrow::int64()),
      *indexArray, dictionary);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto retained = retainResultBatch(memory::retainColumnarBatch(arrow::RecordBatch::Make(
      arrow::schema({arrow::field("encoded", (*encoded)->type())}), 3, {*encoded}), pool_), state_);
  auto valueBuffer = retained->column(0)->data()->dictionary->buffers[1];
  retained.reset();
  encoded = arrow::Status::Invalid("released local dictionary root");
  dictionary.reset();
  indexArray = arrow::Status::Invalid("released local index root");
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_FALSE(state_->evidence().drained());
  EXPECT_EQ(reinterpret_cast<const int64_t*>(valueBuffer->data())[1], 42);
  valueBuffer.reset();
  EXPECT_TRUE(state_->evidence().drained());
}
}  // namespace
}  // namespace pangrova::result::test
