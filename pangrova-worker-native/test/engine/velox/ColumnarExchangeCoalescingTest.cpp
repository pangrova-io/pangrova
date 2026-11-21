#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "jni/RuntimeInitializer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/SerializedPage.h"
#include "velox/exec/Task.h"
#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "engine/velox/serde/CanonicalSerdeOptionsFactory.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
namespace schema = pangrova::velox::schema;

substrait::NamedStruct regularSchema() {
  return schema::named({
      schema::computed("id", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED)),
      schema::computed("text", schema::primitive("string", substrait::Type::NULLABILITY_NULLABLE))});
}

// Component test of the actual SDK operator and canonical serialized pages.
// It intentionally supplies pages directly: this is not distributed SQL E2E.
class ColumnarExchangeInput final : public exec::Exchange {
public:
  ColumnarExchangeInput(exec::DriverCtx *context,
                        const std::shared_ptr<const core::ExchangeNode> &node)
      : exec::Exchange(0, context, node, nullptr) {}

  void supply(std::vector<std::unique_ptr<folly::IOBuf>> pages) {
    VELOX_CHECK(currentPages_.empty());
    VELOX_CHECK_NULL(inputStream_);
    for (auto &page : pages) {
      currentPages_.push_back(std::make_unique<exec::SerializedPage>(
          std::move(page), nullptr, std::optional<int64_t>{}));
    }
  }

  const RowVectorPtr &published() const { return result_; }
};

class ColumnarExchangeCoalescingTest : public ::testing::Test {
protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("columnar-exchange-coalescing");
  }

  void TearDown() override {
    exchange_.reset();
    context_.reset();
    task_.reset();
  }

  void create(const RowTypePtr &type, substrait::NamedStruct declared) {
    options_.canonical_schema = std::make_shared<const substrait::NamedStruct>(declared);
    auto node =
        std::make_shared<core::ExchangeNode>("exchange", type, "ArrowBatch");
    const auto serde = std::make_shared<const CanonicalSerdeOptionsFactory>(
        CanonicalSerdeOptionsFactory::SchemaBindings{{node->id(), std::move(declared)}}, options_.maximum_bytes);
    auto query = core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
        facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"},
            {"require_vector_serde_options_factory", "true"}},
            facebook::velox::exec::FunctionRegistrySnapshot::capture(), serde));
    task_ = exec::Task::create("columnar-exchange-component",
                               core::PlanFragment{node}, 0, std::move(query),
                               exec::Task::ExecutionMode::kParallel,
                               exec::Consumer{});
    context_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    exchange_ = std::make_unique<ColumnarExchangeInput>(context_.get(), node);
  }

  RowVectorPtr row(int64_t id) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    return maker.rowVector(
        {"id", "text"},
        {maker.flatVector<int64_t>({id}),
         maker.flatVectorNullable<std::string>(
             {id % 7 == 0 ? std::nullopt
                          : std::optional<std::string>(std::string(256, 'x') +
                                                       std::to_string(id))})});
  }

  std::unique_ptr<folly::IOBuf> encode(const RowVectorPtr &batch) {
    StreamArena arena(pool_.get());
    ArrowBatchVectorSerde serde;
    auto writer = serde.createIterativeSerializer(batch->rowType(),
                                                  batch->size(), &arena, &options_);
    writer->append(batch);
    IOBufOutputStream output(*pool_);
    writer->flush(&output);
    return output.getIOBuf();
  }

  void check(const RowVectorPtr &batch, int64_t first) {
    for (vector_size_t index = 0; index < batch->size(); ++index) {
      auto reference = row(first + index);
      EXPECT_TRUE(batch->equalValueAt(reference.get(), index, 0))
          << first + index;
    }
  }

  int64_t metric(const std::string &name) {
    const auto stats = exchange_->stats(false);
    const auto found = stats.runtimeStats.find(name);
    EXPECT_NE(found, stats.runtimeStats.end()) << name;
    return found == stats.runtimeStats.end() ? -1 : found->second.sum;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  ArrowBatchVectorSerde::StreamOptions options_;
  std::shared_ptr<exec::Task> task_;
  std::unique_ptr<exec::DriverCtx> context_;
  std::unique_ptr<ColumnarExchangeInput> exchange_;
};

TEST_F(ColumnarExchangeCoalescingTest,
       ManyPagesCopyEachRowOnceAndRetainEarlierOutputOwners) {
  create(row(0)->rowType(), regularSchema());
  std::vector<std::unique_ptr<folly::IOBuf>> pages;
  for (int64_t index = 0; index < 128; ++index)
    pages.push_back(encode(row(index)));
  exchange_->supply(std::move(pages));
  std::vector<RowVectorPtr> retained;
  int64_t rows = 0;
  while (auto output = exchange_->getOutput()) {
    check(output, rows);
    rows += output->size();
    retained.push_back(std::move(output));
  }
  ASSERT_EQ(rows, 128);
  ASSERT_GE(retained.size(), 2);
  int64_t first = 0;
  for (const auto &output : retained) {
    check(output, first);
    first += output->size();
  }
  EXPECT_EQ(metric("columnarCoalesceInputBatches"), 128);
  EXPECT_EQ(metric("columnarCoalesceCopiedRows"), 128);
}

TEST_F(ColumnarExchangeCoalescingTest,
       OnePageKeepsItsImportedOwnerWithoutAConcatenationCopy) {
  create(row(9)->rowType(), regularSchema());
  std::vector<std::unique_ptr<folly::IOBuf>> pages;
  pages.push_back(encode(row(9)));
  exchange_->supply(std::move(pages));
  const auto output = exchange_->getOutput();
  ASSERT_NE(output, nullptr);
  check(output, 9);
  EXPECT_EQ(metric("columnarCoalesceInputBatches"), 1);
  EXPECT_EQ(metric("columnarCoalesceCopiedRows"), 0);
  EXPECT_EQ(exchange_->getOutput(), nullptr);
}

TEST_F(ColumnarExchangeCoalescingTest,
       LateMalformedPageDoesNotChangeAnAlreadyPublishedOutput) {
  create(row(11)->rowType(), regularSchema());
  std::vector<std::unique_ptr<folly::IOBuf>> original;
  original.push_back(encode(row(11)));
  exchange_->supply(std::move(original));
  const auto published = exchange_->getOutput();
  ASSERT_NE(published, nullptr);
  std::vector<std::unique_ptr<folly::IOBuf>> malformed;
  malformed.push_back(encode(row(12)));
  malformed.push_back(folly::IOBuf::copyBuffer("invalid canonical stream"));
  exchange_->supply(std::move(malformed));
  EXPECT_THROW(exchange_->getOutput(), VeloxException);
  EXPECT_EQ(exchange_->published(), published);
  check(published, 11);
  EXPECT_EQ(metric("columnarCoalesceInputBatches"), 1);
  EXPECT_EQ(metric("columnarCoalesceCopiedRows"), 0);
}

TEST_F(ColumnarExchangeCoalescingTest,
       ZeroColumnBatchesPreserveEveryLogicalRow) {
  const auto type = ROW(std::vector<std::string>{}, std::vector<TypePtr>{});
  create(type, schema::named({}));
  std::vector<std::unique_ptr<folly::IOBuf>> pages;
  for (vector_size_t size : {3, 5}) {
    pages.push_back(encode(std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size, std::vector<VectorPtr>{})));
  }
  exchange_->supply(std::move(pages));
  const auto output = exchange_->getOutput();
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->size(), 8);
  EXPECT_EQ(output->childrenSize(), 0);
  EXPECT_EQ(metric("columnarCoalesceInputBatches"), 2);
  EXPECT_EQ(metric("columnarCoalesceCopiedRows"), 8);
}
} // namespace
} // namespace pangrova::velox::test
