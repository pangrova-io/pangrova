#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <folly/io/IOBuf.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "../TestMemoryHelper.h"
#include "jni/RuntimeInitializer.h"
#include "memory/ColumnarMemoryPool.h"
#include "velox/serde/ArrowBatchVectorSerde.h"
#include "velox/serde/CanonicalSerdeOptionsFactory.h"
#include "velox/plan/CanonicalPlanSchema.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/Task.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Pages = std::vector<std::unique_ptr<folly::IOBuf>>;
using Row = std::tuple<int64_t, std::optional<int64_t>, std::optional<std::string>>;
namespace schema = pangrova::velox::schema;

// Real SDK task/operator/output-buffer integration, not distributed SQL E2E.
class CanonicalPartitionedOutputExecutionTest : public ::testing::Test {
 protected:
  struct Response {
    std::mutex mutex;
    std::condition_variable changed;
    std::exception_ptr error;
    std::optional<Pages> pages;
    int64_t sequence{-1};
  };

  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("canonical-partitioned-output");
    buffers_ = exec::OutputBufferManager::getInstanceRef();
    response_ = std::make_shared<Response>();
  }

  void TearDown() override {
    if (task_) {
      auto joined = task_->requestCancel().via(pangrova::jni::globalExecutor().get());
      joined.wait(std::chrono::seconds(10));
      ASSERT_TRUE(joined.isReady()) << task_->toString();
      std::move(joined).get();
      buffers_->removeTask(task_->taskId());
      task_.reset();
    }
  }

  RowVectorPtr input(std::vector<Row>& rows) {
    std::vector<int64_t> ids;
    std::vector<std::optional<int64_t>> keys;
    std::vector<std::optional<std::string>> texts;
    for (int64_t row = 0; row < 97; ++row) {
      ids.push_back(row);
      keys.push_back(row % 7 == 0 ? std::nullopt : std::optional<int64_t>(row % 19 - 9));
      texts.push_back(row % 11 == 0 ? std::nullopt :
          std::optional<std::string>("重复-" + std::to_string(row % 5)));
      rows.emplace_back(ids.back(), keys.back(), texts.back());
    }
    facebook::velox::test::VectorMaker maker(pool_.get());
    return maker.rowVector({"row_id", "key", "text"}, {
        maker.flatVector<int64_t>(ids), maker.flatVectorNullable<int64_t>(keys),
        maker.flatVectorNullable<std::string>(texts)});
  }

  void start(const RowVectorPtr& values, uint32_t drivers, int partitions) {
    auto source = std::make_shared<core::ValuesNode>("values",
        std::vector<RowVectorPtr>{values}, drivers > 1);
    auto hash = std::make_shared<exec::HashPartitionFunctionSpec>(
        values->rowType(), std::vector<column_index_t>{1});
    auto output = std::make_shared<core::PartitionedOutputNode>("output",
        core::PartitionedOutputNode::Kind::kPartitioned,
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "key")},
        partitions, false, hash, values->rowType(), "ArrowBatch", source);
    static std::atomic<uint64_t> next_id{0};
    const auto declared = schema::named({
        schema::computed("row_id", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED)),
        schema::computed("key", schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE)),
        schema::computed("text", schema::primitive("string", substrait::Type::NULLABILITY_NULLABLE))});
    const auto serde = std::make_shared<const CanonicalSerdeOptionsFactory>(
        CanonicalSerdeOptionsFactory::SchemaBindings{{output->id(), declared}}, 256LL * 1024 * 1024);
    auto query = core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
        facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"},
            {"require_vector_serde_options_factory", "true"}},
            facebook::velox::exec::FunctionRegistrySnapshot::capture(), serde));
    task_ = exec::Task::create("canonical-partitioned-output-" +
        std::to_string(next_id.fetch_add(1)), core::PlanFragment{output}, 0,
        std::move(query), exec::Task::ExecutionMode::kParallel,
        exec::Consumer{}, 0, std::nullopt,
        [response = response_](std::exception_ptr error) {
          {
            std::lock_guard<std::mutex> lock(response->mutex);
            response->error = std::move(error);
          }
          response->changed.notify_all();
        });
    task_->start(drivers);
  }

  Pages read(int partition, int64_t sequence) {
    {
      std::lock_guard<std::mutex> lock(response_->mutex);
      response_->pages.reset();
    }
    const bool accepted = buffers_->getData(task_->taskId(), partition, 4 * 1024 * 1024,
        sequence, [response = response_](Pages pages, int64_t actual_sequence,
                      std::vector<int64_t>) {
          {
            std::lock_guard<std::mutex> lock(response->mutex);
            response->sequence = actual_sequence;
            response->pages.emplace(std::move(pages));
          }
          response->changed.notify_all();
        });
    std::unique_lock<std::mutex> lock(response_->mutex);
    if (!accepted) {
      if (response_->error) std::rethrow_exception(response_->error);
      VELOX_FAIL("Actual Task output buffer was not initialized: {}", task_->errorMessage());
    }
    VELOX_CHECK(response_->changed.wait_for(lock, std::chrono::seconds(10), [&] {
      return response_->pages.has_value() || response_->error;
    }), "Actual Task output did not arrive: {}", task_->errorMessage());
    if (response_->error) std::rethrow_exception(response_->error);
    VELOX_CHECK_EQ(response_->sequence, sequence, "Actual output sequence must be contiguous");
    return std::move(*response_->pages);
  }

  void decode(folly::IOBuf& page, std::multiset<Row>& rows) {
    page.coalesce();
    auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.memory_pool = allocator.get();
    options.use_threads = false;
    auto input = std::make_shared<arrow::io::BufferReader>(
        std::make_shared<arrow::Buffer>(page.data(), page.length()));
    auto opened = arrow::ipc::RecordBatchStreamReader::Open(input, options);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    auto reader = *opened;
    ASSERT_EQ(reader->schema()->field_names(),
        (std::vector<std::string>{"row_id", "key", "text"}));
    EXPECT_EQ(reader->schema()->field(0)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(reader->schema()->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(reader->schema()->field(2)->type()->id(), arrow::Type::STRING);
    EXPECT_FALSE(reader->schema()->field(0)->nullable());
    EXPECT_TRUE(reader->schema()->field(1)->nullable());
    EXPECT_TRUE(reader->schema()->field(2)->nullable());
    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      const auto status = reader->ReadNext(&batch);
      ASSERT_TRUE(status.ok()) << status.ToString();
      if (!batch) break;
      ASSERT_TRUE(batch->ValidateFull().ok());
      auto ids = std::dynamic_pointer_cast<arrow::Int64Array>(batch->column(0));
      auto keys = std::dynamic_pointer_cast<arrow::Int64Array>(batch->column(1));
      auto texts = std::dynamic_pointer_cast<arrow::StringArray>(batch->column(2));
      ASSERT_NE(ids, nullptr);
      ASSERT_NE(keys, nullptr);
      ASSERT_NE(texts, nullptr);
      for (int64_t row = 0; row < batch->num_rows(); ++row) {
        rows.emplace(ids->Value(row), keys->IsNull(row) ? std::nullopt :
            std::optional<int64_t>(keys->Value(row)), texts->IsNull(row) ? std::nullopt :
            std::optional<std::string>(texts->GetString(row)));
      }
    }
    auto consumed = input->Tell();
    ASSERT_TRUE(consumed.ok());
    EXPECT_EQ(*consumed, static_cast<int64_t>(page.length()));
  }

  std::multiset<Row> consume(int partition) {
    std::multiset<Row> rows;
    int64_t sequence = 0;
    for (int request = 0; request < 100; ++request) {
      auto pages = read(partition, sequence);
      for (auto& page : pages) {
        if (!page) {
          buffers_->deleteResults(task_->taskId(), partition);
          return rows;
        }
        decode(*page, rows);
        ++sequence;
      }
    }
    VELOX_FAIL("Actual output buffer did not produce EOF");
  }

  void checkCompletion(uint32_t drivers) {
    auto completion = task_->taskCompletionFuture().via(pangrova::jni::globalExecutor().get());
    completion.wait(std::chrono::seconds(10));
    ASSERT_TRUE(completion.isReady()) << task_->toString();
    std::move(completion).get();
    ASSERT_FALSE(task_->error()) << task_->errorMessage();
    EXPECT_EQ(task_->state(), exec::TaskState::kFinished);
    EXPECT_EQ(task_->numRunningDrivers(), 0);
    const auto stats = task_->taskStats();
    EXPECT_EQ(stats.numTotalDrivers, drivers);
    bool found = false;
    for (const auto& pipeline : stats.pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        const auto metric = op.runtimeStats.find(std::string(exec::Operator::kShuffleSerdeKind));
        if (metric == op.runtimeStats.end()) continue;
        found = true;
        EXPECT_EQ(metric->second.count, drivers);
        EXPECT_EQ(VectorSerde::kindName(static_cast<VectorSerde::Kind>(metric->second.min)), "ArrowBatch");
        EXPECT_EQ(metric->second.min, metric->second.max);
      }
    }
    EXPECT_TRUE(found) << "Actual finished operator must report its real wire kind";
  }

  void execute(uint32_t drivers) {
    constexpr int partitions = 4;
    std::vector<Row> original;
    const auto values = input(original);
    exec::HashPartitionFunctionSpec spec(values->rowType(), {1});
    auto hash = spec.create(partitions, false);
    std::vector<uint32_t> destinations(values->size());
    const auto single = hash->partition(*values, destinations);
    ASSERT_FALSE(single.has_value());
    std::vector<std::multiset<Row>> expected(partitions);
    bool sparse = false;
    for (int partition = 0; partition < partitions; ++partition) {
      int64_t previous = -1;
      for (int64_t row = 0; row < values->size(); ++row) {
        if (destinations[row] != static_cast<uint32_t>(partition)) continue;
        sparse |= previous >= 0 && row > previous + 1;
        previous = row;
        for (uint32_t driver = 0; driver < drivers; ++driver) expected[partition].insert(original[row]);
      }
      ASSERT_FALSE(expected[partition].empty());
    }
    ASSERT_TRUE(sparse) << "Fixture must exercise non-contiguous row append";
    // SDK parallelizable Values replicates the real input per driver; the
    // expected multiset explicitly preserves those duplicates, not fake nodes.
    start(values, drivers, partitions);
    for (int partition = 0; partition < partitions; ++partition) {
      EXPECT_EQ(consume(partition), expected[partition]) << partition;
    }
    checkCompletion(drivers);
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<exec::OutputBufferManager> buffers_;
  std::shared_ptr<Response> response_;
  std::shared_ptr<exec::Task> task_;
};

TEST_F(CanonicalPartitionedOutputExecutionTest, SparseHashPartitionRowsNullsAndDuplicates) {
  execute(1);
}

TEST_F(CanonicalPartitionedOutputExecutionTest, TwoActualDriversPreserveEveryPartitionAndFinishStats) {
  execute(2);
}

TEST_F(CanonicalPartitionedOutputExecutionTest, ActualSdkWireKindHasACompleteNameRoundTrip) {
  const auto kind = VectorSerde::kindByName("ArrowBatch");
  EXPECT_EQ(VectorSerde::kindName(kind), "ArrowBatch");
}
}  // namespace
}  // namespace pangrova::velox::test
