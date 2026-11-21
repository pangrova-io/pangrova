#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <vector>

#include "TestMemoryHelper.h"
#include "engine/velox/connector/PangrovaTableHandle.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "jni/RuntimeInitializer.h"
#include "velox/common/config/Config.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/Task.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
namespace sdk = facebook::velox;

TEST(HashJoinScanFilterTest, ActualBuildFiltersOnlyItsInnerProbeScan) {
  auto root = pangrova::test::testRootPool("HashJoinScanFilter");
  auto pool = root->addLeafChild("fixture");
  sdk::test::VectorMaker maker(pool.get());
  auto prefix = std::filesystem::temp_directory_path() /
      ("pangrova-join-filter-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  auto makeScan = [&](const std::string& name, const std::vector<int64_t>& keys) {
    auto input = maker.rowVector({"physical_key"}, {maker.flatVector<int64_t>(keys)});
    auto file = prefix.string() + "-" + name + ".parquet";
    auto properties = std::make_shared<const sdk::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    sdk::parquet::WriterOptions options;
    options.memoryPool = root.get();
    sdk::dwio::common::FileSink::Options sinkOptions{.connectorProperties = properties, .pool = pool.get()};
    sdk::parquet::Writer writer(
        std::make_unique<sdk::dwio::common::LocalFileSink>(file, sinkOptions), options, input->rowType());
    writer.write(input);
    writer.close();
    const auto connector = PangrovaVeloxConnector::kConnectorId;
    sdk::connector::ColumnHandleMap assignments;
    assignments.emplace(name, std::make_shared<PangrovaColumnHandle>("physical_key"));
    auto type = sdk::ROW({name}, {sdk::BIGINT()});
    auto nodeId = name + "_scan";
    auto handle = std::make_shared<PangrovaTableHandle>(connector, name, nodeId, input->rowType());
    return std::make_pair(file, std::make_shared<sdk::core::TableScanNode>(nodeId, type, handle, assignments));
  };
  auto probe = makeScan("probe", {1, 2, 3, 4});
  auto removeProbe = folly::makeGuard([&] { std::filesystem::remove(probe.first); });
  auto build = makeScan("build", {2, 4, 4});
  auto removeBuild = folly::makeGuard([&] { std::filesystem::remove(build.first); });
  for (auto joinType : {sdk::core::JoinType::kInner, sdk::core::JoinType::kLeft}) {
    using Field = sdk::core::FieldAccessTypedExpr;
    std::vector<sdk::core::FieldAccessTypedExprPtr> left{std::make_shared<Field>(sdk::BIGINT(), "probe")};
    std::vector<sdk::core::FieldAccessTypedExprPtr> right{std::make_shared<Field>(sdk::BIGINT(), "build")};
    auto join = std::make_shared<sdk::core::HashJoinNode>("bound_join", joinType, false,
        left, right, nullptr, probe.second, build.second, probe.second->outputType());
    std::vector<int64_t> output;
    std::mutex mutex;
    auto task = sdk::exec::Task::create("native-probe-" + std::to_string(static_cast<int>(joinType)),
        sdk::core::PlanFragment{join}, 0,
        sdk::core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
            sdk::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
                                   sdk::exec::FunctionRegistrySnapshot::capture())),
        sdk::exec::Task::ExecutionMode::kParallel,
        [&](sdk::RowVectorPtr batch, bool, sdk::ContinueFuture*) {
          if (batch) {
            sdk::DecodedVector decoded(*batch->childAt(0));
            std::lock_guard<std::mutex> lock(mutex);
            for (sdk::vector_size_t i = 0; i < batch->size(); ++i) output.push_back(decoded.valueAt<int64_t>(i));
          }
          return sdk::exec::BlockingReason::kNotBlocked;
        });
    auto release = folly::makeGuard([&] {
      task->requestCancel().wait();
      auto deleted = task->taskDeletionFuture();
      task.reset();
      deleted.wait();
    });
    for (const auto* side : {&probe, &build}) {
      split::SplitConfig split;
      split.connector_id = PangrovaVeloxConnector::kConnectorId;
      split.split_id = side->second->id();
      split.table_name = side->second->outputType()->nameOf(0);
      split.file_path = side->first;
      split.file_format = split::FileFormatType::PARQUET;
      split.length = std::filesystem::file_size(split.file_path);
      task->addSplit(split.split_id, sdk::exec::Split{
          std::make_shared<PangrovaConnectorSplit>(split.connector_id, split)});
      task->noMoreSplits(split.split_id);
    }
    task->start(2);
    task->taskCompletionFuture().wait();
    ASSERT_FALSE(task->error()) << task->errorMessage();
    std::sort(output.begin(), output.end());
    bool inner = joinType == sdk::core::JoinType::kInner;
    EXPECT_EQ(output, inner ? std::vector<int64_t>({2, 4, 4}) : std::vector<int64_t>({1, 2, 3, 4, 4}));
    size_t scanOperators = 0;
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        if (op.planNodeId != "probe_scan") continue;
        ++scanOperators;
        EXPECT_EQ(op.dynamicFilterStats.producerNodeIds.count("bound_join"), inner ? 1 : 0);
        EXPECT_EQ(op.outputPositions, inner ? 2 : 4);
      }
    }
    EXPECT_EQ(scanOperators, 1);
  }
}

}  // namespace
}  // namespace pangrova::velox::test
