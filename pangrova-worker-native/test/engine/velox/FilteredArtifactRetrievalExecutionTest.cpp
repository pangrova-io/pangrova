// Native component oracle: real Parquet file + built Vamana artifacts + actual
// TableScan/Filter/filtered owner/hydration in one Task. SQL/JNI/Flight acceptance
// is separate; these small inputs are not performance measurements.
#include <gtest/gtest.h>
#include <openssl/evp.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <atomic>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "TestMemoryHelper.h"
#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "capabilities/artifact/RowLocatorSidecar.h"
#include "jni/RuntimeInitializer.h"
#include "engine/resource/TaskObjectReadBudget.h"
#include "vector/diskann/DiskAnnBuilder.h"
#include "vector/diskann/DiskAnnIndexCache.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "engine/velox/plan/FilteredArtifactRetrievalOperator.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/common/config/Config.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/Task.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
namespace sdk = facebook::velox;
namespace proto = protocol::v2;

std::string sha256(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  std::string bytes((std::istreambuf_iterator<char>(input)), {});
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int length = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), digest.data(), &length, EVP_sha256(), nullptr) != 1
      || length != 32) throw std::runtime_error("Actual immutable file digest failed");
  return {reinterpret_cast<const char*>(digest.data()), length};
}

class FilteredArtifactRetrievalExecutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = pangrova::test::testRootPool("FilteredRetrievalExecution");
    leaf_ = root_->addLeafChild("fixture");
    directory_ = std::filesystem::temp_directory_path() / ("pangrova-filtered-task-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(directory_);
    artifact::ImmutableArtifactCache::instance().shutdown();
    artifact::ImmutableArtifactCache::instance().configure((directory_ / "cache").string(), 4 * 1024 * 1024);
    if (vector::diskann::DiskAnnIndexCache::instance().enabled()) {
      vector::diskann::DiskAnnIndexCache::instance().shutdown();
    }
    vector::diskann::DiskAnnIndexCache::instance().configure(4 * 1024 * 1024);
    writeSource();
    buildGraph();
  }
  void TearDown() override {
    vector::diskann::DiskAnnIndexCache::instance().shutdown();
    artifact::ImmutableArtifactCache::instance().shutdown();
    std::filesystem::remove_all(directory_);
    leaf_.reset();
    root_.reset();
  }
  void writeSource() {
    sdk::test::VectorMaker maker(leaf_.get());
    auto category = maker.flatVectorNullable<std::string>({std::nullopt, "drop", "keep", "keep"});
    ASSERT_TRUE(category->isNullAt(0));
    ASSERT_EQ(category->getNullCount(), 1);  // Parquet's Arrow export consumes this cached count.
    auto data = maker.rowVector({"id", "category", "embedding"}, {
        maker.flatVector<int64_t>({101, 102, 103, 104}), category,
        maker.arrayVector<float>({{0, 0, 0, 0}, {1, 0, 0, 0}, {2, 0, 0, 0}, {3, 0, 0, 0}})});
    schema_ = std::dynamic_pointer_cast<const sdk::RowType>(data->type());
    source_ = directory_ / "source.parquet";
    sdk::parquet::WriterOptions options;
    options.memoryPool = root_.get();
    options.enableDictionary = true;
    const std::shared_ptr<const sdk::config::ConfigBase> connector_properties =
        std::make_shared<const sdk::config::ConfigBase>(std::unordered_map<std::string, std::string>{});
    sdk::dwio::common::FileSink::Options sink_options{
        .connectorProperties = connector_properties, .pool = leaf_.get()};
    sdk::parquet::Writer writer(
        std::make_unique<sdk::dwio::common::LocalFileSink>(source_.string(), sink_options), options, schema_);
    writer.write(data);
    writer.close();
    ASSERT_GT(std::filesystem::file_size(source_), 0);
  }
  void buildGraph() {
    const std::array<float, 16> vectors{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0};
    vector::diskann::VamanaConfig options;
    options.dimension = 4;
    options.maxDegree = 3;
    options.searchListSize = 4;
    options.pqSubvectors = 2;
    options.pqBitsPerCode = 2;
    options.beamWidth = 2;
    auto graph = directory_ / "source.graph";
    vector::diskann::DiskAnnBuilder(options).buildFromVectors(vectors.data(), 4, 4, graph.string());
    auto locators = directory_ / "source.locators";
    artifact::RowLocatorSidecar::writePhysicalRowPositions(locators.string(), std::array<uint64_t, 4>{0, 1, 2, 3});
    artifact_.set_source_id("table");
    artifact_.set_source_snapshot("snapshot");
    addArtifact(graph, "DISKANN_GRAPH", 0);
    addArtifact(graph.string() + ".vecs", "RAW_VECTORS", 1);
    addArtifact(locators, "ROW_LOCATORS", 2);
  }
  void addArtifact(const std::filesystem::path& path, const std::string& role, uint32_t ordinal) {
    auto* object = artifact_.add_split_objects();
    object->set_object_id("object:" + role);
    object->set_source_object_id("object");
    object->set_scope(proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
    object->set_object_role(role);
    object->set_ordinal(ordinal);
    object->set_row_count(4);
    object->set_size_bytes(std::filesystem::file_size(path));
    object->set_opaque_uri(path.string());
    object->mutable_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    object->mutable_checksum()->set_digest(sha256(path));
  }
  split::SplitConfig sourceSplit() {
    split::SplitConfig split;
    split.split_id = "immutable-source";
    split.connector_id = PangrovaVeloxConnector::kConnectorId;
    split.table_name = "table";
    split.file_path = source_.string();
    split.file_format = split::FileFormatType::PARQUET;
    split.length = std::filesystem::file_size(source_);
    split.source_snapshot = "snapshot";
    split.data_object_id = "object";
    return split;
  }
  sdk::core::PlanNodePtr plan(const std::string& accepted,
      const std::shared_ptr<resource::TaskObjectReadBudget>& budget) {
    auto names = schema_->names();
    auto types = schema_->children();
    const std::array<std::string, 4> identities{std::string(kSourceIdColumnName),
        std::string(kSourceSnapshotColumnName), std::string(kSourceObjectIdColumnName),
        std::string(kPhysicalRowPositionColumnName)};
    for (size_t index = 0; index < identities.size(); ++index) {
      names.push_back(identities[index]);
      types.push_back(index == 3 ? sdk::TypePtr(sdk::BIGINT()) : sdk::TypePtr(sdk::VARCHAR()));
    }
    auto output = sdk::ROW(names, types);
    auto handle = std::make_shared<const PangrovaTableHandle>(
        PangrovaVeloxConnector::kConnectorId, "table", "qualify", output, nullptr, budget);
    sdk::connector::ColumnHandleMap hydration_assignments;
    for (const auto& name : names) hydration_assignments.emplace(name, std::make_shared<PangrovaColumnHandle>(name));
    auto hydration = std::make_shared<sdk::core::TableScanNode>("hydrate", output, handle, hydration_assignments);
    sdk::connector::ColumnHandleMap qualification_assignments;
    qualification_assignments.emplace("category", hydration_assignments.at("category"));
    std::vector<std::string> qualification_names{"category"};
    std::vector<sdk::TypePtr> qualification_types{sdk::VARCHAR()};
    std::vector<sdk::core::TypedExprPtr> projections;
    for (size_t index = 0; index < identities.size(); ++index) {
      qualification_names.push_back(identities[index]);
      qualification_types.push_back(index == 3 ? sdk::TypePtr(sdk::BIGINT()) : sdk::TypePtr(sdk::VARCHAR()));
      qualification_assignments.emplace(identities[index], hydration_assignments.at(identities[index]));
      projections.push_back(std::make_shared<sdk::core::FieldAccessTypedExpr>(qualification_types.back(), identities[index]));
    }
    auto scan = std::make_shared<sdk::core::TableScanNode>("qualify",
        sdk::ROW(qualification_names, qualification_types), handle, qualification_assignments);
    auto predicate = std::make_shared<sdk::core::CallTypedExpr>(sdk::BOOLEAN(),
        std::vector<sdk::core::TypedExprPtr>{std::make_shared<sdk::core::FieldAccessTypedExpr>(sdk::VARCHAR(), "category"),
            std::make_shared<sdk::core::ConstantTypedExpr>(sdk::VARCHAR(), sdk::variant(accepted))}, "eq");
    auto filtered = std::make_shared<sdk::core::FilterNode>("actual-sql-filter", predicate, scan);
    auto identity = std::make_shared<sdk::core::ProjectNode>("eligible-identities",
        std::vector<std::string>(identities.begin(), identities.end()), projections, filtered);
    FilteredArtifactRetrievalConfig config;
    config.artifact = artifact_;
    config.query = {0.25f, 0, 0, 0};
    config.output_limit = 3;
    config.search_list_size = 4;
    config.max_work = 4;
    config.max_read_bytes = 16 * 1024 * 1024;
    config.read_budget = budget;
    config.splits = {sourceSplit()};
    config.hydration_scan = hydration;
    return std::make_shared<FilteredArtifactRetrievalNode>("filtered-owner", std::move(config),
        sdk::core::LocalPartitionNode::gather("eligibility-gather", {identity}));
  }
  void execute(const std::string& category, const std::vector<int64_t>& expected) {
    auto budget = std::make_shared<resource::TaskObjectReadBudget>(16 * 1024 * 1024);
    std::mutex mutex;
    std::vector<int64_t> rows;
    auto task = sdk::exec::Task::create("actual-filtered-parquet", sdk::core::PlanFragment{plan(category, budget)}, 0,
        sdk::core::QueryCtx::create(pangrova::jni::globalExecutor().get(), sdk::core::QueryConfig(
            {{"require_function_registry_snapshot", "true"}}, sdk::exec::FunctionRegistrySnapshot::capture())),
        sdk::exec::Task::ExecutionMode::kParallel,
        [&](sdk::RowVectorPtr batch, bool, sdk::ContinueFuture*) {
          if (batch) {
            sdk::DecodedVector ids(*batch->childAt(0));
            std::lock_guard<std::mutex> lock(mutex);
            for (sdk::vector_size_t row = 0; row < batch->size(); ++row) rows.push_back(ids.valueAt<int64_t>(row));
          }
          return sdk::exec::BlockingReason::kNotBlocked;
        });
    auto release = folly::makeGuard([&] {
      budget->requestCancel();
      task->requestCancel().wait();
      budget->stopAndDrain();
      auto deleted = task->taskDeletionFuture();
      task.reset();
      deleted.wait();
    });
    task->addSplit("qualify", sdk::exec::Split{
        std::make_shared<PangrovaConnectorSplit>(PangrovaVeloxConnector::kConnectorId, sourceSplit())});
    task->noMoreSplits("qualify");
    task->start(2);
    task->taskCompletionFuture().wait();
    ASSERT_FALSE(task->error()) << task->errorMessage();
    ASSERT_EQ(task->state(), sdk::exec::TaskState::kFinished);
    std::sort(rows.begin(), rows.end());
    EXPECT_EQ(rows, expected);
    size_t owners = 0;
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& operation : pipeline.operatorStats) {
        if (operation.operatorType == "FilteredArtifactRetrieval") {
          ++owners;
          EXPECT_EQ(operation.planNodeId, "filtered-owner");
          EXPECT_EQ(operation.inputPositions, expected.size());
          EXPECT_EQ(operation.outputPositions, expected.size());
          const auto expanded = operation.runtimeStats.find("filteredAnnExpandedNodes");
          const auto rescored = operation.runtimeStats.find("filteredAnnRescoredRows");
          ASSERT_NE(expanded, operation.runtimeStats.end());
          ASSERT_NE(rescored, operation.runtimeStats.end());
          EXPECT_EQ(expanded->second.sum, expected.empty() ? 0 : 4);
          EXPECT_EQ(rescored->second.sum, expected.size());
        }
      }
    }
    EXPECT_EQ(owners, 1);
    uint64_t cold_artifact_bytes = 0;
    for (const auto& object : artifact_.split_objects()) cold_artifact_bytes += object.size_bytes();
    EXPECT_GT(budget->requestedBytes(), cold_artifact_bytes);  // Actual source scan uses the same owner.
    EXPECT_EQ(budget->activeReads(), 0);
  }
  void rejectActualFault(uint64_t grant) {
    auto budget = std::make_shared<resource::TaskObjectReadBudget>(grant);
    std::atomic<size_t> delivered{0};
    {
      auto task = sdk::exec::Task::create("failed-filtered-parquet", sdk::core::PlanFragment{plan("keep", budget)}, 0,
          sdk::core::QueryCtx::create(pangrova::jni::globalExecutor().get(), sdk::core::QueryConfig(
              {{"require_function_registry_snapshot", "true"}}, sdk::exec::FunctionRegistrySnapshot::capture())),
          sdk::exec::Task::ExecutionMode::kParallel,
          [&](sdk::RowVectorPtr batch, bool, sdk::ContinueFuture*) {
            if (batch) delivered.fetch_add(batch->size());
            return sdk::exec::BlockingReason::kNotBlocked;
          });
      // The failed task can publish its state while another driver is still
      // releasing a read. Inspect resource ownership after the real drain.
      auto release = folly::makeGuard([&] {
        budget->requestCancel();
        task->requestCancel().wait();
        budget->stopAndDrain();
        auto deleted = task->taskDeletionFuture();
        task.reset();
        deleted.wait();
      });
      task->addSplit("qualify", sdk::exec::Split{
          std::make_shared<PangrovaConnectorSplit>(PangrovaVeloxConnector::kConnectorId, sourceSplit())});
      task->noMoreSplits("qualify");
      task->start(2);
      task->taskCompletionFuture().wait();
      ASSERT_TRUE(task->error());
      ASSERT_EQ(task->state(), sdk::exec::TaskState::kFailed);
    }
    EXPECT_EQ(delivered.load(), 0);
    EXPECT_LE(budget->requestedBytes(), grant);
    EXPECT_EQ(budget->activeReads(), 0);
  }
  void executeCandidateScan(bool output_position, bool materialized_score) {
    auto budget = std::make_shared<resource::TaskObjectReadBudget>(16 * 1024 * 1024);
    std::vector<std::string> names{"id"};
    std::vector<sdk::TypePtr> types{sdk::BIGINT()};
    if (materialized_score) {
      names.emplace_back(kMaterializedRetrievalScoreColumnName);
      types.push_back(sdk::DOUBLE());
    }
    if (output_position) {
      names.emplace_back(kPhysicalRowPositionColumnName);
      types.push_back(sdk::BIGINT());
    }
    names.emplace_back("category");
    types.push_back(sdk::VARCHAR());
    auto output = sdk::ROW(names, types);
    auto handle = std::make_shared<const PangrovaTableHandle>(
        PangrovaVeloxConnector::kConnectorId, "table", "candidate-scan", output, nullptr, budget);
    sdk::connector::ColumnHandleMap assignments;
    for (const auto& name : names) assignments.emplace(name, std::make_shared<PangrovaColumnHandle>(name));
    auto scan = std::make_shared<sdk::core::TableScanNode>("candidate-scan", output, handle, assignments);
    struct ActualRow {
      int64_t id;
      bool category_null;
      std::string category;
      int64_t position;
      double score;
    };
    std::mutex mutex;
    std::vector<ActualRow> rows;
    auto task = sdk::exec::Task::create("actual-candidate-parquet", sdk::core::PlanFragment{scan}, 0,
        sdk::core::QueryCtx::create(pangrova::jni::globalExecutor().get(), sdk::core::QueryConfig(
            {{"require_function_registry_snapshot", "true"}}, sdk::exec::FunctionRegistrySnapshot::capture())),
        sdk::exec::Task::ExecutionMode::kParallel,
        [&](sdk::RowVectorPtr batch, bool, sdk::ContinueFuture*) {
          if (batch) {
            sdk::DecodedVector ids(*batch->childAt(output->getChildIdx("id")));
            sdk::DecodedVector categories(*batch->childAt(output->getChildIdx("category")));
            std::lock_guard<std::mutex> lock(mutex);
            for (sdk::vector_size_t row = 0; row < batch->size(); ++row) {
              ActualRow actual{ids.valueAt<int64_t>(row), categories.isNullAt(row), "", -1, 0};
              if (!actual.category_null) actual.category = categories.valueAt<sdk::StringView>(row).str();
              if (output_position) {
                sdk::DecodedVector positions(*batch->childAt(output->getChildIdx(kPhysicalRowPositionColumnName)));
                VELOX_CHECK(!positions.isNullAt(row));
                actual.position = positions.valueAt<int64_t>(row);
              }
              if (materialized_score) {
                sdk::DecodedVector scores(*batch->childAt(output->getChildIdx(kMaterializedRetrievalScoreColumnName)));
                VELOX_CHECK(!scores.isNullAt(row));
                actual.score = scores.valueAt<double>(row);
              }
              rows.push_back(std::move(actual));
            }
          }
          return sdk::exec::BlockingReason::kNotBlocked;
        });
    auto release = folly::makeGuard([&] {
      budget->requestCancel();
      task->requestCancel().wait();
      budget->stopAndDrain();
      auto deleted = task->taskDeletionFuture();
      task.reset();
      deleted.wait();
    });
    auto config = sourceSplit();
    config.row_position_filter_enabled = true;
    config.row_position_allowlist = {0, 3};
    config.materialized_retrieval_score_enabled = materialized_score;
    if (materialized_score) config.materialized_retrieval_scores = {{0, 1.25}, {3, 9.5}};
    task->addSplit("candidate-scan", sdk::exec::Split{
        std::make_shared<PangrovaConnectorSplit>(PangrovaVeloxConnector::kConnectorId, std::move(config))});
    task->noMoreSplits("candidate-scan");
    task->start(1);
    task->taskCompletionFuture().wait();
    ASSERT_FALSE(task->error()) << task->errorMessage();
    ASSERT_EQ(task->state(), sdk::exec::TaskState::kFinished);
    std::sort(rows.begin(), rows.end(), [](const auto& left, const auto& right) { return left.id < right.id; });
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0].id, 101);
    EXPECT_EQ(rows[1].id, 104);
    EXPECT_TRUE(rows[0].category_null);
    EXPECT_FALSE(rows[1].category_null);
    EXPECT_EQ(rows[1].category, "keep");
    if (output_position) {
      EXPECT_EQ(rows[0].position, 0);
      EXPECT_EQ(rows[1].position, 3);  // Original DWIO positions, not selected-row ordinals.
    }
    if (materialized_score) {
      EXPECT_DOUBLE_EQ(rows[0].score, 1.25);
      EXPECT_DOUBLE_EQ(rows[1].score, 9.5);
    }
    EXPECT_GT(budget->requestedBytes(), 0);
    EXPECT_EQ(budget->activeReads(), 0);
  }
  std::filesystem::path directory_;
  std::filesystem::path source_;
  sdk::RowTypePtr schema_;
  proto::ArtifactBinding artifact_;
  std::shared_ptr<sdk::memory::MemoryPool> root_;
  std::shared_ptr<sdk::memory::MemoryPool> leaf_;
};

TEST_F(FilteredArtifactRetrievalExecutionTest, ActualDictionaryParquetFilterAndHydrationPreserveEligibleRows) {
  execute("keep", {103, 104});
}

TEST_F(FilteredArtifactRetrievalExecutionTest, ActualEmptyFilterFinishesItsOneLiveOwnerWithoutHydrationRows) {
  execute("absent", {});
}

TEST_F(FilteredArtifactRetrievalExecutionTest, ExhaustedActualTaskReadGrantFailsWithoutAlternativeRows) {
  rejectActualFault(1);
}

TEST_F(FilteredArtifactRetrievalExecutionTest, CorruptedColdImmutableObjectFailsTheActualNativeTask) {
  std::fstream raw(artifact_.split_objects(1).opaque_uri(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(raw.good());
  raw.put(static_cast<char>(0xff));
  raw.close();
  ASSERT_TRUE(raw.good());
  // Keep the original expected digest; corrupt the real object on disk before
  // its first cold materialization, rather than fabricate a failure receipt.
  rejectActualFault(16 * 1024 * 1024);
}

TEST_F(FilteredArtifactRetrievalExecutionTest, ActualParquetCandidateScanKeepsRequestedPositionInMiddle) {
  executeCandidateScan(true, false);
}

TEST_F(FilteredArtifactRetrievalExecutionTest, ActualParquetCandidateScanOmitsInternalOnlyPosition) {
  executeCandidateScan(false, false);
}

TEST_F(FilteredArtifactRetrievalExecutionTest, ActualParquetCandidateScanPreservesPositionAndMaterializedScore) {
  executeCandidateScan(true, true);
}

TEST_F(FilteredArtifactRetrievalExecutionTest, ActualParquetCandidateScanMaterializesScoreWithoutOutputPosition) {
  executeCandidateScan(false, true);
}

}  // namespace
}  // namespace pangrova::velox::test
