#include <arrow/io/file.h>
#include "velox/expression/FunctionRegistrySnapshot.h"
#include <arrow/ipc/reader.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../TestMemoryHelper.h"
#include "artifact/ArtifactWorkReportState.h"
#include "artifact/GeneratedValueSegment.h"
#include "artifact/ImmutableArtifactCache.h"
#include "artifact/ImmutableArtifactWriter.h"
#include "artifact/RowLocatorSidecar.h"
#include "capability/NativeCapabilityCatalog.h"
#include "jni/RuntimeInitializer.h"
#include "text/bm25/Bm25Analyzer.h"
#include "text/bm25/Bm25CorpusStats.h"
#include "text/bm25/Bm25Segment.h"
#include "vector/hnsw/HnswIndex.h"
#include "vector/ivfpq/IvfPqIndex.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

class TestDirectory final {
 public:
  TestDirectory() {
    path_ = std::filesystem::temp_directory_path() /
            ("pangrova-artifact-execution-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(path_);
  }
  ~TestDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

class ArtifactCacheGuard final {
 public:
  explicit ArtifactCacheGuard(const std::filesystem::path& root) {
    if (!artifact::ImmutableArtifactCache::instance().enabled()) {
      artifact::ImmutableArtifactCache::instance().configure(root.string(),
                                                             8 * 1024 * 1024);
      owns_cache_ = true;
    }
  }

  ~ArtifactCacheGuard() {
    if (owns_cache_) {
      artifact::ImmutableArtifactCache::instance().shutdown();
    }
  }

 private:
  bool owns_cache_{false};
};

class ArtifactBuildExecutionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("ArtifactBuildExecutionTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    ArtifactBuildExecutionTest::pool_;

TEST_F(ArtifactBuildExecutionTest, BuildsImmutableDiskAnnWorkPageAndReport) {
  TestDirectory directory;
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto input = maker.rowVector(
      {std::string(kPhysicalRowPositionColumnName), "embedding"},
      {maker.flatVector<int64_t>({0, 1, 2, 3}),
       maker.arrayVector<float>({{1.0F, 0.0F, 0.0F, 0.0F},
                                 {0.0F, 1.0F, 0.0F, 0.0F},
                                 {0.0F, 0.0F, 1.0F, 0.0F},
                                 {0.0F, 0.0F, 0.0F, 1.0F}})});
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "artifact-values", std::vector<facebook::velox::RowVectorPtr>{input},
      true);

  ArtifactBuildConfig config;
  config.output.set_artifact_id("index-1");
  config.output.set_artifact_type("VECTOR_INDEX");
  config.output.set_generation("7");
  config.output.set_source_id("catalog.schema.items");
  config.output.set_source_snapshot("snapshot-7");
  config.output.set_staging_uri((directory.path() / "objects").string());
  *config.output.mutable_format() = capability::diskAnnArtifactFormat();
  *config.output.mutable_row_locator() =
      capability::physicalRowLocatorContract();
  config.output.add_expected_source_object_ids("source-file-1");
  config.output.set_publication_fencing_token(13);
  config.output.set_work_unit_id("page-1");
  config.output.set_idempotency_key("job-1:page-1:generation-7");
  config.task.job_id = "job-1";
  config.task.page_id = "page-1";
  config.task.fencing_token = 13;
  config.task.scratch_directory = (directory.path() / "scratch").string();
  config.task.max_input_bytes = 8 * 1024 * 1024;
  config.task.max_spill_bytes = 8 * 1024 * 1024;
  config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
  config.task.report_state =
      std::make_shared<artifact::ArtifactWorkReportState>();
  config.row_position_channel = 0;
  config.vector_channel = 1;
  config.max_rows = 100;
  config.diskann.dimension = 4;
  config.diskann.maxDegree = 2;
  config.diskann.searchListSize = 4;
  config.diskann.pqSubvectors = 1;
  config.diskann.pqBitsPerCode = 2;
  config.diskann.beamWidth = 2;

  auto sink =
      std::make_shared<ArtifactBuildNode>("artifact-build-1", config, values);
  auto query_context = facebook::velox::core::QueryCtx::create(
      pangrova::jni::globalExecutor().get(),
      facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
          facebook::velox::exec::FunctionRegistrySnapshot::capture()));
  auto task = facebook::velox::exec::Task::create(
      "artifact-build-execution-test",
      facebook::velox::core::PlanFragment{sink}, 0, std::move(query_context),
      facebook::velox::exec::Task::ExecutionMode::kParallel,
      [](facebook::velox::RowVectorPtr, bool,
         facebook::velox::ContinueFuture*) {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      });
  task->start(2);
  task->taskCompletionFuture().wait();
  ASSERT_FALSE(task->error()) << task->errorMessage();
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (task->numRunningDrivers() != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(task->numRunningDrivers(), 0);

  auto report = config.task.report_state->snapshot();
  ASSERT_TRUE(report.has_value());
  EXPECT_EQ(report->job_id(), "job-1");
  EXPECT_EQ(report->page_id(), "page-1");
  EXPECT_EQ(report->fencing_token(), 13);
  ASSERT_EQ(report->outputs_size(), 3);
  EXPECT_EQ(report->outputs(0).object_role(), "DISKANN_GRAPH");
  EXPECT_EQ(report->outputs(1).object_role(), "RAW_VECTORS");
  EXPECT_EQ(report->outputs(2).object_role(), "ROW_LOCATORS");
  for (const auto& output : report->outputs()) {
    EXPECT_TRUE(std::filesystem::exists(output.opaque_uri()));
    EXPECT_EQ(std::filesystem::file_size(output.opaque_uri()),
              output.size_bytes());
    EXPECT_EQ(output.checksum().digest().size(), 32);
    EXPECT_EQ(output.row_count(), 4);
  }
  EXPECT_EQ(artifact::RowLocatorSidecar::readPhysicalRowPositions(
                report->outputs(2).opaque_uri(), 4),
            (std::vector<uint64_t>{0, 1, 2, 3}));
  EXPECT_EQ(report->report_checksum().digest().size(), 32);
  EXPECT_GT(report->usage().object_store_write_bytes(), 0);
}

TEST_F(ArtifactBuildExecutionTest, BuildsImmutableHnswAndIvfPqWorkPages) {
  for (const bool hnsw : {true, false}) {
    for (const auto metric :
         {vector::DistanceMetric::L2, vector::DistanceMetric::COSINE,
          vector::DistanceMetric::INNER_PRODUCT}) {
      TestDirectory directory;
      facebook::velox::test::VectorMaker maker(pool_.get());
      auto input = maker.rowVector(
          {std::string(kPhysicalRowPositionColumnName), "embedding"},
          {maker.flatVector<int64_t>({10, 20, 30, 40}),
           maker.arrayVector<float>({{1.0F, 0.0F, 0.0F, 0.0F},
                                     {0.0F, 1.0F, 0.0F, 0.0F},
                                     {0.0F, 0.0F, 1.0F, 0.0F},
                                     {0.0F, 0.0F, 0.0F, 1.0F}})});
      auto values = std::make_shared<facebook::velox::core::ValuesNode>(
          "artifact-values", std::vector<facebook::velox::RowVectorPtr>{input},
          true);

      ArtifactBuildConfig config;
      config.kind = hnsw ? ArtifactBuildKind::HNSW : ArtifactBuildKind::IVF_PQ;
      config.output.set_artifact_id("index-1");
      config.output.set_artifact_type("VECTOR_INDEX");
      config.output.set_generation("7");
      config.output.set_source_id("catalog.schema.items");
      config.output.set_source_snapshot("snapshot-7");
      config.output.set_staging_uri((directory.path() / "objects").string());
      *config.output.mutable_format() = hnsw
                                            ? capability::hnswArtifactFormat()
                                            : capability::ivfPqArtifactFormat();
      *config.output.mutable_row_locator() =
          capability::physicalRowLocatorContract();
      config.output.add_expected_source_object_ids("source-file-1");
      config.output.set_publication_fencing_token(13);
      config.output.set_work_unit_id("page-1");
      config.output.set_idempotency_key("job-1:page-1:generation-7");
      config.task.job_id = "job-1";
      config.task.page_id = "page-1";
      config.task.fencing_token = 13;
      config.task.scratch_directory = (directory.path() / "scratch").string();
      config.task.max_input_bytes = 8 * 1024 * 1024;
      config.task.max_spill_bytes = 8 * 1024 * 1024;
      config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
      config.task.report_state =
          std::make_shared<artifact::ArtifactWorkReportState>();
      config.row_position_channel = 0;
      config.vector_channel = 1;
      config.max_rows = 100;
      config.hnsw = {4, 2, 4, metric};
      config.ivf_pq = {4, 4, 1, 2, 4, 4, metric};

      auto sink = std::make_shared<ArtifactBuildNode>(
          hnsw ? "artifact-build-hnsw" : "artifact-build-ivfpq", config,
          values);
      auto query_context = facebook::velox::core::QueryCtx::create(
          pangrova::jni::globalExecutor().get(),
          facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
              facebook::velox::exec::FunctionRegistrySnapshot::capture()));
      auto task = facebook::velox::exec::Task::create(
          hnsw ? "artifact-build-hnsw-test" : "artifact-build-ivfpq-test",
          facebook::velox::core::PlanFragment{sink}, 0,
          std::move(query_context),
          facebook::velox::exec::Task::ExecutionMode::kParallel,
          [](facebook::velox::RowVectorPtr, bool,
             facebook::velox::ContinueFuture*) {
            return facebook::velox::exec::BlockingReason::kNotBlocked;
          });
      task->start(2);
      task->taskCompletionFuture().wait();
      ASSERT_FALSE(task->error()) << task->errorMessage();

      auto report = config.task.report_state->snapshot();
      ASSERT_TRUE(report.has_value());
      ASSERT_EQ(report->outputs_size(), 3);
      EXPECT_EQ(report->outputs(0).object_role(),
                hnsw ? "HNSW_GRAPH" : "IVF_PQ_INDEX");
      EXPECT_EQ(report->outputs(1).object_role(), "RAW_VECTORS");
      EXPECT_EQ(report->outputs(2).object_role(), "ROW_LOCATORS");
      EXPECT_EQ(artifact::RowLocatorSidecar::readPhysicalRowPositions(
                    report->outputs(2).opaque_uri(), 4),
                (std::vector<uint64_t>{10, 20, 30, 40}));
      if (hnsw) {
        vector::hnsw::HnswIndex loaded({4, 2, 2});
        ASSERT_TRUE(loaded.load(report->outputs(0).opaque_uri(), 1024 * 1024));
        EXPECT_EQ(loaded.size(), 4);
        EXPECT_EQ(loaded.metric(), metric);
      } else {
        vector::ivfpq::IvfPqIndex loaded({4, 1, 1, 1, 1, 1});
        ASSERT_TRUE(loaded.load(report->outputs(0).opaque_uri(), 1024 * 1024));
        EXPECT_EQ(loaded.size(), 4);
        EXPECT_EQ(loaded.metric(), metric);
      }
    }
  }
}

TEST_F(ArtifactBuildExecutionTest, BuildsImmutableGeneratedValuesAndLocators) {
  unsigned scenario = 0;
  for (const auto& validity :
       {std::vector<uint8_t>{1, 1}, std::vector<uint8_t>{0, 1},
        std::vector<uint8_t>{0, 0}, std::vector<uint8_t>{0, 0}}) {
    const bool denied = scenario++ == 3;
    TestDirectory directory;
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto input = maker.rowVector(
        {std::string(kPhysicalRowPositionColumnName), "generated_value"},
        {maker.flatVector<int64_t>({3, 8}),
         maker.arrayVector<float>({{1.0F, 2.0F}, {3.0F, 4.0F}})});
    input->childAt(1)->setNull(0, validity[0] == 0);
    input->childAt(1)->setNull(1, validity[1] == 0);
    auto values = std::make_shared<facebook::velox::core::ValuesNode>(
        "generated-values", std::vector<facebook::velox::RowVectorPtr>{input},
        true);

    ArtifactBuildConfig config;
    config.kind = ArtifactBuildKind::GENERATED_VALUES;
    config.output.set_artifact_id("generated-column-1");
    config.output.set_artifact_type("GENERATED_COLUMN_VALUES");
    config.output.set_generation("9");
    config.output.set_source_id("catalog.schema.items");
    config.output.set_source_snapshot("snapshot-9");
    config.output.set_staging_uri((directory.path() / "objects").string());
    *config.output.mutable_format() =
        capability::generatedValuesArtifactFormat();
    *config.output.mutable_row_locator() =
        capability::physicalRowLocatorContract();
    config.output.add_expected_source_object_ids("source-file-9");
    config.output.set_publication_fencing_token(23);
    config.output.set_work_unit_id("page-9");
    config.output.set_idempotency_key("job-9:page-9:generation-9");
    config.task.job_id = "job-9";
    config.task.page_id = "page-9";
    config.task.fencing_token = 23;
    config.task.scratch_directory = (directory.path() / "scratch").string();
    config.task.max_input_bytes = denied ? 1024 : 8 * 1024 * 1024;
    config.task.max_spill_bytes = 8 * 1024 * 1024;
    config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
    config.task.report_state =
        std::make_shared<artifact::ArtifactWorkReportState>();
    config.row_position_channel = 0;
    config.generated_value_channel = 1;
    config.generated_value_dimension = denied ? 2048 : 2;
    config.max_rows = 100;

    auto sink = std::make_shared<ArtifactBuildNode>(
        "artifact-build-generated-values", config, values);
    auto query_context = facebook::velox::core::QueryCtx::create(
        pangrova::jni::globalExecutor().get(),
        facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
            facebook::velox::exec::FunctionRegistrySnapshot::capture()));
    auto task = facebook::velox::exec::Task::create(
        "artifact-build-generated-values-test",
        facebook::velox::core::PlanFragment{sink}, 0, std::move(query_context),
        facebook::velox::exec::Task::ExecutionMode::kParallel,
        [](facebook::velox::RowVectorPtr, bool,
           facebook::velox::ContinueFuture*) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        });
    task->start(1);
    task->taskCompletionFuture().wait();
    if (denied) {
      ASSERT_TRUE(task->error());
      EXPECT_NE(task->errorMessage().find(
                    "dense materialization exceeds memory grant"),
                std::string::npos);
      EXPECT_FALSE(config.task.report_state->snapshot().has_value());
      continue;
    }
    ASSERT_FALSE(task->error()) << task->errorMessage();

    auto report = config.task.report_state->snapshot();
    ASSERT_TRUE(report.has_value());
    ASSERT_EQ(report->outputs_size(), 2);
    EXPECT_EQ(report->outputs(0).object_role(), "GENERATED_VALUES");
    EXPECT_EQ(report->outputs(0).ordinal(), 0);
    EXPECT_EQ(report->outputs(1).object_role(), "ROW_LOCATORS");
    EXPECT_EQ(report->outputs(1).ordinal(), 1);
    for (const auto& output : report->outputs()) {
      EXPECT_EQ(output.source_object_id(), "source-file-9");
      EXPECT_EQ(output.row_count(), 2);
      EXPECT_EQ(output.checksum().digest().size(), 32);
      EXPECT_EQ(std::filesystem::file_size(output.opaque_uri()),
                output.size_bytes());
    }
    auto restored = artifact::GeneratedValueSegment::read(
        report->outputs(0).opaque_uri(), 2, 2, report->outputs(0).size_bytes());
    EXPECT_EQ(restored.validity, validity);
    EXPECT_EQ(restored.values,
              (std::vector<float>{
                  validity[0] ? 1.0F : 0.0F, validity[0] ? 2.0F : 0.0F,
                  validity[1] ? 3.0F : 0.0F, validity[1] ? 4.0F : 0.0F}));
    EXPECT_EQ(artifact::RowLocatorSidecar::readPhysicalRowPositions(
                  report->outputs(1).opaque_uri(), 2),
              (std::vector<uint64_t>{3, 8}));
    EXPECT_EQ(report->report_checksum().digest().size(), 32);
  }
}

TEST_F(ArtifactBuildExecutionTest, StreamsImmutableTabularExportAndReport) {
  TestDirectory directory;
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto first = maker.rowVector({"customer_id", "country"},
                               {maker.flatVector<int64_t>({1, 2}),
                                maker.flatVector<std::string>({"CN", "US"})});
  auto second = maker.rowVector(
      {"customer_id", "country"},
      {maker.flatVector<int64_t>({3}), maker.flatVector<std::string>({"SG"})});
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "tabular-export-values",
      std::vector<facebook::velox::RowVectorPtr>{first, second}, true);

  ArtifactBuildConfig config;
  config.kind = ArtifactBuildKind::TABULAR_EXPORT;
  config.output.set_artifact_id("feature-view-export-1");
  config.output.set_artifact_type("TABULAR_EXPORT");
  config.output.set_generation("11");
  config.output.set_source_id("lake.raw.customers");
  config.output.set_source_snapshot("41");
  config.output.set_staging_uri((directory.path() / "objects").string());
  *config.output.mutable_format() = capability::tabularExportFormat();
  config.output.set_work_kind(
      protocol::v2::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION);
  config.output.set_output_scope_id("page-11");
  config.output.set_publication_fencing_token(31);
  config.output.set_work_unit_id("page-11");
  config.output.set_idempotency_key("job-11:page-11:generation-11");
  config.task.job_id = "job-11";
  config.task.page_id = "page-11";
  config.task.fencing_token = 31;
  config.task.scratch_directory = (directory.path() / "scratch").string();
  config.task.max_input_bytes = 8 * 1024 * 1024;
  config.task.max_spill_bytes = 8 * 1024 * 1024;
  config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
  config.task.report_state =
      std::make_shared<artifact::ArtifactWorkReportState>();
  config.max_rows = 100;

  auto sink = std::make_shared<ArtifactBuildNode>(
      "artifact-build-tabular-export", config, values);
  auto query_context = facebook::velox::core::QueryCtx::create(
      pangrova::jni::globalExecutor().get(),
      facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
          facebook::velox::exec::FunctionRegistrySnapshot::capture()));
  auto task = facebook::velox::exec::Task::create(
      "artifact-build-tabular-export-test",
      facebook::velox::core::PlanFragment{sink}, 0, std::move(query_context),
      facebook::velox::exec::Task::ExecutionMode::kParallel,
      [](facebook::velox::RowVectorPtr, bool,
         facebook::velox::ContinueFuture*) {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      });
  task->start(1);
  task->taskCompletionFuture().wait();
  ASSERT_FALSE(task->error()) << task->errorMessage();

  auto report = config.task.report_state->snapshot();
  ASSERT_TRUE(report.has_value());
  ASSERT_EQ(report->outputs_size(), 1);
  const auto& output = report->outputs(0);
  EXPECT_EQ(output.object_role(), "TABULAR_DATA");
  EXPECT_EQ(output.source_object_id(), "page-11");
  EXPECT_EQ(output.row_count(), 3);
  EXPECT_EQ(output.scope(), protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT);
  EXPECT_EQ(output.checksum().digest().size(), 32);
  EXPECT_EQ(std::filesystem::file_size(output.opaque_uri()),
            output.size_bytes());
  EXPECT_GT(report->usage().object_store_write_bytes(), 0);

  auto file = arrow::io::ReadableFile::Open(output.opaque_uri());
  ASSERT_TRUE(file.ok()) << file.status().ToString();
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(*file);
  ASSERT_TRUE(reader.ok()) << reader.status().ToString();
  int64_t rows = 0;
  while (true) {
    auto batch = (*reader)->Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!*batch) {
      break;
    }
    rows += (*batch)->num_rows();
  }
  EXPECT_EQ(rows, 3);
}

TEST_F(ArtifactBuildExecutionTest, PublishesSchemaBearingZeroRowTabularExport) {
  TestDirectory directory;
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto empty = maker.rowVector(
      {"customer_id", "country"},
      {maker.flatVector<int64_t>(std::vector<int64_t>{}),
       maker.flatVector<std::string>(std::vector<std::string>{})});
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "empty-tabular-export-values",
      std::vector<facebook::velox::RowVectorPtr>{empty}, true);

  ArtifactBuildConfig config;
  config.kind = ArtifactBuildKind::TABULAR_EXPORT;
  config.output.set_artifact_id("feature-view-export-empty");
  config.output.set_artifact_type("TABULAR_EXPORT");
  config.output.set_generation("12");
  config.output.set_source_id("lake.raw.customers");
  config.output.set_source_snapshot("42");
  config.output.set_staging_uri((directory.path() / "objects").string());
  *config.output.mutable_format() = capability::tabularExportFormat();
  config.output.set_work_kind(
      protocol::v2::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION);
  config.output.set_output_scope_id("page-empty");
  config.output.set_publication_fencing_token(32);
  config.output.set_work_unit_id("page-empty");
  config.output.set_idempotency_key("job-empty:page-empty:generation-12");
  config.output.set_empty_partition_policy(
      protocol::v2::ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS);
  config.task.job_id = "job-empty";
  config.task.page_id = "page-empty";
  config.task.fencing_token = 32;
  config.task.scratch_directory = (directory.path() / "scratch").string();
  config.task.max_input_bytes = 8 * 1024 * 1024;
  config.task.max_spill_bytes = 8 * 1024 * 1024;
  config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
  config.task.report_state =
      std::make_shared<artifact::ArtifactWorkReportState>();
  config.max_rows = 100;

  auto sink = std::make_shared<ArtifactBuildNode>(
      "artifact-build-empty-tabular-export", config, values);
  auto query_context = facebook::velox::core::QueryCtx::create(
      pangrova::jni::globalExecutor().get(),
      facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
          facebook::velox::exec::FunctionRegistrySnapshot::capture()));
  auto task = facebook::velox::exec::Task::create(
      "artifact-build-empty-tabular-export-test",
      facebook::velox::core::PlanFragment{sink}, 0, std::move(query_context),
      facebook::velox::exec::Task::ExecutionMode::kParallel,
      [](facebook::velox::RowVectorPtr, bool,
         facebook::velox::ContinueFuture*) {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      });
  task->start(1);
  task->taskCompletionFuture().wait();
  ASSERT_FALSE(task->error()) << task->errorMessage();

  auto report = config.task.report_state->snapshot();
  ASSERT_TRUE(report.has_value());
  ASSERT_EQ(report->outputs_size(), 1);
  const auto& output = report->outputs(0);
  EXPECT_EQ(output.object_role(), "TABULAR_DATA");
  EXPECT_EQ(output.source_object_id(), "page-empty");
  EXPECT_EQ(output.scope(), protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT);
  EXPECT_EQ(output.row_count(), 0);
  EXPECT_GT(output.size_bytes(), 0);
  EXPECT_EQ(output.checksum().digest().size(), 32);

  auto file = arrow::io::ReadableFile::Open(output.opaque_uri());
  ASSERT_TRUE(file.ok()) << file.status().ToString();
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(*file);
  ASSERT_TRUE(reader.ok()) << reader.status().ToString();
  EXPECT_EQ((*reader)->schema()->field_names(),
            (std::vector<std::string>{"customer_id", "country"}));
  auto batch = (*reader)->Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  EXPECT_EQ(*batch, nullptr);
}

TEST_F(ArtifactBuildExecutionTest, BuildsImmutableBm25WorkPageAndReport) {
  TestDirectory directory;
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto input = maker.rowVector(
      {std::string(kPhysicalRowPositionColumnName), "description"},
      {maker.flatVector<int64_t>({2, 5, 9}),
       maker.flatVector<std::string>(
           {"distributed search search", "query engine", "search engine"})});
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "artifact-values", std::vector<facebook::velox::RowVectorPtr>{input},
      true);

  ArtifactBuildConfig config;
  config.kind = ArtifactBuildKind::BM25;
  config.output.set_artifact_id("text-index-1");
  config.output.set_artifact_type("TEXT_INDEX");
  config.output.set_generation("3");
  config.output.set_source_id("catalog.schema.items");
  config.output.set_source_snapshot("snapshot-3");
  config.output.set_staging_uri((directory.path() / "objects").string());
  *config.output.mutable_format() = capability::bm25ArtifactFormat();
  *config.output.mutable_row_locator() =
      capability::physicalRowLocatorContract();
  config.output.add_expected_source_object_ids("source-file-1");
  config.output.set_publication_fencing_token(17);
  config.output.set_work_unit_id("page-3");
  config.output.set_idempotency_key("job-3:page-3:generation-3");
  config.task.job_id = "job-3";
  config.task.page_id = "page-3";
  config.task.fencing_token = 17;
  config.task.scratch_directory = (directory.path() / "scratch").string();
  config.task.max_input_bytes = 8 * 1024 * 1024;
  config.task.max_spill_bytes = 8 * 1024 * 1024;
  config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
  config.task.report_state =
      std::make_shared<artifact::ArtifactWorkReportState>();
  config.row_position_channel = 0;
  config.text_channel = 1;
  config.max_rows = 100;

  auto sink = std::make_shared<ArtifactBuildNode>("artifact-build-bm25", config,
                                                  values);
  auto query_context = facebook::velox::core::QueryCtx::create(
      pangrova::jni::globalExecutor().get(),
      facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
          facebook::velox::exec::FunctionRegistrySnapshot::capture()));
  auto task = facebook::velox::exec::Task::create(
      "artifact-build-bm25-execution-test",
      facebook::velox::core::PlanFragment{sink}, 0, std::move(query_context),
      facebook::velox::exec::Task::ExecutionMode::kParallel,
      [](facebook::velox::RowVectorPtr, bool,
         facebook::velox::ContinueFuture*) {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      });
  task->start(2);
  task->taskCompletionFuture().wait();
  ASSERT_FALSE(task->error()) << task->errorMessage();

  auto report = config.task.report_state->snapshot();
  ASSERT_TRUE(report.has_value());
  ASSERT_EQ(report->outputs_size(), 3);
  EXPECT_EQ(report->outputs(0).object_role(), "BM25_POSTINGS");
  EXPECT_EQ(report->outputs(1).object_role(), "ROW_LOCATORS");
  EXPECT_EQ(report->outputs(2).object_role(), "BM25_STATS_SHARD");
  auto segment = text::bm25::Bm25Segment::load(report->outputs(0).opaque_uri(),
                                               8 * 1024 * 1024);
  auto corpus_stats = text::bm25::Bm25CorpusStats::load(
      report->outputs(2).opaque_uri(), 8 * 1024 * 1024);
  auto matches = segment.search(text::bm25::Bm25Analyzer::tokenize("search"), 2,
                                1.2, 0.75, corpus_stats);
  ASSERT_EQ(matches.size(), 2);
  EXPECT_EQ(matches[0].document, 0);
  EXPECT_EQ(artifact::RowLocatorSidecar::readPhysicalRowPositions(
                report->outputs(1).opaque_uri(), 3),
            (std::vector<uint64_t>{2, 5, 9}));
}

TEST_F(ArtifactBuildExecutionTest, FinalizesGlobalBm25StatisticsInNative) {
  TestDirectory directory;
  ArtifactCacheGuard cache(directory.path() / "cache");
  const auto first_path = (directory.path() / "first.stats").string();
  const auto second_path = (directory.path() / "second.stats").string();
  text::bm25::Bm25CorpusStats::writeShard(first_path, 2, 6,
                                          {{"alpha", 2}, {"beta", 1}});
  text::bm25::Bm25CorpusStats::writeShard(second_path, 3, 9,
                                          {{"alpha", 1}, {"gamma", 3}});
  const auto staging = (directory.path() / "objects").string();
  const auto first = artifact::ImmutableArtifactWriter::putFile(
      first_path, staging, "BM25_STATS_SHARD");
  const auto second = artifact::ImmutableArtifactWriter::putFile(
      second_path, staging, "BM25_STATS_SHARD");

  facebook::velox::test::VectorMaker maker(pool_.get());
  auto empty = maker.rowVector(
      {std::string(kPhysicalRowPositionColumnName), "description"},
      {maker.flatVector<int64_t>({}), maker.flatVector<std::string>({})});
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "artifact-values", std::vector<facebook::velox::RowVectorPtr>{empty},
      true);

  ArtifactBuildConfig config;
  config.kind = ArtifactBuildKind::BM25_FINALIZE;
  config.output.set_artifact_id("text-index-1");
  config.output.set_artifact_type("TEXT_INDEX");
  config.output.set_generation("3");
  config.output.set_source_id("catalog.schema.items");
  config.output.set_source_snapshot("snapshot-3");
  config.output.set_staging_uri(staging);
  *config.output.mutable_format() = capability::bm25ArtifactFormat();
  *config.output.mutable_row_locator() =
      capability::physicalRowLocatorContract();
  config.output.set_publication_fencing_token(19);
  config.output.set_work_unit_id("page-final");
  config.output.set_idempotency_key("job-4:page-final:generation-3");
  config.output.set_work_kind(
      protocol::v2::ARTIFACT_WORK_KIND_GENERATION_FINALIZE);
  config.output.set_output_scope_id("3");
  auto* analyzer = config.output.add_properties();
  analyzer->set_name("analyzer");
  analyzer->mutable_value()->set_string_value("standard-v1");
  for (const auto& [id, object] :
       std::vector<std::pair<std::string, artifact::ImmutableArtifactObject>>{
           {"page-a", first}, {"page-b", second}}) {
    auto* input = &config.input_objects.emplace_back();
    input->set_object_id(id);
    input->set_opaque_uri(object.uri);
    input->set_size_bytes(object.size_bytes);
    input->mutable_checksum()->set_algorithm(
        protocol::v2::Checksum::ALGORITHM_SHA256);
    input->mutable_checksum()->set_digest(object.sha256);
    input->set_object_role("BM25_STATS_SHARD");
    input->set_source_object_id(id);
    input->set_row_count(id == "page-a" ? 2 : 3);
    input->set_ordinal(0);
    input->set_scope(protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT);
  }
  config.task.job_id = "job-4";
  config.task.page_id = "page-final";
  config.task.fencing_token = 19;
  config.task.scratch_directory = (directory.path() / "scratch").string();
  config.task.max_input_bytes = 8 * 1024 * 1024;
  config.task.max_spill_bytes = 8 * 1024 * 1024;
  config.task.max_object_store_read_bytes = 8 * 1024 * 1024;
  config.task.max_object_store_write_bytes = 8 * 1024 * 1024;
  config.task.report_state =
      std::make_shared<artifact::ArtifactWorkReportState>();

  auto sink = std::make_shared<ArtifactBuildNode>("artifact-build-final",
                                                  config, values);
  auto query_context = facebook::velox::core::QueryCtx::create(
      pangrova::jni::globalExecutor().get(),
      facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
          facebook::velox::exec::FunctionRegistrySnapshot::capture()));
  auto task = facebook::velox::exec::Task::create(
      "artifact-build-final-execution-test",
      facebook::velox::core::PlanFragment{sink}, 0, std::move(query_context),
      facebook::velox::exec::Task::ExecutionMode::kParallel,
      [](facebook::velox::RowVectorPtr, bool,
         facebook::velox::ContinueFuture*) {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      });
  task->start(1);
  task->taskCompletionFuture().wait();
  ASSERT_FALSE(task->error()) << task->errorMessage();

  auto report = config.task.report_state->snapshot();
  ASSERT_TRUE(report.has_value());
  ASSERT_EQ(report->outputs_size(), 1);
  EXPECT_EQ(report->outputs(0).object_role(), "BM25_CORPUS_STATS");
  EXPECT_EQ(report->outputs(0).source_object_id(), "3");
  EXPECT_EQ(report->outputs(0).row_count(), 5);
  EXPECT_EQ(report->outputs(0).scope(),
            protocol::v2::ARTIFACT_OBJECT_SCOPE_GENERATION);
  auto statistics = text::bm25::Bm25CorpusStats::load(
      report->outputs(0).opaque_uri(), 8 * 1024 * 1024);
  EXPECT_EQ(statistics.documentCount(), 5);
  EXPECT_EQ(statistics.documentFrequency("alpha"), 3);
  EXPECT_EQ(statistics.documentFrequency("gamma"), 3);
}

}  // namespace
}  // namespace pangrova::velox::test
