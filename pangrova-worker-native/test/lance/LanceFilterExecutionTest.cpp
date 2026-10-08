// Native component oracle: real Lance write/commit -> canonical FilterRel ->
// production connector -> real Velox Task. This is not JNI or SQL E2E evidence.
#include "LanceDatasetTestFixture.h"
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <atomic>
#include <thread>
#include <unordered_map>

#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>
#include "columnar/ColumnarImportLayout.h"
#include "executor/driver/TaskExecutor.h"
#include "jni/command/StatusSerializer.h"
#include "observability/ColumnarReadMetrics.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Split.h"
#include "velox/exec/Task.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::lance::test {
namespace {

substrait::Expression field(int index) {
  substrait::Expression expression;
  auto* selection = expression.mutable_selection();
  selection->mutable_root_reference();
  selection->mutable_direct_reference()->mutable_struct_field()->set_field(index);
  return expression;
}

substrait::Expression integer(int64_t value) {
  substrait::Expression expression;
  expression.mutable_literal()->set_i64(value);
  return expression;
}

substrait::Expression predicate(
    int anchor, const substrait::Expression& left,
    const substrait::Expression& right) {
  substrait::Expression expression;
  auto* call = expression.mutable_scalar_function();
  call->set_function_reference(anchor);
  call->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  call->add_arguments()->mutable_value()->CopyFrom(left);
  call->add_arguments()->mutable_value()->CopyFrom(right);
  return expression;
}

substrait::Plan filterPlan(
    const RowTypePtr& schema, const substrait::Expression& condition,
    const std::vector<std::string>& functions,
    const std::vector<int>& projection = {}) {
  substrait::Plan plan;
  for (size_t i = 0; i < functions.size(); ++i) {
    auto* function = plan.add_extensions()->mutable_extension_function();
    function->set_function_anchor(i);
    function->set_name(functions[i]);
  }
  auto* root = plan.add_relations()->mutable_root();
  auto* filter = root->mutable_input()->mutable_filter();
  filter->mutable_condition()->CopyFrom(condition);
  auto* read = filter->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("lance_table");
  auto* base = read->mutable_base_schema();
  base->mutable_struct_();
  for (size_t i = 0; i < schema->size(); ++i) {
    base->add_names(schema->nameOf(i));
    auto* type = base->mutable_struct_()->add_types();
    switch (schema->childAt(i)->kind()) {
      case TypeKind::BIGINT:
        type->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
        break;
      case TypeKind::VARCHAR:
        type->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
        break;
      case TypeKind::DOUBLE:
        type->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
        break;
      case TypeKind::BOOLEAN:
        type->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
        break;
      default: throw std::invalid_argument("Unsupported Lance oracle schema");
    }
  }
  if (projection.empty()) {
    for (const auto& name : schema->names()) root->add_names(name);
  } else {
    for (int index : projection) {
      read->mutable_common()->mutable_emit()->add_output_mapping(index);
      root->add_names(schema->nameOf(index));
    }
  }
  return plan;
}

class LanceFilterExecutionTest : public LanceDatasetTestFixture {
 protected:
  std::string writeBitmapDataset() {
    const auto path = (tmpDir_ / "bitmap.lance").string();
    const auto type = ROW({"id", "flag"}, {BIGINT(), BOOLEAN()});
    std::vector<std::vector<uint8_t>> fragments;
    std::vector<std::string> staging;
    auto config = createConfig(path, "bitmap-write", type);
    for (int partition = 0; partition < 2; ++partition) {
      auto allocation = createConfig(path, "bitmap-write", type,
          LanceWriteOperation::kCreate, 0, partition);
      LanceBatchWriter writer(pool_, type, allocation);
      auto ids = BaseVector::create(BIGINT(), 16, pool_.get());
      auto flags = BaseVector::create(BOOLEAN(), 16, pool_.get());
      for (int row = 0; row < 16; ++row) {
        ids->asFlatVector<int64_t>()->set(row, partition * 16 + row + 1);
        flags->asFlatVector<bool>()->set(row, row % 2 == 0);
        if (row % 3 == 0) flags->setNull(row, true);
      }
      writer.writeBatch(std::make_shared<RowVector>(pool_.get(), type, nullptr,
          16, std::vector<VectorPtr>{ids, flags}));
      fragments.push_back(writer.finish());
      staging.push_back(allocation.transactionStagingPrefix);
    }
    EXPECT_EQ(commitFragments(config, fragments, false, 0, staging), 1);
    return path;
  }

  // Oracle independently imports actual C ABI batches and measures the
  // normalization usage, without a fabricated runtime counter/stat writer.
  uint64_t actualCopyBytes(const std::string& path) {
    uint64_t total = 0;
    const auto splits = partitions(path);
    EXPECT_EQ(splits.size(), 2);
    auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
    for (const auto& split : splits) {
      LanceStorageContext storage(split.storage_binding);
      std::unique_ptr<LanceCReader, decltype(&lance_ffi_reader_close)> reader(
          lance_ffi_reader_open(storage.get(), path.c_str(), 1,
              reinterpret_cast<const uint8_t*>(split.file_scan_detail.data()),
              split.file_scan_detail.size(), 0), lance_ffi_reader_close);
      if (!reader) throw std::runtime_error(lance_ffi_last_error());
      uint64_t split_bytes = 0;
      while (true) {
        ArrowArray array{};
        ArrowSchema schema{};
        const int rc = lance_ffi_reader_next(reader.get(), &array, &schema, 2);
        if (rc == 0) break;
        if (rc != 1) throw std::runtime_error(lance_ffi_last_error());
        auto imported = arrow::ImportRecordBatch(&array, &schema).ValueOrDie();
        pangrova::columnar::ColumnarImportUsage usage;
        auto normalized = pangrova::columnar::normalizeImportLayout(*imported, allocator, usage).ValueOrDie();
        EXPECT_EQ(normalized->num_rows(), 2);
        EXPECT_EQ(usage.allocatedOffsetBytes, 0);
        split_bytes += usage.copiedBytes;
      }
      EXPECT_GT(split_bytes, 0);
      total += split_bytes;
    }
    return total;
  }

  pangrova::executor::TaskExecutorConfig copyTaskConfig(const std::string& path) {
    static std::atomic<uint64_t> sequence{0};
    pangrova::executor::TaskExecutorConfig config;
    config.task_id = "lance-copy-task-" + std::to_string(sequence.fetch_add(1));
    config.query_id = "lance-copy-query";
    config.function_registry = exec::FunctionRegistrySnapshot::capture();
    auto plan = filterPlan(ROW({"id", "flag"}, {BIGINT(), BOOLEAN()}),
        predicate(0, field(0), integer(0)), {"gt"});
    const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    config.substrait_plan.assign(bytes.begin(), bytes.end());
    config.splits = partitions(path);
    config.shuffle_batch_size = 2;
    config.resource_limit.max_threads = 1;
    config.session_properties["velox.max_drivers"] = "1";
    config.queue_poll_timeout = std::chrono::milliseconds(10);
    return config;
  }

  protocol::v2::TaskReport report(pangrova::executor::TaskExecutor& executor) {
    const auto bytes = pangrova::jni::serializeTaskStatus(executor);
    protocol::v2::TaskReport result;
    if (!result.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
      throw std::runtime_error("Actual scan report cannot be decoded");
    }
    return result;
  }

  void drain(pangrova::executor::TaskExecutor& executor) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    uint64_t rows = 0;
    while (std::chrono::steady_clock::now() < deadline) {
      RowVectorPtr batch;
      const auto rc = executor.getNextBatch(batch);
      if (rc == pangrova::executor::kDataAvailable) {
        EXPECT_NE(batch, nullptr);
        rows += batch->size();
      } else if (rc == pangrova::executor::kEof) {
        EXPECT_EQ(rows, 32);
        return;
      } else if (rc == pangrova::executor::kError) {
        throw std::runtime_error(executor.errorMessage());
      }
    }
    throw std::runtime_error("Actual Lance copy scan did not drain");
  }

  std::vector<pangrova::split::SplitConfig> partitions(const std::string& path) {
    const auto version = datasetVersion(path);
    const auto storage = pangrova::test::testStorageBinding(path);
    std::vector<pangrova::split::SplitConfig> splits;
    bool has_cursor = false;
    uint64_t after = 0;
    do {
      const auto page = pangrova::test::testScanPage(path, version, storage, 256, has_cursor, after);
      for (const auto& partition : page.partitions()) {
        pangrova::split::SplitConfig split;
        split.split_id = "lance-partition-" + std::to_string(partition.partition_id());
        split.connector_id = pangrova::velox::PangrovaVeloxConnector::kConnectorId;
        split.table_name = "lance_table";
        split.file_path = path;
        split.file_format = pangrova::split::FileFormatType::LANCE;
        split.source_snapshot = std::to_string(version);
        split.data_object_id = "table-a:" + split.source_snapshot + ":" +
            std::to_string(partition.partition_id());
        split.storage_binding = storage;
        split.file_scan_detail = pangrova::test::testReadDetail(partition);
        splits.push_back(std::move(split));
      }
      has_cursor = page.has_more();
      after = page.next_partition_id();
    } while (has_cursor);
    return splits;
  }

  std::vector<RowVectorPtr> execute(
      const std::string& path, substrait::Plan& plan, uint32_t batch_rows,
      bool mutate_after_conversion = false) {
    auto splits = partitions(path);
    const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    pangrova::velox::VeloxPlanConverter converter(converter_registry);
    auto converted = converter.convert({bytes.begin(), bytes.end()}, splits);
    // Mutating the original IR cannot change the already compiled predicate.
    if (mutate_after_conversion) {
      plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_filter()
          ->mutable_condition()->mutable_scalar_function()->mutable_arguments(1)
          ->mutable_value()->mutable_literal()->set_i64(100);
    }
    const auto& scans = converter.scanNodeOutputTypes();
    if (scans.size() != 1 || splits.empty()) {
      throw std::runtime_error("Lance oracle requires one scan and real partitions");
    }
    std::unordered_map<std::string, std::string> config{
        {"require_function_registry_snapshot", "true"},
        {core::QueryConfig::kPreferredOutputBatchRows, std::to_string(batch_rows)},
        {core::QueryConfig::kMaxOutputBatchRows, std::to_string(batch_rows)}};
    static std::atomic<uint64_t> sequence{0};
    auto task = exec::Task::create(
        "canonical-lance-filter-" + std::to_string(sequence.fetch_add(1)),
        core::PlanFragment{converted}, 0,
        core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
            core::QueryConfig{std::move(config), converter_registry}), exec::Task::ExecutionMode::kSerial);
    EXPECT_TRUE(task->supportSerialExecutionMode());
    for (auto& split : splits) {
      task->addSplit(scans.begin()->first, exec::Split{
          std::make_shared<pangrova::velox::PangrovaConnectorSplit>(
              pangrova::velox::PangrovaVeloxConnector::kConnectorId, std::move(split))});
    }
    task->noMoreSplits(scans.begin()->first);
    std::vector<RowVectorPtr> output;
    while (true) {
      // next() assigns a future only when externally blocked. A default
      // SemiFuture<Unit> is already valid/ready, not an empty out-parameter.
      auto future = ContinueFuture::makeEmpty();
      auto batch = task->next(&future);
      if (batch) {
        EXPECT_LE(batch->size(), batch_rows);
        output.push_back(std::dynamic_pointer_cast<RowVector>(BaseVector::copy(*batch, pool_.get())));
      } else if (future.valid()) {
        std::move(future).get();
      } else {
        break;
      }
    }
    EXPECT_FALSE(task->error()) << task->errorMessage();
    EXPECT_EQ(task->state(), exec::TaskState::kFinished);
    EXPECT_EQ(task->numRunningDrivers(), 0);
    // Task is destroyed before converter (which owns expression pools).
    return output;
  }

  void rejectLateProjection(const std::string& path) {
    const auto storage_binding = pangrova::test::testStorageBinding(path);
    const auto version = datasetVersion(path);
    const auto page = pangrova::test::testScanPage(path, version, storage_binding);
    ASSERT_GT(page.partitions_size(), 0);
    const auto detail = pangrova::test::testReadDetail(page.partitions(0));
    LanceStorageContext storage(storage_binding);
    std::unique_ptr<LanceCReader, decltype(&lance_ffi_reader_close)> reader(
        lance_ffi_reader_open(storage.get(), path.c_str(), version,
            reinterpret_cast<const uint8_t*>(detail.data()), detail.size(), 0),
        lance_ffi_reader_close);
    ASSERT_NE(reader, nullptr) << lance_ffi_last_error();
    ArrowArray array{};
    ArrowSchema schema{};
    const auto result = lance_ffi_reader_next(reader.get(), &array, &schema, 3);
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    ASSERT_EQ(result, 1) << lance_ffi_last_error();
    const char* columns[] = {"id"};
    ASSERT_EQ(lance_ffi_reader_set_projection(reader.get(), columns, 1), -1);
    const auto* error = lance_ffi_last_error();
    ASSERT_NE(error, nullptr);
    EXPECT_NE(std::string(error).find("after scanning starts"), std::string::npos);
  }
};

TEST_F(LanceFilterExecutionTest, ActualCopiesReachTaskReportAcrossSplitsAndRelease) {
  const auto path = writeBitmapDataset();
  const auto expected = actualCopyBytes(path);
  ASSERT_GT(expected, 0);
  pangrova::executor::TaskExecutor executor(copyTaskConfig(path));
  executor.start();
  drain(executor);
  auto* runner = executor.veloxTaskRunner();
  ASSERT_NE(runner, nullptr);
  ASSERT_NE(runner->veloxTask(), nullptr);
  const auto actual_stats = runner->veloxTask()->taskStats();
  uint64_t runtime_bytes = 0;
  for (const auto& pipeline : actual_stats.pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      const auto metric = op.runtimeStats.find(std::string(pangrova::observability::kColumnarReadCopiedBytes));
      if (metric == op.runtimeStats.end()) continue;
      EXPECT_EQ(metric->second.unit, RuntimeCounter::Unit::kBytes);
      runtime_bytes += metric->second.sum;
    }
  }
  EXPECT_EQ(runtime_bytes, expected);
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
  EXPECT_EQ(executor.metricsCollector().snapshot().columnar_conversion_copied_bytes, expected);
  executor.release();
  EXPECT_TRUE(executor.hasFinalTaskStats());
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
}

TEST_F(LanceFilterExecutionTest, ActualCopiesSurviveCancelledScanAndReaderRelease) {
  const auto path = writeBitmapDataset();
  auto config = copyTaskConfig(path);
  config.output_queue_capacity = 1;
  config.queue_poll_timeout = std::chrono::seconds(30);
  pangrova::executor::TaskExecutor executor(config);
  executor.start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  uint64_t before = 0;
  while (before == 0 && std::chrono::steady_clock::now() < deadline) {
    before = report(executor).usage().columnar_conversion_copied_bytes();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_GT(before, 0);
  executor.cancel();
  executor.release();
  ASSERT_EQ(executor.state(), pangrova::executor::ExecutorState::CANCELLED);
  ASSERT_TRUE(executor.hasFinalTaskStats());
  const auto final = report(executor).usage().columnar_conversion_copied_bytes();
  EXPECT_GE(final, before);
  EXPECT_LE(final, actualCopyBytes(path));
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), final);
}

TEST_F(LanceFilterExecutionTest, ActualCopiesSurviveSubsequentSplitFailure) {
  const auto path = writeBitmapDataset();
  const auto expected = actualCopyBytes(path);
  auto config = copyTaskConfig(path);
  pangrova::velox::VeloxPlanConverter converter(config.function_registry);
  auto converted = converter.convert(config.substrait_plan, config.splits);
  const auto& scans = converter.scanNodeOutputTypes();
  ASSERT_EQ(scans.size(), 1);
  std::unordered_map<std::string, std::string> properties{
      {"require_function_registry_snapshot", "true"},
      {core::QueryConfig::kPreferredOutputBatchRows, "2"},
      {core::QueryConfig::kMaxOutputBatchRows, "2"}};
  auto rows = std::make_shared<std::atomic<uint64_t>>(0);
  exec::Consumer consumer = [rows](RowVectorPtr output, bool, ContinueFuture*) {
    if (output) rows->fetch_add(output->size());
    return exec::BlockingReason::kNotBlocked;
  };
  auto task = exec::Task::create("lance-phased-split-failure", core::PlanFragment{converted}, 0,
      core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
          core::QueryConfig{std::move(properties), config.function_registry}), exec::Task::ExecutionMode::kParallel, std::move(consumer));
  auto cancel_task = folly::makeGuard([&task] { if (task) task->requestCancel().wait(); });
  for (const auto& split : config.splits) {
    task->addSplit(scans.begin()->first, exec::Split{
        std::make_shared<pangrova::velox::PangrovaConnectorSplit>(
            pangrova::velox::PangrovaVeloxConnector::kConnectorId, split)});
  }
  // Keep the real split queue open until both fragments have produced all rows.
  task->start(2);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (rows->load() < 32 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(rows->load(), 32);
  ASSERT_EQ(task->state(), exec::TaskState::kRunning);
  pangrova::observability::NativeMetricsCollector collector;
  collector.observeColumnarReadTaskStats(task->taskStats());
  ASSERT_EQ(collector.snapshot().columnar_conversion_copied_bytes, expected);
  auto missing = config.splits.back();
  missing.split_id = "missing-after-real-scan";
  missing.file_path = (tmpDir_ / "missing.lance").string();
  task->addSplit(scans.begin()->first, exec::Split{
      std::make_shared<pangrova::velox::PangrovaConnectorSplit>(
          pangrova::velox::PangrovaVeloxConnector::kConnectorId, std::move(missing))});
  task->noMoreSplits(scans.begin()->first);
  ASSERT_NO_THROW(task->taskCompletionFuture().within(std::chrono::seconds(20)).get());
  ASSERT_NO_THROW(task->requestCancel().wait());
  ASSERT_EQ(task->state(), exec::TaskState::kFailed);
  EXPECT_NE(task->errorMessage().find("missing.lance"), std::string::npos);
  const auto final_stats = task->taskStats();
  collector.observeColumnarReadTaskStats(final_stats);
  EXPECT_EQ(collector.snapshot().columnar_conversion_copied_bytes, expected);
  task.reset();
  collector.observeColumnarReadTaskStats(final_stats);
  EXPECT_EQ(collector.snapshot().columnar_conversion_copied_bytes, expected);
}

TEST_F(LanceFilterExecutionTest, ActualCopiesReachFailedTaskReportAfterBlockingSort) {
  const auto path = writeBitmapDataset();
  const auto expected = actualCopyBytes(path);
  auto config = copyTaskConfig(path);
  auto plan = filterPlan(ROW({"id", "flag"}, {BIGINT(), BOOLEAN()}),
      predicate(0, field(0), integer(0)), {"gt", "divide"});
  auto* root = plan.mutable_relations(0)->mutable_root();
  const auto original = root->input();
  root->clear_names();
  root->add_names("invalid_quotient");
  auto* project = root->mutable_input()->mutable_project();
  auto* sort = project->mutable_input()->mutable_sort();
  *sort->mutable_input() = original;
  auto* order = sort->add_sorts();
  order->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST);
  *order->mutable_expr() = field(0);
  auto* quotient = project->add_expressions()->mutable_scalar_function();
  quotient->set_function_reference(1);
  quotient->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *quotient->add_arguments()->mutable_value() = field(0);
  *quotient->add_arguments()->mutable_value() = integer(0);
  project->mutable_common()->mutable_emit()->add_output_mapping(2);
  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  config.substrait_plan.assign(bytes.begin(), bytes.end());
  pangrova::executor::TaskExecutor executor(config);
  executor.start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (executor.state() == pangrova::executor::ExecutorState::RUNNING &&
      std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(executor.state(), pangrova::executor::ExecutorState::FAILED) << executor.errorMessage();
  EXPECT_NE(executor.errorMessage().find("division by zero"), std::string::npos);
  auto* runner = executor.veloxTaskRunner();
  ASSERT_NE(runner, nullptr);
  ASSERT_NE(runner->veloxTask(), nullptr);
  uint64_t scanned_rows = 0;
  for (const auto& pipeline : runner->veloxTask()->taskStats().pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      if (op.operatorType == "TableScan") scanned_rows += op.rawInputPositions;
    }
  }
  ASSERT_EQ(scanned_rows, 32);
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
  executor.release();
  ASSERT_TRUE(executor.hasFinalTaskStats());
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
  EXPECT_EQ(report(executor).usage().columnar_conversion_copied_bytes(), expected);
}

TEST_F(LanceFilterExecutionTest, FilterReducesOutputRows) {
  const auto path = writeFilterableDataset();
  substrait::Expression boundary;
  boundary.mutable_literal()->set_fp64(100.0);
  auto plan = filterPlan(ROW({"id", "category", "score"}, {BIGINT(), VARCHAR(), DOUBLE()}),
      predicate(0, field(2), boundary), {"gt"});
  std::vector<int64_t> ids;
  for (const auto& batch : execute(path, plan, 1024)) {
    DecodedVector id(*batch->childAt(0));
    DecodedVector score(*batch->childAt(2));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      ids.push_back(id.valueAt<int64_t>(row));
      EXPECT_DOUBLE_EQ(score.valueAt<double>(row), 40.0 + ids.back() * 10.0);
      EXPECT_GT(score.valueAt<double>(row), 100.0);
    }
  }
  EXPECT_EQ(ids, (std::vector<int64_t>{7, 8, 9, 10}));
}

TEST_F(LanceFilterExecutionTest, FilterByStringEquality) {
  const auto path = writeFilterableDataset();
  substrait::Expression category;
  category.mutable_literal()->set_string("tech");
  auto plan = filterPlan(ROW({"id", "category", "score"}, {BIGINT(), VARCHAR(), DOUBLE()}),
      predicate(0, field(1), category), {"equal"});
  std::vector<int64_t> ids;
  for (const auto& batch : execute(path, plan, 1024)) {
    DecodedVector id(*batch->childAt(0));
    DecodedVector value(*batch->childAt(1));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      ids.push_back(id.valueAt<int64_t>(row));
      EXPECT_EQ(value.valueAt<StringView>(row).str(), "tech");
    }
  }
  EXPECT_EQ(ids, (std::vector<int64_t>{1, 4, 7, 10}));
}

TEST_F(LanceFilterExecutionTest, BoundedBatchesAndLateConfigurationCannotChangeCompiledPredicate) {
  const auto path = writeSimpleDataset(10);
  auto plan = filterPlan(ROW({"id", "name"}, {BIGINT(), VARCHAR()}),
      predicate(0, field(0), integer(1)), {"gt"});
  std::vector<int64_t> ids;
  for (const auto& batch : execute(path, plan, 3, true)) {
    DecodedVector id(*batch->childAt(0));
    for (vector_size_t row = 0; row < batch->size(); ++row) ids.push_back(id.valueAt<int64_t>(row));
  }
  EXPECT_EQ(ids, (std::vector<int64_t>{2, 3, 4, 5, 6, 7, 8, 9, 10}));
  rejectLateProjection(path);
}

TEST_F(LanceFilterExecutionTest, PhysicalPositionsPreserveProjectionFilterAndBatchLimits) {
  const auto path = writeSimpleDataset(10);
  const std::string position(pangrova::velox::kPhysicalRowPositionColumnName);
  const std::string source(pangrova::velox::kSourceObjectIdColumnName);
  const auto schema = ROW({"id", "name", position, source}, {BIGINT(), VARCHAR(), BIGINT(), VARCHAR()});
  auto condition = predicate(1, predicate(0, field(1), integer(2)), predicate(0, field(1), integer(5)));
  auto plan = filterPlan(schema, condition, {"equal", "or"}, {2, 0, 3});
  const auto expected_source = partitions(path).at(0).data_object_id;
  std::vector<std::pair<int64_t, int64_t>> values;
  for (const auto& batch : execute(path, plan, 1)) {
    ASSERT_TRUE(batch->type()->equivalent(*ROW({position, "id", source}, {BIGINT(), BIGINT(), VARCHAR()})));
    DecodedVector physical(*batch->childAt(0));
    DecodedVector id(*batch->childAt(1));
    DecodedVector object(*batch->childAt(2));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      values.emplace_back(physical.valueAt<int64_t>(row), id.valueAt<int64_t>(row));
      EXPECT_EQ(object.valueAt<StringView>(row).str(), expected_source);
    }
  }
  EXPECT_EQ(values, (std::vector<std::pair<int64_t, int64_t>>{{1, 2}, {4, 5}}));
  const auto storage = pangrova::test::testStorageBinding(path);
  const auto partition = pangrova::test::testScanPage(path, datasetVersion(path), storage).partitions(0);
  LanceBatchReader reader(pool_.get(), ROW({position, "id"}, {BIGINT(), BIGINT()}), path,
      datasetVersion(path), {position, "id"}, storage, pangrova::test::testReadDetail(partition), 0);
  ASSERT_NE(reader.next(1), nullptr);
  EXPECT_THROW(reader.lastBatchRowStart(), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::lance::test
