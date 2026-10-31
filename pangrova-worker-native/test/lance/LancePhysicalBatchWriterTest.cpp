#include "CanonicalTestAttempt.h"
#include "LancePhysicalWriteFixture.h"

#include <memory>
#include <thread>

#include <arrow/c/bridge.h>
#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/ipc/reader.h>
#include "engine/columnar/ColumnarImportLayout.h"
#include "engine/executor/driver/TaskExecutor.h"
#include "bridge/command/StatusSerializer.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "substrait/plan.pb.h"
#include "velox/vector/arrow/Bridge.h"
#include "engine/write/WriteArrowExport.h"

#include "velox/common/base/Exceptions.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"

namespace pangrova::lance::test {
namespace {

void expectExplicitFailureDrain(LancePhysicalBatchWriter& writer, bool cleanupReportsFailure) {
  if (cleanupReportsFailure) {
    try {
      writer.close();
      FAIL() << "The actual SDK staging failure must remain visible after drain";
    } catch (const std::runtime_error& error) {
      EXPECT_NE(std::string(error.what()).find("Physical staging byte grant exhausted"), std::string::npos);
    }
  } else {
    ASSERT_NO_THROW(writer.close());
  }
  // This value is populated by the actual C ABI drain, not destructor entry.
  ASSERT_TRUE(writer.isDrainedAndCleaned());
  ASSERT_NO_THROW(writer.releaseDrained());
  EXPECT_NO_THROW(writer.close());
  EXPECT_THROW(writer.finish(), std::runtime_error);
}

void expectStagingGrantFailure(LancePhysicalBatchWriter& writer, const RowVectorPtr& input) {
  bool rejected = false;
  try {
    writer.writeBatch(input);
    (void)writer.finish();
  } catch (const std::exception& error) {
    rejected = true;
    EXPECT_NE(std::string(error.what()).find("Physical staging byte grant exhausted"), std::string::npos);
  }
  ASSERT_TRUE(rejected);
}

TEST_F(LancePhysicalWriteFixture, RealPhysicalFileRoundTripPreservesDoubleAndBothNullKinds) {
  auto allocation = config();
  const auto uri = allocation.binding.files(0).opaque_location();
  const auto original = batch();
  const auto baseline = pool_->usedBytes();
  LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
  writer.writeBatch(original);
  const auto copied_bytes = writer.resourceUsage().columnar_conversion_copied_bytes();
  EXPECT_GT(copied_bytes, 0);
  EXPECT_FALSE(std::filesystem::exists(uri));
  auto bytes = writer.finish();
  EXPECT_EQ(writer.resourceUsage().columnar_conversion_copied_bytes(), copied_bytes);
  proto::PhysicalFileWriteFragment fragment;
  ASSERT_TRUE(fragment.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  ASSERT_EQ(fragment.files_size(), 1);
  EXPECT_EQ(fragment.contract_version(), 1);
  EXPECT_EQ(fragment.input_rows(), 3);
  EXPECT_EQ(fragment.task_attempt().SerializeAsString(),
      allocation.binding.files(0).task_attempt().SerializeAsString());
  const auto& receipt = fragment.files(0);
  EXPECT_EQ(receipt.allocation().SerializeAsString(), allocation.binding.files(0).SerializeAsString());
  EXPECT_EQ(receipt.physical_rows(), 3);
  EXPECT_EQ(receipt.size_bytes(), std::filesystem::file_size(uri));
  EXPECT_EQ(receipt.encoding_major(), 0);
  EXPECT_EQ(receipt.encoding_minor(), 3);
  EXPECT_EQ(receipt.min_sequence(), 0);
  EXPECT_EQ(receipt.max_sequence(), 2);
  const auto object = actualFileBytes(uri);
  EXPECT_EQ(receipt.object_checksum().algorithm(), proto::Checksum::ALGORITHM_SHA256);
  EXPECT_EQ(receipt.object_checksum().digest(), checksum(object).digest());
  ASSERT_GE(object.size(), 8);
  EXPECT_EQ(object.substr(object.size() - 8), std::string("\0\0\3\0LANC", 8));
  EXPECT_GT(fragment.input_bytes(), 0);
  EXPECT_EQ(pool_->usedBytes(), baseline); // Actual uploaded payload owner was released.
  EXPECT_THROW(writer.finish(), std::runtime_error);
  EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
  proto::FileSplitDetail detail;
  detail.set_file_format(proto::FILE_FORMAT_LANCE);
  auto* scan = detail.mutable_physical_file_scan();
  scan->set_physical_rows(3);
  scan->set_row_end(3);
  auto named = ROW({"id", "embedding"}, {BIGINT(), ARRAY(DOUBLE())});
  LanceBatchReader reader(pool_.get(), named, uri, 1, {},
      allocation.storageBinding, detail.SerializeAsString(), receipt.size_bytes());
  auto actual = reader.next(16);
  ASSERT_NE(actual, nullptr);
  ASSERT_EQ(actual->size(), 3);
  EXPECT_EQ(actual->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 7);
  EXPECT_EQ(actual->childAt(0)->asFlatVector<int64_t>()->valueAt(2), 11);
  auto vectors = actual->childAt(1)->as<ArrayVector>();
  ASSERT_NE(vectors, nullptr);
  EXPECT_FALSE(vectors->isNullAt(0));
  EXPECT_TRUE(vectors->isNullAt(1));
  EXPECT_FALSE(vectors->isNullAt(2));
  EXPECT_EQ(vectors->sizeAt(0), 3);
  auto values = vectors->elements()->asFlatVector<double>();
  EXPECT_DOUBLE_EQ(values->valueAt(vectors->offsetAt(0)), 1.0000000001);
  EXPECT_TRUE(values->isNullAt(vectors->offsetAt(0) + 1));
  EXPECT_DOUBLE_EQ(values->valueAt(vectors->offsetAt(2) + 2), 5.0);
  EXPECT_GE(reader.columnarLayoutOffsetBytes(), 4 * sizeof(int32_t));
  EXPECT_EQ(reader.columnarConversionCopiedBytes(), 0); // Unsliced validity/value buffers share owners.
  EXPECT_EQ(reader.next(16), nullptr);
}

// The physical decoder may rebase fixed-list primitive buffers, so use an
// actual BOOLEAN page whose C ABI slices retain bitmap bit offsets.
class LancePhysicalCopyMetricsTest : public LancePhysicalWriteFixture {
 protected:
  PhysicalWriterConfig bitmapConfig() {
    auto allocation = config();
    std::shared_ptr<arrow::io::BufferReader> input =
        arrow::io::BufferReader::FromString(allocation.binding.physical_schema());
    auto source = arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();
    auto fields = source->schema()->fields();
    fields.push_back(arrow::field("flag", arrow::boolean(), true,
        arrow::key_value_metadata({"PARQUET:field_id"}, {"2"})));
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto encoder = arrow::ipc::MakeStreamWriter(sink, arrow::schema(fields)).ValueOrDie();
    if (!encoder->Close().ok()) throw std::runtime_error("Cannot encode bitmap schema");
    allocation.binding.set_physical_schema(sink->Finish().ValueOrDie()->ToString());
    *allocation.binding.mutable_physical_schema_checksum() = checksum(allocation.binding.physical_schema());
    auto* column = allocation.binding.add_columns();
    column->set_field_id(2);
    column->set_name("flag");
    column->set_nullable(true);
    column->mutable_logical_type()->set_canonical_sql("BOOLEAN");
    column->mutable_logical_type()->mutable_substrait_type()->mutable_bool_()->set_nullability(
        substrait::Type::NULLABILITY_NULLABLE);
    output_type_ = ROW({"c0", "c1", "c2"}, {BIGINT(), ARRAY(DOUBLE()), BOOLEAN()});
    return allocation;
  }

  RowVectorPtr bitmapBatch() {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto ids = maker.flatVector<int64_t>(16, [](vector_size_t row) { return row + 1; });
    std::vector<std::optional<std::vector<std::optional<double>>>> values;
    for (vector_size_t row = 0; row < 16; ++row) {
      if (row == 7) values.push_back(std::nullopt);
      else values.push_back(std::vector<std::optional<double>>{row + 0.25, row + 0.5, row + 0.75});
    }
    auto embeddings = maker.arrayVectorNullable<double>(values);
    auto flags = BaseVector::create(BOOLEAN(), 16, pool_.get());
    for (vector_size_t row = 0; row < 16; ++row) {
      flags->asFlatVector<bool>()->set(row, row % 2 == 0);
      if (row % 3 == 0) flags->setNull(row, true);
    }
    return std::make_shared<RowVector>(pool_.get(), output_type_, nullptr, 16,
        std::vector<VectorPtr>{ids, embeddings, flags});
  }

  uint64_t actualWriteCopyBytes(const RowVectorPtr& input) {
    auto normalized = pangrova::write::prepareWriteArrowExport(input);
    ArrowArray array{};
    ArrowSchema schema{};
    ArrowOptions options;
    options.flattenDictionary = true;
    options.flattenConstant = true;
    facebook::velox::exportToArrow(normalized, array, pool_.get(), options);
    facebook::velox::exportToArrow(normalized, schema, options);
    auto imported = arrow::ImportRecordBatch(&array, &schema).ValueOrDie();
    auto vectors = std::dynamic_pointer_cast<arrow::ListArray>(imported->column(1));
    if (!vectors || !vectors->IsNull(7) || vectors->value_length(7) != 0) {
      throw std::runtime_error("Actual NULL vector must omit its three physical slots");
    }
    // Arrow buffers borrow MemoryPool*: all temporary arrays must be destroyed
    // before this allocator. The imported input still owns its existing buffers.
    const auto baseline = pool_->usedBytes();
    auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
    uint64_t bytes = 0;
    {
      arrow::ArrayVector slots;
      for (int64_t row = 0; row < vectors->length(); ++row) {
        if (vectors->IsNull(row)) {
          slots.push_back(arrow::MakeArrayOfNull(arrow::float64(), 3, allocator.get()).ValueOrDie());
        } else {
          EXPECT_EQ(vectors->value_length(row), 3);
          slots.push_back(vectors->value_slice(row));
        }
      }
      // Independent official Arrow concatenation exposes the actual buffers
      // required to materialize omitted NULL slots; no hand-coded byte constant.
      auto payload = arrow::Concatenate(slots, allocator.get()).ValueOrDie();
      EXPECT_EQ(payload->length(), 48);
      EXPECT_EQ(payload->null_count(), 3);
      EXPECT_NE(payload->data()->buffers.at(1)->data(), vectors->values()->data()->buffers.at(1)->data());
      for (const auto& buffer : payload->data()->buffers) if (buffer) bytes += buffer->size();
      EXPECT_GT(allocator->bytes_allocated(), 0);
    }
    EXPECT_EQ(allocator->bytes_allocated(), 0);
    EXPECT_EQ(pool_->usedBytes(), baseline);
    return bytes;
  }
};

TEST_F(LancePhysicalCopyMetricsTest, ActualReadAndWriteCopiesSumInTaskReportAfterRelease) {
  auto allocation = bitmapConfig();
  const auto uri = allocation.binding.files(0).opaque_location();
  const auto input = bitmapBatch();
  const auto expected_write = actualWriteCopyBytes(input);
  ASSERT_GT(expected_write, 0);
  LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
  writer.writeBatch(input);
  const auto written = writer.resourceUsage().columnar_conversion_copied_bytes();
  ASSERT_GT(written, 0);
  ASSERT_EQ(written, expected_write);
  const auto receipt = writer.finish();
  proto::PhysicalFileWriteFragment fragment;
  ASSERT_TRUE(fragment.ParseFromArray(receipt.data(), static_cast<int>(receipt.size())));
  proto::FileSplitDetail detail;
  detail.set_file_format(proto::FILE_FORMAT_LANCE);
  auto* physical = detail.mutable_physical_file_scan();
  physical->set_physical_rows(16);
  physical->set_row_end(16);
  const auto detail_bytes = detail.SerializeAsString();
  const auto file_size = fragment.files(0).size_bytes();
  uint64_t read = 0;
  int sliced_bitmaps = 0;
  {
    LanceStorageContext storage(allocation.storageBinding);
    std::unique_ptr<LanceCReader, decltype(&lance_ffi_reader_close)> source(
        lance_ffi_reader_open(storage.get(), uri.c_str(), 1,
            reinterpret_cast<const uint8_t*>(detail_bytes.data()), detail_bytes.size(), file_size),
        lance_ffi_reader_close);
    ASSERT_NE(source, nullptr) << lance_ffi_last_error();
    auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
    for (int row = 0; row < 16; ++row) {
      ArrowArray array{};
      ArrowSchema schema{};
      ASSERT_EQ(lance_ffi_reader_next(source.get(), &array, &schema, 1), 1);
      auto imported = arrow::ImportRecordBatch(&array, &schema).ValueOrDie();
      const auto& flag = imported->column(2)->data();
      ASSERT_EQ(flag->type->id(), arrow::Type::BOOL);
      ASSERT_NE(flag->buffers.at(1), nullptr);
      pangrova::columnar::ColumnarImportUsage usage;
      auto normalized = pangrova::columnar::normalizeImportLayout(*imported, allocator, usage).ValueOrDie();
      ASSERT_EQ(normalized->num_rows(), 1);
      EXPECT_GT(usage.allocatedOffsetBytes, 0);
      if (flag->offset > 0) {
        ++sliced_bitmaps;
        EXPECT_GT(usage.copiedBytes, 0);
        EXPECT_EQ(normalized->column(2)->offset(), 0);
      }
      read += usage.copiedBytes;
    }
  }
  ASSERT_GT(sliced_bitmaps, 0);
  ASSERT_GT(read, 0);
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("id");
  root->add_names("embedding");
  root->add_names("flag");
  auto* relation = root->mutable_input()->mutable_read();
  relation->mutable_named_table()->add_names("physical_table");
  auto* schema = relation->mutable_base_schema();
  schema->add_names("id");
  schema->add_names("embedding");
  schema->add_names("flag");
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* list = schema->mutable_struct_()->add_types()->mutable_list();
  list->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  list->mutable_type()->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  pangrova::executor::TaskExecutorConfig task_config;
  task_config.task_id = "actual-physical-copy-task";
  task_config.query_id = "actual-physical-copy-query";
  task_config.function_registry = exec::FunctionRegistrySnapshot::capture();
  const auto serialized = plan.SerializeAsString();
  task_config.substrait_plan.assign(serialized.begin(), serialized.end());
  task_config.shuffle_batch_size = 1;
  task_config.resource_limit.max_threads = 1;
  pangrova::split::SplitConfig split;
  split.split_id = "actual-physical-file";
  split.connector_id = pangrova::velox::PangrovaVeloxConnector::kConnectorId;
  split.table_name = "physical_table";
  split.file_path = uri;
  split.file_format = pangrova::split::FileFormatType::LANCE;
  split.physical_lance_file = true;
  split.length = file_size;
  split.source_snapshot = "1";
  split.data_object_id = "actual-physical-file";
  split.storage_binding = allocation.storageBinding;
  split.file_scan_detail = detail_bytes;
  task_config.splits.push_back(std::move(split));
  pangrova::test::bindTestAttempt(task_config);
  pangrova::executor::TaskExecutor executor(task_config);
  executor.metricsCollector().observeColumnarConversionCopiedBytes(written);
  executor.start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (executor.state() == pangrova::executor::ExecutorState::RUNNING && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(executor.state(), pangrova::executor::ExecutorState::FINISHED) << executor.errorMessage();
  executor.release();
  for (int observation = 0; observation < 3; ++observation) {
    executor.metricsCollector().observeColumnarConversionCopiedBytes(written);
    const auto bytes = pangrova::jni::serializeTaskStatus(executor);
    proto::TaskReport report;
    ASSERT_TRUE(report.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
    EXPECT_EQ(report.usage().columnar_conversion_copied_bytes(), read + written);
    EXPECT_EQ(executor.metricsCollector().snapshot().columnar_conversion_copied_bytes, read + written);
  }
}

TEST_F(LancePhysicalWriteFixture, ConditionalCreateCannotOverwriteExistingActualFile) {
  auto allocation = config();
  const auto original = batch();
  const auto baseline = pool_->usedBytes();
  std::vector<uint8_t> first_receipt;
  {
    LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
    writer.writeBatch(original);
    first_receipt = writer.finish();
  }
  const auto uri = allocation.binding.files(0).opaque_location();
  const auto original_size = std::filesystem::file_size(uri);
  const auto original_object = actualFileBytes(uri);
  {
    LancePhysicalBatchWriter collision(pool_, output_type_, allocation);
    collision.writeBatch(original);
    EXPECT_THROW(collision.finish(), std::runtime_error);
    EXPECT_THROW(collision.finish(), std::runtime_error);
    ASSERT_NO_FATAL_FAILURE(expectExplicitFailureDrain(collision, false));
    EXPECT_EQ(pool_->usedBytes(), baseline);
  }
  EXPECT_EQ(pool_->usedBytes(), baseline);
  EXPECT_EQ(std::filesystem::file_size(uri), original_size);
  EXPECT_EQ(actualFileBytes(uri), original_object);
  EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
  EXPECT_FALSE(first_receipt.empty());
}

TEST_F(LancePhysicalWriteFixture, StagingGrantRejectsBeforePublicationAndFailureStaysPoisoned) {
  auto allocation = config();
  allocation.maxSpillBytes = 1;
  const auto original = batch();
  const auto baseline = pool_->usedBytes();
  const auto uri = allocation.binding.files(0).opaque_location();
  proto::ResourceUsage observed;
  allocation.resourceUsagePublisher = [&observed](const proto::ResourceUsage& usage) { observed = usage; };
  {
    LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
    // Encoding may fill the cache before the SDK writes; either real phase
    // must report the same hard staging refusal, never create a final object.
    bool rejected = false;
    try {
      writer.writeBatch(original);
      (void)writer.finish();
    } catch (const std::exception& error) {
      rejected = true;
      EXPECT_NE(std::string(error.what()).find("Physical staging byte grant exhausted"), std::string::npos);
    }
    EXPECT_TRUE(rejected);
    EXPECT_GT(writer.resourceUsage().columnar_conversion_copied_bytes(), 0);
    EXPECT_EQ(observed.columnar_conversion_copied_bytes(),
        writer.resourceUsage().columnar_conversion_copied_bytes());
    EXPECT_THROW(writer.finish(), std::runtime_error);
    ASSERT_NO_FATAL_FAILURE(expectExplicitFailureDrain(writer, true));
    EXPECT_EQ(pool_->usedBytes(), baseline);
  }
  EXPECT_EQ(pool_->usedBytes(), baseline);
  EXPECT_FALSE(std::filesystem::exists(uri));
  EXPECT_GT(observed.columnar_conversion_copied_bytes(), 0);
  EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
}

TEST_F(LancePhysicalWriteFixture, RepeatedConditionalCreateFailuresReleaseActualTaskPools) {
  const auto allocation = config();
  const auto original = batch();
  {
    LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
    writer.writeBatch(original);
    ASSERT_FALSE(writer.finish().empty());
  }
  const auto uri = allocation.binding.files(0).opaque_location();
  const auto published = actualFileBytes(uri);
  auto* manager = pangrova::test::testMemoryManager();
  const auto baseline_bytes = manager->getTotalBytes();
  const auto baseline_pools = manager->numPools();
  for (int attempt = 0; attempt < 32; ++attempt) {
    SCOPED_TRACE(attempt);
    auto root = manager->addRootPool("physical-create-retry-" + std::to_string(attempt), 1024 * 1024);
    auto leaf = root->addLeafChild("write-drain");
    std::weak_ptr<facebook::velox::memory::MemoryPool> root_owner = root;
    std::weak_ptr<facebook::velox::memory::MemoryPool> leaf_owner = leaf;
    const auto baseline = leaf->usedBytes();
    {
      LancePhysicalBatchWriter writer(leaf, output_type_, allocation);
      writer.writeBatch(original);
      EXPECT_THROW(writer.finish(), std::runtime_error);
      EXPECT_THROW(writer.finish(), std::runtime_error);
      ASSERT_NO_FATAL_FAILURE(expectExplicitFailureDrain(writer, false));
      EXPECT_EQ(leaf->usedBytes(), baseline);
    }
    leaf.reset();
    root.reset();
    EXPECT_TRUE(leaf_owner.expired());
    EXPECT_TRUE(root_owner.expired());
    EXPECT_EQ(manager->getTotalBytes(), baseline_bytes);
    EXPECT_EQ(manager->numPools(), baseline_pools);
    EXPECT_EQ(actualFileBytes(uri), published);
    EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
  }
}

TEST_F(LancePhysicalWriteFixture, RepeatedStagingGrantFailuresReleaseActualTaskPools) {
  auto allocation = config();
  allocation.maxSpillBytes = 1;
  const auto original = batch();
  const auto uri = allocation.binding.files(0).opaque_location();
  auto* manager = pangrova::test::testMemoryManager();
  const auto baseline_bytes = manager->getTotalBytes();
  const auto baseline_pools = manager->numPools();
  for (int attempt = 0; attempt < 32; ++attempt) {
    SCOPED_TRACE(attempt);
    auto root = manager->addRootPool("physical-stage-retry-" + std::to_string(attempt), 1024 * 1024);
    auto leaf = root->addLeafChild("write-drain");
    std::weak_ptr<facebook::velox::memory::MemoryPool> root_owner = root;
    std::weak_ptr<facebook::velox::memory::MemoryPool> leaf_owner = leaf;
    const auto baseline = leaf->usedBytes();
    {
      LancePhysicalBatchWriter writer(leaf, output_type_, allocation);
      ASSERT_NO_FATAL_FAILURE(expectStagingGrantFailure(writer, original));
      EXPECT_GT(writer.resourceUsage().columnar_conversion_copied_bytes(), 0);
      EXPECT_THROW(writer.finish(), std::runtime_error);
      ASSERT_NO_FATAL_FAILURE(expectExplicitFailureDrain(writer, true));
      EXPECT_EQ(leaf->usedBytes(), baseline);
    }
    leaf.reset();
    root.reset();
    EXPECT_TRUE(leaf_owner.expired());
    EXPECT_TRUE(root_owner.expired());
    EXPECT_EQ(manager->getTotalBytes(), baseline_bytes);
    EXPECT_EQ(manager->numPools(), baseline_pools);
    EXPECT_FALSE(std::filesystem::exists(uri));
    EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
  }
}

TEST_F(LancePhysicalWriteFixture, GenuineZeroInputEmitsNoPhysicalFile) {
  auto allocation = config();
  LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
  auto bytes = writer.finish();
  proto::PhysicalFileWriteFragment fragment;
  ASSERT_TRUE(fragment.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  EXPECT_EQ(fragment.input_rows(), 0);
  EXPECT_EQ(fragment.files_size(), 0);
  EXPECT_FALSE(std::filesystem::exists(allocation.binding.files(0).opaque_location()));
  EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
}

TEST_F(LancePhysicalWriteFixture, NullRootRowsCannotDisappearDuringRecordBatchExport) {
  auto allocation = config();
  auto original = batch();
  original->setNull(1, true);
  LancePhysicalBatchWriter writer(pool_, output_type_, allocation);
  EXPECT_THROW(writer.writeBatch(original), std::invalid_argument);
  EXPECT_THROW(writer.finish(), std::runtime_error);
  EXPECT_FALSE(std::filesystem::exists(allocation.binding.files(0).opaque_location()));
}

TEST_F(LancePhysicalWriteFixture, ActualTaskPoolCapRejectsClosedSdkPayloadBeforePublication) {
  constexpr int64_t kTaskBytes = 1024 * 1024;
  constexpr int32_t kDimension = 65535;
  auto allocation = config(kDimension);
  const auto uri = allocation.binding.files(0).opaque_location();
  // The already-produced input belongs to a different pool; the upload payload
  // must still obey the actual destination task's root cap.
  const auto original = largeBatch(kDimension);
  auto* governor = pangrova::jni::globalVeloxMemoryGovernor();
  ASSERT_NE(governor, nullptr);
  const auto active_before = governor->getMemorySnapshot().active_pool_count;
  std::weak_ptr<facebook::velox::memory::MemoryPool> task_root_owner;
  std::weak_ptr<facebook::velox::memory::MemoryPool> task_leaf_owner;
  {
    auto task_root = governor->createTaskPool("physical-payload-cap", "task-0", kTaskBytes);
    auto task_leaf = task_root->addLeafChild("write-drain");
    task_root_owner = task_root;
    task_leaf_owner = task_leaf;
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, active_before + 1);
    ASSERT_EQ(task_leaf->maxCapacity(), kTaskBytes);
    const auto baseline = task_leaf->usedBytes();
    {
      LancePhysicalBatchWriter writer(task_leaf, output_type_, allocation);
      ASSERT_NO_THROW(writer.writeBatch(original));
      bool memory_rejected = false;
      try {
        (void)writer.finish();
      } catch (const VeloxRuntimeError& error) {
        memory_rejected = true;
        EXPECT_EQ(error.errorCode(), error_code::kMemCapExceeded);
      }
      ASSERT_TRUE(memory_rejected);
      std::vector<std::filesystem::path> staged_files;
      for (const auto& entry : std::filesystem::recursive_directory_iterator(directory_ / "stage")) {
        if (entry.is_regular_file()) staged_files.push_back(entry.path());
      }
      ASSERT_EQ(staged_files.size(), 1);
      const auto staged = actualFileBytes(staged_files.front().string());
      ASSERT_GT(staged.size(), kTaskBytes);
      EXPECT_LE(staged.size(), allocation.maxSpillBytes);
      EXPECT_EQ(staged.substr(staged.size() - 8), std::string("\0\0\3\0LANC", 8));
      EXPECT_FALSE(std::filesystem::exists(uri));
      EXPECT_EQ(task_leaf->usedBytes(), baseline);
      EXPECT_THROW(writer.finish(), std::runtime_error);
    }
    EXPECT_EQ(task_leaf->usedBytes(), baseline);
    EXPECT_FALSE(std::filesystem::exists(uri));
    EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
    task_root.reset();
    EXPECT_FALSE(task_root_owner.expired());
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, active_before + 1);
  }
  EXPECT_TRUE(task_leaf_owner.expired());
  EXPECT_TRUE(task_root_owner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, active_before);
}

}  // namespace
}  // namespace pangrova::lance::test
