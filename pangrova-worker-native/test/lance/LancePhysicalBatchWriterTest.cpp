#include "LancePhysicalWriteFixture.h"

#include <memory>

#include "velox/common/base/Exceptions.h"
#include "velox/memory/VeloxMemoryGovernor.h"

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
