#include "../TestMemoryHelper.h"

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::columnar {
namespace {
template <typename CData>
struct ReleaseObservation {
  CData actual;
  std::shared_ptr<std::atomic<uint32_t>> count;
  static void release(CData* slot) noexcept {
    auto owner = std::unique_ptr<ReleaseObservation>(static_cast<ReleaseObservation*>(slot->private_data));
    slot->release = nullptr;
    owner->actual.release(&owner->actual);
    owner->count->fetch_add(1);
  }
  static void install(CData& slot, const std::shared_ptr<std::atomic<uint32_t>>& count) {
    auto owner = std::make_unique<ReleaseObservation>(ReleaseObservation{slot, count});
    slot.private_data = owner.get();
    slot.release = release;
    owner.release(); // One C Data owner; observes the actual export release callback.
  }
};

struct ObservedExport {
  ArrowArray array{};
  ArrowSchema schema{};
  std::shared_ptr<std::atomic<uint32_t>> arrays = std::make_shared<std::atomic<uint32_t>>(0);
  std::shared_ptr<std::atomic<uint32_t>> schemas = std::make_shared<std::atomic<uint32_t>>(0);
  explicit ObservedExport(const arrow::RecordBatch& batch) {
    auto status = arrow::ExportRecordBatch(batch, &array, &schema);
    if (!status.ok()) throw std::runtime_error(status.ToString());
    ReleaseObservation<ArrowArray>::install(array, arrays);
    ReleaseObservation<ArrowSchema>::install(schema, schemas);
  }
  ~ObservedExport() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

std::shared_ptr<arrow::Array> actualDoubles() {
  arrow::DoubleBuilder builder;
  EXPECT_TRUE(builder.Append(1.0000000001).ok());
  EXPECT_TRUE(builder.AppendNull().ok());
  return builder.Finish().ValueOrDie();
}

TEST(ColumnarImportOwnershipTest, ActualChildBuffersKeepBothCDataOwnersUntilLastRelease) {
  auto pool = pangrova::test::testLeafPool("c-data-owner");
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::float64(), true)}),
      2, {actualDoubles()});
  ObservedExport exported(*batch);
  const auto* sourceValues = exported.array.children[0]->buffers[1];
  const auto* sourceNulls = exported.array.children[0]->buffers[0];
  auto childArrays = std::make_shared<std::atomic<uint32_t>>(0);
  auto childSchemas = std::make_shared<std::atomic<uint32_t>>(0);
  ReleaseObservation<ArrowArray>::install(*exported.array.children[0], childArrays);
  ReleaseObservation<ArrowSchema>::install(*exported.schema.children[0], childSchemas);
  auto vector = facebook::velox::importFromArrowAsOwner(exported.schema, exported.array, pool.get());
  ASSERT_EQ(exported.array.release, nullptr);
  ASSERT_EQ(exported.schema.release, nullptr);
  batch.reset();
  auto child = vector->as<facebook::velox::RowVector>()->childAt(0);
  ASSERT_NE(child->nulls(), nullptr);
  EXPECT_EQ(child->asFlatVector<double>()->rawValues(), sourceValues);
  EXPECT_EQ(child->rawNulls(), sourceNulls);
  vector.reset();
  // A non-null ROW has no shared buffers. Its children own the actual
  // zero-copy payload and bitmap, through separate recursive C Data owners.
  EXPECT_EQ(exported.arrays->load(), 1);
  EXPECT_EQ(exported.schemas->load(), 1);
  EXPECT_EQ(childArrays->load(), 0);
  EXPECT_EQ(childSchemas->load(), 0);
  EXPECT_DOUBLE_EQ(child->asFlatVector<double>()->valueAt(0), 1.0000000001);
  EXPECT_TRUE(child->isNullAt(1));
  child.reset();
  EXPECT_EQ(exported.arrays->load(), 1);
  EXPECT_EQ(exported.schemas->load(), 1);
  EXPECT_EQ(childArrays->load(), 1);
  EXPECT_EQ(childSchemas->load(), 1);
}

TEST(ColumnarImportOwnershipTest, UnsupportedActualLayoutConsumesBothOwnersOnceOnFailure) {
  auto pool = pangrova::test::testLeafPool("unsupported-c-data-owner");
  auto fixed = arrow::FixedSizeListArray::FromArrays(actualDoubles(), 2).ValueOrDie();
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("embedding", fixed->type(), true)}), 1, {fixed});
  ObservedExport exported(*batch);
  EXPECT_THROW(facebook::velox::importFromArrowAsOwner(exported.schema, exported.array, pool.get()),
      facebook::velox::VeloxException);
  EXPECT_EQ(exported.array.release, nullptr);
  EXPECT_EQ(exported.schema.release, nullptr);
  EXPECT_EQ(exported.arrays->load(), 1);
  EXPECT_EQ(exported.schemas->load(), 1);
}

TEST(ColumnarImportOwnershipTest, ActualTaskCapFailureDuringListSizesReleasesEachOwnerOnce) {
  constexpr int64_t kTaskBytes = 1024 * 1024;
  constexpr int64_t kRows = 300000;
  pangrova::test::testMemoryManager();
  auto* governor = pangrova::jni::globalVeloxMemoryGovernor();
  ASSERT_NE(governor, nullptr);
  const auto activeBefore = governor->getMemorySnapshot().active_pool_count;
  const auto allocatorBefore = governor->usedMemory();
  const auto poolsBefore = governor->veloxMemoryManager()->numPools();
  auto root = governor->createTaskPool("import-owner-cap", "task-0", kTaskBytes);
  auto pool = root->addLeafChild("columnar-import");
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = root;
  std::weak_ptr<facebook::velox::memory::MemoryPool> leafOwner = pool;
  auto task = folly::makeGuard([&] {
    pool.reset();
    root.reset();
  });
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, activeBefore + 1);
  arrow::Int32Builder offsets;
  ASSERT_TRUE(offsets.AppendValues(std::vector<int32_t>(kRows + 1, 0)).ok());
  arrow::DoubleBuilder values;
  auto lists = arrow::ListArray::FromArrays(*offsets.Finish().ValueOrDie(),
      *values.Finish().ValueOrDie()).ValueOrDie();
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("arrays", lists->type(), true)}), kRows, {lists});
  ObservedExport exported(*batch);
  bool rejected = false;
  try {
    (void)facebook::velox::importFromArrowAsOwner(exported.schema, exported.array, pool.get());
  } catch (const facebook::velox::VeloxRuntimeError& error) {
    rejected = true;
    EXPECT_EQ(error.errorCode(), facebook::velox::error_code::kMemCapExceeded);
  }
  ASSERT_TRUE(rejected);
  EXPECT_EQ(exported.array.release, nullptr);
  EXPECT_EQ(exported.schema.release, nullptr);
  EXPECT_EQ(exported.arrays->load(), 1);
  EXPECT_EQ(exported.schemas->load(), 1);
  EXPECT_EQ(pool->usedBytes(), 0);
  root.reset();
  EXPECT_FALSE(rootOwner.expired());
  EXPECT_FALSE(leafOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, activeBefore + 1);
  pool.reset();
  EXPECT_TRUE(leafOwner.expired());
  EXPECT_TRUE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, activeBefore);
  EXPECT_EQ(governor->usedMemory(), allocatorBefore);
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), poolsBefore);
  EXPECT_EQ(exported.arrays->load(), 1);
  EXPECT_EQ(exported.schemas->load(), 1);
}
}  // namespace
}  // namespace pangrova::columnar
