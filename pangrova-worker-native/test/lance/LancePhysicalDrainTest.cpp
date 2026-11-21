#include "LancePhysicalWriteFixture.h"

#include <folly/ScopeGuard.h>

#include "lance/LanceStorageContext.h"
#include "lance_c_ffi.h"

namespace pangrova::lance::test {
namespace {
TEST_F(LancePhysicalWriteFixture, ActualDrainRetainsHandleAndDestroyRefusesUnjoinedOwners) {
  const auto allocation = config();
  const auto binding = bytes(allocation.binding);
  const LanceCPhysicalWriterOptions options{
      reinterpret_cast<const uint8_t*>(binding.data()), binding.size(),
      allocation.taskAttempt.data(), allocation.taskAttempt.size(), allocation.tenantId.c_str(),
      allocation.spillDirectory.c_str(), allocation.maxSpillBytes, allocation.maxObjectWriteBytes};
  LanceStorageContext storage(allocation.storageBinding);
  auto* writer = lance_ffi_physical_writer_open(storage.get(), &options);
  ASSERT_NE(writer, nullptr);
  auto owner = folly::makeGuard([&] {
    if (writer) {
      LanceCWriteDrainEvidence evidence{};
      EXPECT_EQ(lance_ffi_physical_writer_drain(writer, &evidence), 0);
      EXPECT_EQ(lance_ffi_physical_writer_destroy(writer), 0);
    }
  });
  EXPECT_NE(lance_ffi_physical_writer_destroy(writer), 0);
  LanceCWriteDrainEvidence evidence{};
  ASSERT_EQ(lance_ffi_physical_writer_drain(writer, &evidence), 0);
  EXPECT_EQ(evidence.encoders_and_uploads_joined, 1);
  EXPECT_EQ(evidence.private_stage_cleaned, 1);
  EXPECT_EQ(evidence.staging_publication, 1); // Real SDK Started, no issued PUT.
  LanceCBuffer usage{};
  ASSERT_EQ(lance_ffi_physical_writer_resource_usage(writer, &usage), 0);
  lance_ffi_buffer_free(usage);
  EXPECT_EQ(lance_ffi_physical_writer_drain(writer, &evidence), 0);
  ASSERT_EQ(lance_ffi_physical_writer_destroy(writer), 0);
  writer = nullptr;
  EXPECT_FALSE(std::filesystem::exists(allocation.binding.files(0).opaque_location()));
  EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
}

TEST_F(LancePhysicalWriteFixture, ActualFilesystemCleanupFailureRetainsRetryOwner) {
  const auto allocation = config();
  const auto binding = bytes(allocation.binding);
  const LanceCPhysicalWriterOptions options{
      reinterpret_cast<const uint8_t*>(binding.data()), binding.size(),
      allocation.taskAttempt.data(), allocation.taskAttempt.size(), allocation.tenantId.c_str(),
      allocation.spillDirectory.c_str(), allocation.maxSpillBytes, allocation.maxObjectWriteBytes};
  LanceStorageContext storage(allocation.storageBinding);
  auto* writer = lance_ffi_physical_writer_open(storage.get(), &options);
  ASSERT_NE(writer, nullptr);
  const auto stage = std::filesystem::directory_iterator(directory_ / "stage")->path();
  const auto retained = directory_ / "retained-private-stage";
  std::filesystem::rename(stage, retained);
  {
    std::ofstream blocker(stage);
    ASSERT_TRUE(blocker.good());
    blocker << "actual filesystem fault: not a directory";
    blocker.close();
    ASSERT_FALSE(blocker.fail());
  }
  auto restore = [&] {
    if (std::filesystem::exists(retained)) {
      std::filesystem::remove(stage);
      std::filesystem::rename(retained, stage);
    }
  };
  auto owner = folly::makeGuard([&] {
    restore();
    if (writer) {
      LanceCWriteDrainEvidence evidence{};
      EXPECT_EQ(lance_ffi_physical_writer_drain(writer, &evidence), 0);
      EXPECT_EQ(lance_ffi_physical_writer_destroy(writer), 0);
    }
  });
  LanceCWriteDrainEvidence evidence{};
  EXPECT_NE(lance_ffi_physical_writer_drain(writer, &evidence), 0);
  EXPECT_EQ(evidence.encoders_and_uploads_joined, 1);
  EXPECT_EQ(evidence.private_stage_cleaned, 0);
  EXPECT_EQ(evidence.staging_publication, 1);
  EXPECT_NE(lance_ffi_physical_writer_destroy(writer), 0);
  LanceCBuffer usage{};
  ASSERT_EQ(lance_ffi_physical_writer_resource_usage(writer, &usage), 0);
  lance_ffi_buffer_free(usage);
  restore();
  ASSERT_EQ(lance_ffi_physical_writer_drain(writer, &evidence), 0);
  EXPECT_EQ(evidence.private_stage_cleaned, 1);
  ASSERT_EQ(lance_ffi_physical_writer_destroy(writer), 0);
  writer = nullptr;
  EXPECT_TRUE(std::filesystem::is_empty(directory_ / "stage"));
}
}  // namespace
}  // namespace pangrova::lance::test
