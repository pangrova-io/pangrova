#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <thread>

#include "folly/CancellationToken.h"
#include "resource/TaskBudgetReadFile.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::resource::test {
namespace {

class TaskObjectReadBudgetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    facebook::velox::filesystems::registerLocalFileSystem();
    path_ = std::filesystem::temp_directory_path() / ("pangrova-task-read-" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count()));
    std::ofstream output(path_, std::ios::binary);
    output << "0123456789abcdefghijklmnopqrstuvwxyz";
    output.close();
    ASSERT_TRUE(output.good());
  }
  void TearDown() override { std::filesystem::remove(path_); }
  std::shared_ptr<facebook::velox::ReadFile> open() {
    return facebook::velox::filesystems::getFileSystem(path_.string(), nullptr)
        ->openFileForRead(path_.string());
  }
  std::filesystem::path path_;
};

TEST_F(TaskObjectReadBudgetTest, DownloadQualificationAndHydrationShareOneActualReadGrant) {
  auto owner = std::make_shared<TaskObjectReadBudget>(24);
  TaskBudgetReadFile download(open(), owner, {});
  TaskBudgetReadFile qualification(open(), owner, {});
  TaskBudgetReadFile hydration(open(), owner, {});
  std::array<char, 8> buffer{};
  EXPECT_EQ(download.pread(0, 8, buffer.data()), "01234567");
  EXPECT_EQ(qualification.pread(8, 8, buffer.data()), "89abcdef");
  EXPECT_EQ(hydration.pread(16, 8, buffer.data()), "ghijklmn");
  EXPECT_EQ(owner->requestedBytes(), 24);
  EXPECT_EQ(owner->completedReturnedBytes(), 24);
  EXPECT_THROW(hydration.pread(24, 1, buffer.data()), std::runtime_error);
  EXPECT_EQ(owner->requestedBytes(), 24);
  EXPECT_EQ(owner->activeReads(), 0);
}

TEST_F(TaskObjectReadBudgetTest, ActualDiscontiguousRegionsReadOnlyTheirAdmittedRanges) {
  auto owner = std::make_shared<TaskObjectReadBudget>(8);
  TaskBudgetReadFile input(open(), owner, {});
  const std::array<facebook::velox::common::Region, 2> regions{{{0, 4}, {24, 4}}};
  std::array<folly::IOBuf, 2> output;
  EXPECT_EQ(input.preadv(folly::Range(regions.data(), regions.size()),
      folly::Range(output.data(), output.size())), 8);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(output[0].data()), output[0].length()), "0123");
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(output[1].data()), output[1].length()), "opqr");
  EXPECT_EQ(owner->requestedBytes(), 8);
  EXPECT_EQ(owner->completedReturnedBytes(), 8);
  EXPECT_EQ(owner->activeReads(), 0);
}

TEST_F(TaskObjectReadBudgetTest, ActualSdkAsyncCompletionReleasesOwnerWithoutConsumerPolling) {
  auto owner = std::make_shared<TaskObjectReadBudget>(16);
  auto backend = open();
  ASSERT_FALSE(backend->hasPreadvAsync());  // Genuine fixed-SDK LocalReadFile contract.
  TaskBudgetReadFile input(std::move(backend), owner, {});
  EXPECT_FALSE(input.hasPreadvAsync());
  std::array<char, 8> first{};
  std::array<char, 8> second{};
  auto future = input.preadvAsync(0, {folly::Range(first.data(), first.size()),
      folly::Range(second.data(), second.size())});
  // LocalReadFile's genuine SDK implementation completes synchronously. This
  // checks the completion callback ran without consuming or cancelling its future.
  EXPECT_EQ(owner->activeReads(), 0);
  EXPECT_EQ(owner->requestedBytes(), 16);
  EXPECT_EQ(owner->completedReturnedBytes(), 16);
  EXPECT_EQ(std::move(future).get(), 16);
  EXPECT_EQ(std::string(first.data(), first.size()), "01234567");
  EXPECT_EQ(std::string(second.data(), second.size()), "89abcdef");
}

TEST_F(TaskObjectReadBudgetTest, CancellationNeverStartsARealBackendReadOrLeaksRejectedAttempt) {
  auto owner = std::make_shared<TaskObjectReadBudget>(8);
  folly::CancellationSource cancellation;
  TaskBudgetReadFile input(open(), owner, cancellation.getToken());
  cancellation.requestCancellation();
  std::array<char, 8> buffer{};
  EXPECT_THROW(input.pread(0, 8, buffer.data()), std::runtime_error);
  EXPECT_EQ(owner->requestedBytes(), 0);
  EXPECT_EQ(owner->completedReturnedBytes(), 0);
  owner->requestCancel();
  EXPECT_THROW(owner->begin(1), std::runtime_error);
  EXPECT_EQ(owner->activeReads(), 0);
  owner->stopAndDrain();
}

TEST_F(TaskObjectReadBudgetTest, DrainWaitsForTheActualReadAttemptOwnerRatherThanCancellingItsFuture) {
  auto owner = std::make_shared<TaskObjectReadBudget>(8);
  auto file = open();
  std::promise<void> read_completed;
  std::promise<void> release_owner;
  auto released = release_owner.get_future();
  auto reading = std::async(std::launch::async, [&] {
    auto attempt = owner->begin(8);
    std::array<char, 8> buffer{};
    auto bytes = file->pread(0, 8, buffer.data());
    attempt->complete(bytes.size());
    read_completed.set_value();
    released.wait();  // The genuine operation owner still retains its buffers.
  });
  read_completed.get_future().get();
  EXPECT_EQ(owner->activeReads(), 1);
  auto draining = std::async(std::launch::async, [&] { owner->stopAndDrain(); });
  EXPECT_EQ(draining.wait_for(std::chrono::milliseconds(25)), std::future_status::timeout);
  release_owner.set_value();
  reading.get();
  draining.get();
  EXPECT_EQ(owner->activeReads(), 0);
  EXPECT_EQ(owner->completedReturnedBytes(), 8);
  EXPECT_THROW(owner->begin(1), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::resource::test
