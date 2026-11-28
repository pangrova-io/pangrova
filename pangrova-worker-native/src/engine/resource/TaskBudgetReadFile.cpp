#include "engine/resource/TaskBudgetReadFile.h"

#include <algorithm>
#include <array>
#include <exception>
#include <limits>
#include <stdexcept>
#include <openssl/crypto.h>
#include <openssl/evp.h>

#include "folly/executors/InlineExecutor.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::resource {
namespace {
void checkpoint(const folly::CancellationToken& token) {
  if (token.isCancellationRequested()) {
    throw std::runtime_error("Task backend read cancelled");
  }
}
uint64_t fileSize(const std::shared_ptr<facebook::velox::ReadFile>& input,
    const std::shared_ptr<TaskObjectReadBudget>& budget, const folly::CancellationToken& cancellation) {
  if (!input || !budget) throw std::invalid_argument("Task backend requires its admitted read owner");
  checkpoint(cancellation);
  auto attempt = budget->begin(0, TaskObjectReadBudget::Operation::METADATA);
  attempt.startCall();
  const auto size = input->size();
  attempt.completeCall(0);
  checkpoint(cancellation);
  return size;
}
uint64_t addLength(uint64_t total, uint64_t length) {
  if (length > std::numeric_limits<uint64_t>::max() - total) {
    throw std::runtime_error("Task backend read range overflows");
  }
  return total + length;
}
uint64_t requested(const std::vector<folly::Range<char*>>& buffers) {
  uint64_t bytes = 0;
  for (const auto& buffer : buffers) bytes = addLength(bytes, buffer.size());
  return bytes;  // Includes gaps that the provider may coalesce over.
}
}  // namespace

std::shared_ptr<TaskBudgetReadFile> TaskBudgetReadFile::open(
    facebook::velox::filesystems::FileSystem& filesystem, std::string_view path,
    const std::shared_ptr<TaskObjectReadBudget>& budget, const folly::CancellationToken& cancellation,
    std::optional<int64_t> file_size) {
  checkpoint(cancellation);
  auto opening = budget->begin(0, TaskObjectReadBudget::Operation::METADATA);
  opening.startCall();
  facebook::velox::filesystems::FileOptions options;
  options.fileSize = file_size;
  auto file = filesystem.openFileForRead(path, options);
  opening.completeCall(0);
  return std::make_shared<TaskBudgetReadFile>(std::move(file), budget, cancellation);
}

TaskBudgetReadFile::TaskBudgetReadFile(
    std::shared_ptr<facebook::velox::ReadFile> input,
    std::shared_ptr<TaskObjectReadBudget> budget, folly::CancellationToken cancellation)
    : input_(std::move(input)), budget_(std::move(budget)), cancellation_(std::move(cancellation)),
      size_(fileSize(input_, budget_, cancellation_)) {}

void TaskBudgetReadFile::verifySha256(std::string_view expected, uint64_t declaredLength) const {
  checkpoint(cancellation_);
  if (expected.size() != 32 || declaredLength != size_) {
    throw std::runtime_error("Task file differs from its pinned length or SHA-256 declaration");
  }
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
  if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1) {
    throw std::runtime_error("Cannot initialize task file qualification");
  }
  std::array<char, 64 * 1024> buffer;
  for (uint64_t offset = 0; offset < declaredLength;) {
    const auto length = std::min<uint64_t>(buffer.size(), declaredLength - offset);
    auto bytes = pread(offset, length, buffer.data());
    if (bytes.size() != length || EVP_DigestUpdate(digest.get(), bytes.data(), bytes.size()) != 1) {
      throw std::runtime_error("Task file qualification read was truncated");
    }
    offset += length;
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> actual{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(digest.get(), actual.data(), &size) != 1 || size != expected.size() ||
      CRYPTO_memcmp(actual.data(), expected.data(), size) != 0) {
    throw std::runtime_error("Task file differs from its pinned SHA-256");
  }
  checkpoint(cancellation_);
}

std::string_view TaskBudgetReadFile::pread(uint64_t offset, uint64_t length,
    void* buffer, const facebook::velox::FileIoContext& context) const {
  checkpoint(cancellation_);
  addLength(offset, length);
  auto attempt = budget_->begin(length);
  attempt.startCall();
  auto output = input_->pread(offset, length, buffer, context);
  attempt.completeCall(output.size());
  checkpoint(cancellation_);
  return output;
}

uint64_t TaskBudgetReadFile::preadv(uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const facebook::velox::FileIoContext& context) const {
  checkpoint(cancellation_);
  const auto bytes = requested(buffers);
  addLength(offset, bytes);
  auto attempt = budget_->begin(bytes);
  attempt.startCall();
  const auto output = input_->preadv(offset, buffers, context);
  attempt.completeCall(output);
  checkpoint(cancellation_);
  return output;
}

uint64_t TaskBudgetReadFile::preadv(
    folly::Range<const facebook::velox::common::Region*> regions,
    folly::Range<folly::IOBuf*> buffers,
    const facebook::velox::FileIoContext& context) const {
  checkpoint(cancellation_);
  uint64_t bytes = 0;
  for (const auto& region : regions) {
    addLength(region.offset, region.length);
    bytes = addLength(bytes, region.length);
  }
  if (regions.size() != buffers.size()) {
    throw std::invalid_argument("Task backend regions require one output buffer per range");
  }
  auto attempt = budget_->begin(bytes);
  uint64_t output = 0;
  // The SDK regions overload permits undisclosed coalesced gap bytes. Issue
  // these exact admitted ranges through the same backend instead, so this
  // owner cannot expand its grant through provider-specific coalescing.
  for (size_t index = 0; index < regions.size(); ++index) {
    checkpoint(cancellation_);
    const auto& region = regions[index];
    buffers[index] = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    attempt.startCall();
    const auto read = input_->pread(region.offset, region.length, buffers[index].writableData(), context);
    attempt.completeCall(read.size());
    if (read.size() != region.length) {
      throw std::runtime_error("Task backend returned a truncated admitted region");
    }
    buffers[index].append(read.size());
    output = addLength(output, read.size());
  }
  checkpoint(cancellation_);
  return output;
}

folly::SemiFuture<uint64_t> TaskBudgetReadFile::preadvAsync(uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const facebook::velox::FileIoContext& context) const {
  try {
    checkpoint(cancellation_);
    if (input_->hasPreadvAsync()) {
      throw std::runtime_error("Task backend native async reads require an admitted buffer owner");
    }
    const auto bytes = requested(buffers);
    addLength(offset, bytes);
    auto attempt = budget_->begin(bytes);
    // SDK hasPreadvAsync=false guarantees this branch is synchronous. Never
    // delegate a native async read whose caller-owned buffers may be released.
    attempt.startCall();
    return input_->preadvAsync(offset, buffers, context)
        .via(&folly::InlineExecutor::instance())
        .thenTry([attempt = std::move(attempt), input = input_, token = cancellation_]
            (folly::Try<uint64_t>&& outcome) mutable {
          (void)input;
          const auto bytes = outcome.value();
          attempt.completeCall(bytes);
          checkpoint(token);
          return bytes;
        }).semi();
  } catch (...) {
    return folly::makeSemiFuture<uint64_t>(folly::exception_wrapper(std::current_exception()));
  }
}

}  // namespace pangrova::resource
