#include "velox/connector/PaimonBranchDirectoryReader.h"

#include <sys/stat.h>

#include <limits>
#include <stdexcept>
#include <utility>

#include "split/PaimonLocalFileUri.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {

int64_t modificationTimeMillis(const struct stat& status) {
#if defined(__APPLE__)
  const auto nanoseconds = status.st_mtimespec.tv_nsec;
#else
  const auto nanoseconds = status.st_mtim.tv_nsec;
#endif
  return static_cast<int64_t>(status.st_mtime) * 1000 + nanoseconds / 1000000;
}

}  // namespace

PaimonBranchDirectoryReader::PaimonBranchDirectoryReader(
    std::string uri, std::string name, int64_t expectedMtimeMs,
    uint64_t expectedDevice, uint64_t expectedInode,
    int32_t zoneOffsetSeconds,
    facebook::velox::RowTypePtr outputType,
    facebook::velox::memory::MemoryPool* pool)
    : uri_(std::move(uri)), name_(std::move(name)),
      expected_mtime_ms_(expectedMtimeMs), expected_device_(expectedDevice),
      expected_inode_(expectedInode), zone_offset_seconds_(zoneOffsetSeconds),
      output_type_(std::move(outputType)),
      pool_(pool) {
  if (name_.empty() || name_ == "." || name_ == ".." ||
      name_.find('/') != std::string::npos ||
      name_.find('\\') != std::string::npos ||
      name_.find('\0') != std::string::npos ||
      expected_mtime_ms_ < 0 || expected_inode_ == 0 ||
      zone_offset_seconds_ < -18 * 3600 || zone_offset_seconds_ > 18 * 3600 ||
      expected_mtime_ms_ > std::numeric_limits<int64_t>::max() - 18LL * 3600 * 1000 ||
      !output_type_ || !pool_) {
    throw std::invalid_argument("Invalid pinned Paimon branch reader input");
  }
  if (!split::decodePaimonLocalFileUri(uri_).ends_with(
          "/branch/branch-" + name_)) {
    throw std::invalid_argument("Paimon branch directory name/path mismatch");
  }
  for (size_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    const auto type = output_type_->childAt(column)->kind();
    if (!((name == "branch_name" && type == facebook::velox::TypeKind::VARCHAR) ||
          (name == "create_time" && type == facebook::velox::TypeKind::TIMESTAMP))) {
      throw std::invalid_argument("Invalid Paimon branches output schema");
    }
  }
}

facebook::velox::RowVectorPtr PaimonBranchDirectoryReader::next(uint64_t maxRows) {
  if (maxRows == 0) {
    throw std::invalid_argument("Metadata scan batch size must be positive");
  }
  if (emitted_) {
    return nullptr;
  }
  struct stat status {};
  const auto path = split::decodePaimonLocalFileUri(uri_);
  if (::lstat(path.c_str(), &status) != 0 || !S_ISDIR(status.st_mode) ||
      static_cast<uint64_t>(status.st_dev) != expected_device_ ||
      static_cast<uint64_t>(status.st_ino) != expected_inode_ ||
      modificationTimeMillis(status) != expected_mtime_ms_) {
    throw std::runtime_error("Pinned Paimon branch directory changed");
  }
  auto row = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(output_type_, 1, pool_));
  for (size_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    auto child = row->childAt(column);
    if (name == "branch_name") {
      child->asFlatVector<facebook::velox::StringView>()->set(
          0, facebook::velox::StringView(name_));
    } else {
      child->asFlatVector<facebook::velox::Timestamp>()->set(
          0, facebook::velox::Timestamp::fromMillis(
              expected_mtime_ms_ + static_cast<int64_t>(zone_offset_seconds_) * 1000));
    }
  }
  emitted_ = true;
  return row;
}

}  // namespace pangrova::velox
