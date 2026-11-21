#pragma once

#include <chrono>
#include <array>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include "lance/LanceBatchReader.h"
#include "lance/LanceStorageContext.h"
#include "LanceManagedSourceTestAuthority.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::test {

inline std::string testReadDetail(const protocol::v2::ScanPartitionDetail& partition) {
  protocol::v2::FileSplitDetail detail;
  detail.set_file_format(protocol::v2::FILE_FORMAT_LANCE);
  *detail.mutable_scan_partition() = partition;
  return detail.SerializeAsString();
}

inline protocol::v2::CatalogExecutionBinding testDatasetCatalog() {
  return LanceManagedSourceTestAuthority::catalog();
}

inline std::string testStorageBinding(const std::string& dataset,
    const protocol::v2::CatalogExecutionBinding& catalog = testDatasetCatalog(), const std::string& tenant = "tenant-a") {
  protocol::v2::ObjectStoreExecutionBinding binding;
  *binding.mutable_catalog() = catalog;
  binding.set_tenant_id(tenant);
  binding.set_warehouse_uri(std::filesystem::path(dataset).parent_path().string());
  return binding.SerializeAsString();
}

inline protocol::v2::DatasetScanPartitionsResult testScanPage(const std::string& dataset,
    uint64_t version, const std::string& storageBinding, uint32_t pageSize = 256,
    bool hasCursor = false, uint64_t after = 0) {
  lance::LanceStorageContext storage(storageBinding);
  const auto accepted = LanceManagedSourceTestAuthority::reference(dataset, version);
  protocol::v2::ObjectStoreExecutionBinding binding;
  if (!binding.ParseFromString(storageBinding)) throw std::runtime_error("Invalid test storage binding");
  protocol::v2::DatasetScanPartitionsRequest request;
  *request.mutable_catalog() = binding.catalog();
  request.mutable_security()->set_tenant_id(binding.tenant_id());
  request.mutable_security()->set_principal("test-principal");
  request.set_dataset_uri(dataset);
  request.set_version(version);
  request.set_page_size(pageSize);
  request.set_has_cursor(hasCursor);
  request.set_after_partition_id(after);
  request.set_deadline_epoch_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count() + 60'000);
  *request.mutable_physical_schema_sha256() = accepted.physical_schema_sha256();
  *request.mutable_accepted_snapshot() = accepted;
  const auto bytes = request.SerializeAsString();
  LanceCBuffer output{};
  if (lance_ffi_scan_partitions(storage.get(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &output) != 0) {
    throw std::runtime_error(std::string("Test scan page failed: ") + lance_ffi_last_error());
  }
  protocol::v2::DatasetScanPartitionsResult result;
  const bool parsed = result.ParseFromArray(output.data, output.len);
  lance_ffi_buffer_free(output);
  if (!parsed) throw std::runtime_error("Invalid test scan metadata response");
  return result;
}

// Test-only aggregation of actual partition-constrained readers. There is no
// whole-dataset production read path; each reader independently enforces its
// exact manifest partition and the test validates the union of those readers.
class PartitionedTestDatasetReader final {
 public:
  PartitionedTestDatasetReader(facebook::velox::memory::MemoryPool* pool,
      const facebook::velox::RowTypePtr& type, const std::string& dataset, uint64_t version,
      const std::vector<std::string>& projection) {
    if (!pool || !type || dataset.empty() || version == 0) throw std::invalid_argument("Invalid test reader input");
    const auto storage = testStorageBinding(dataset);
    bool hasCursor = false;
    uint64_t after = 0;
    do {
      const auto page = testScanPage(dataset, version, storage, 256, hasCursor, after);
      for (const auto& partition : page.partitions()) {
        readers_.push_back(std::make_unique<lance::LanceBatchReader>(pool, type, dataset, version, projection,
            storage, testReadDetail(partition), 0));
      }
      hasCursor = page.has_more();
      after = page.next_partition_id();
    } while (hasCursor);
  }
  facebook::velox::RowVectorPtr next(uint64_t maxRows) {
    while (position_ < readers_.size()) {
      if (auto rows = readers_[position_]->next(maxRows)) return rows;
      ++position_;
    }
    return nullptr;
  }
  bool hasNext() const { return position_ < readers_.size(); }
  const facebook::velox::RowTypePtr& outputType() const { return readers_.at(0)->outputType(); }

 private:
  std::vector<std::unique_ptr<lance::LanceBatchReader>> readers_;
  size_t position_ = 0;
};

}  // namespace pangrova::test
