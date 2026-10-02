#pragma once

#include <array>
#include <cstdint>
#include <string>

struct ArrowSchema;

namespace pangrova::protocol::v2 {
class DatasetSchemaFingerprintRequest;
class FinalizeDistributedWriteRequest;
class FinalizeDistributedWriteResult;
class DatasetScanPartitionsRequest;
class DatasetScanPartitionsResult;
}

namespace pangrova::lance {

/** Metadata-only validation and publication through the canonical Lance ABI. */
class LanceDatasetPublication final {
 public:
  static void validateSchemaRequest(
      const protocol::v2::DatasetSchemaFingerprintRequest& request);
  static std::array<uint8_t, 32> schemaFingerprint(const ArrowSchema& schema);
  /** Invalid input throws; an indeterminate FFI publication returns UNKNOWN. */
  static protocol::v2::FinalizeDistributedWriteResult finalizeWrite(
      const protocol::v2::FinalizeDistributedWriteRequest& request, const std::string& storageBinding);
  static protocol::v2::DatasetScanPartitionsResult scanPartitions(
      const protocol::v2::DatasetScanPartitionsRequest& request, const std::string& storageBinding);
};

}  // namespace pangrova::lance
