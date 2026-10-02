#pragma once

#include <map>
#include <boost/crc.hpp>
#include <roaring.hh>
#include "delete/RowDeleteFilter.h"
#include "lance/LanceStorageContext.h"
#include "pangrova/protocol/v2/connector.pb.h"

namespace pangrova::delete_filter {

// SDK portable 32/64-bit bitmap containers, with bounded exact object ranges.
class PaimonSnapshotDeletionFilter final : public RowDeleteFilter {
 public:
  PaimonSnapshotDeletionFilter(const protocol::v2::DeletionVectorReference& reference,
      const std::string& storage_binding) {
    if (reference.byte_length() < 4 || reference.byte_length() > 64 * 1024 * 1024) {
      throw std::invalid_argument("Paimon deletion vector exceeds its 64 MiB bound");
    }
    lance::LanceStorageContext storage(storage_binding);
    const auto head = read(storage, reference, 8);
    const uint32_t length = integer(head.data(), false);
    const uint32_t magic = integer(head.data() + 4, false);
    const bool wide = integer(head.data() + 4, true) == 1681511377u;
    if (!wide && magic != 1581511376u) throw std::invalid_argument("Invalid Paimon deletion vector magic");
    const uint64_t bytes = reference.byte_length() + (wide ? 0 : 8);
    if (bytes > 64 * 1024 * 1024 || length != bytes - 8) {
      throw std::invalid_argument("Paimon deletion vector length differs from snapshot metadata");
    }
    const auto body = read(storage, reference, bytes);
    boost::crc_32_type crc;
    crc.process_bytes(body.data() + 4, body.size() - 8);
    if (crc.checksum() != integer(body.data() + body.size() - 4, false)) {
      throw std::invalid_argument("Paimon deletion vector checksum mismatch");
    }
    if (wide) decodeWide(body.data() + 8, body.size() - 12);
    else {
      auto bitmap = roaring::Roaring::readSafe(body.data() + 8, body.size() - 12);
      if (bitmap.getSizeInBytes() != body.size() - 12) {
        throw std::invalid_argument("Paimon 32-bit bitmap has trailing bytes");
      }
      bitmaps_.emplace(0, std::move(bitmap));
    }
    uint64_t count = 0;
    for (const auto& [key, bitmap] : bitmaps_) {
      count += bitmap.cardinality();
      if (!bitmap.isEmpty() && (static_cast<uint64_t>(key) << 32 | bitmap.maximum()) >= reference.physical_rows()) {
        throw std::invalid_argument("Paimon deletion vector position escapes its physical file");
      }
    }
    if (reference.has_cardinality() && count != reference.cardinality()) {
      throw std::invalid_argument("Paimon deletion vector cardinality differs from snapshot metadata");
    }
  }

  void apply(const facebook::velox::RowVector& batch, facebook::velox::SelectivityVector& rows,
      int64_t offset) const override {
    if (offset < 0) throw std::invalid_argument("Deletion vector row offset is negative");
    rows.applyToSelected([&](facebook::velox::vector_size_t row) {
      const auto position = static_cast<uint64_t>(offset) + row;
      const auto found = bitmaps_.find(static_cast<uint32_t>(position >> 32));
      if (found != bitmaps_.end() && found->second.contains(static_cast<uint32_t>(position))) {
        rows.setValid(row, false);
      }
    });
    rows.updateBounds();
  }

 private:
  static uint32_t integer(const char* bytes, bool little) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
      value |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[index])) << ((little ? index : 3 - index) * 8);
    }
    return value;
  }
  static std::string read(lance::LanceStorageContext& storage,
      const protocol::v2::DeletionVectorReference& reference, uint64_t length) {
    LanceCBuffer buffer{};
    if (lance_ffi_storage_read_range(storage.get(), reference.opaque_location().c_str(),
        reference.byte_offset(), length, &buffer) != 0) {
      const auto* error = lance_ffi_last_error();
      throw std::runtime_error(error ? error : "Scoped deletion vector read failed");
    }
    std::shared_ptr<void> cleanup(buffer.data, [buffer](void*) { lance_ffi_buffer_free(buffer); });
    return std::string(reinterpret_cast<const char*>(buffer.data), buffer.len);
  }
  void decodeWide(const char* bytes, size_t length) {
    if (length < 8) throw std::invalid_argument("Truncated Paimon 64-bit bitmap count");
    const uint64_t count = integer(bytes, true) | static_cast<uint64_t>(integer(bytes + 4, true)) << 32;
    size_t cursor = 8;
    uint32_t previous = 0;
    if (count > 4096) throw std::invalid_argument("Paimon deletion vector has too many bitmap partitions");
    for (uint64_t index = 0; index < count; ++index) {
      if (cursor + 4 > length) throw std::invalid_argument("Truncated Paimon bitmap key");
      const auto key = integer(bytes + cursor, true);
      cursor += 4;
      if (index && key <= previous) throw std::invalid_argument("Paimon bitmap keys are not strictly ordered");
      auto bitmap = roaring::Roaring::readSafe(bytes + cursor, length - cursor);
      cursor += bitmap.getSizeInBytes();
      bitmaps_.emplace(key, std::move(bitmap));
      previous = key;
    }
    if (cursor != length) throw std::invalid_argument("Paimon bitmap has trailing bytes");
  }
  std::map<uint32_t, roaring::Roaring> bitmaps_;
};

}  // namespace pangrova::delete_filter
