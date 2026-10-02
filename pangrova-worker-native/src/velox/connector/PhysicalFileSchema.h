#pragma once

#include <stdexcept>
#include "merge/MergeReader.h"
#include "velox/connector/SchemaEvolution.h"

namespace pangrova::velox {

// One immutable file's source types and field-ID mapping to the query row type.
class PhysicalFileSchema final {
 public:
  PhysicalFileSchema(facebook::velox::RowTypePtr target,
      std::vector<split::ColumnMappingConfig> mappings,
      std::vector<split::TypePromotionConfig> promotions)
      : target_(std::move(target)), mappings_(std::move(mappings)), promotions_(std::move(promotions)) {
    std::vector<std::string> physical_names;
    std::vector<facebook::velox::TypePtr> physical_types;
    auto logical_types = target_->children();
    for (size_t index = 0; index < target_->size(); ++index) {
      const auto& target_name = target_->nameOf(index);
      auto name = target_name;
      auto type = target_->childAt(index);
      bool present = true;
      for (const auto& mapping : mappings_) {
        if (mapping.target_name == target_name) {
          present = mapping.source_id >= 0;
          name = mapping.source_name;
          break;
        }
      }
      for (const auto& promotion : promotions_) {
        if (promotion.column_name == target_name) {
          type = resolveSchemaEvolutionType(promotion.source_type);
          logical_types[index] = type;
          break;
        }
      }
      if (present) {
        physical_names.push_back(name);
        physical_types.push_back(type);
      }
    }
    physical_ = facebook::velox::ROW(std::move(physical_names), std::move(physical_types));
    intermediate_ = facebook::velox::ROW(target_->names(), std::move(logical_types));
  }

  const facebook::velox::RowTypePtr& physicalType() const { return physical_; }
  facebook::velox::RowVectorPtr apply(facebook::velox::RowVectorPtr batch,
      facebook::velox::memory::MemoryPool* pool) const {
    if (!batch) return nullptr;
    batch = applyColumnMapping(batch, mappings_, intermediate_, pool);
    batch = applyTypePromotion(batch, promotions_, pool);
    if (!batch->type()->equivalent(*target_)) {
      throw std::invalid_argument("Physical file evolution did not produce the exact query row type");
    }
    return batch;
  }

 private:
  facebook::velox::RowTypePtr target_;
  facebook::velox::RowTypePtr physical_;
  facebook::velox::RowTypePtr intermediate_;
  std::vector<split::ColumnMappingConfig> mappings_;
  std::vector<split::TypePromotionConfig> promotions_;
};

class EvolvedMergeBatchReader final : public merge::RowReaderWrapper {
 public:
  EvolvedMergeBatchReader(std::unique_ptr<merge::RowReaderWrapper> reader, PhysicalFileSchema schema)
      : reader_(std::move(reader)), schema_(std::move(schema)) {}
  facebook::velox::RowVectorPtr next(uint64_t size, facebook::velox::memory::MemoryPool* pool) override {
    return schema_.apply(reader_->next(size, pool), pool);
  }
  bool hasNext() const override { return reader_->hasNext(); }
 private:
  std::unique_ptr<merge::RowReaderWrapper> reader_;
  PhysicalFileSchema schema_;
};

}  // namespace pangrova::velox
