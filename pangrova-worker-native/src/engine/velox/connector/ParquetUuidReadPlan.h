#pragma once

#include <memory>
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::core { class ExpressionEvaluator; }
namespace facebook::velox::exec { class ExprSet; }
namespace facebook::velox::dwio::common { class TypeWithId; }

namespace pangrova::velox {

// Parquet stores UUID as annotated big-endian fixed binary. DWIO currently
// exposes that payload as VARBINARY; bind it to the declared UUID with the
// engine's vectorized cast, before relational filters, deletes or exchange.
class ParquetUuidReadPlan final {
 public:
  static std::unique_ptr<ParquetUuidReadPlan> create(
      const facebook::velox::RowTypePtr& logicalType,
      const facebook::velox::dwio::common::TypeWithId& fileType,
      facebook::velox::core::ExpressionEvaluator* evaluator);
  ~ParquetUuidReadPlan();

  const facebook::velox::RowTypePtr& physicalType() const { return physical_type_; }
  facebook::velox::RowVectorPtr convert(const facebook::velox::RowVectorPtr& input);

 private:
  ParquetUuidReadPlan(facebook::velox::RowTypePtr physicalType,
      facebook::velox::RowTypePtr logicalType,
      facebook::velox::core::ExpressionEvaluator* evaluator);
  facebook::velox::RowTypePtr physical_type_;
  facebook::velox::RowTypePtr logical_type_;
  facebook::velox::core::ExpressionEvaluator* evaluator_;
  std::unique_ptr<facebook::velox::exec::ExprSet> expression_;
};

}  // namespace pangrova::velox
