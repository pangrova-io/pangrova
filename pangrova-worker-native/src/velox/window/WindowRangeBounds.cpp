#include "velox/window/WindowRangeBounds.h"

#include <limits>
#include <stdexcept>

#include "capability/RegisteredFunctionBinding.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/Type.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

core::TypedExprPtr exactCast(core::TypedExprPtr value, const TypePtr& type) {
  if (value->type()->equivalent(*type)) return value;
  return std::make_shared<core::CastTypedExpr>(type,
      std::vector<core::TypedExprPtr>{std::move(value)}, false);
}
}  // namespace

facebook::velox::core::TypedExprPtr rangeWindowComparison(
    facebook::velox::core::TypedExprPtr value) {
  using namespace facebook::velox;
  const auto type = value->type();
  if (type->isDecimal()) {
    // Preserve every fractional digit. Expanding precision does not change
    // ordering and keeps boundary arithmetic from overflowing a narrow key.
    return exactCast(std::move(value), DECIMAL(LongDecimalType::kMaxPrecision,
        getDecimalPrecisionScale(*type).second));
  }
  if (type->isDate() || type->isIntervalDayTime() || type->isIntervalYearMonth()) {
    throw std::runtime_error("Integer RANGE offsets require a numeric ordering key");
  }
  switch (type->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      // This domain contains every int64 key plus/minus every int64 offset.
      return exactCast(std::move(value), DECIMAL(LongDecimalType::kMaxPrecision, 0));
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return value;
    default:
      throw std::runtime_error("Integer RANGE offsets require a numeric ordering key");
  }
}

facebook::velox::core::TypedExprPtr rangeWindowBoundary(
    facebook::velox::core::TypedExprPtr comparison, int64_t offset, bool subtract) {
  using namespace facebook::velox;
  if (offset <= 0) throw std::runtime_error("Window bound offset must be strictly positive");
  core::TypedExprPtr amount = std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(offset));
  const auto amountType = comparison->type()->isDecimal()
      ? DECIMAL(std::numeric_limits<int64_t>::digits10 + 1, 0) : comparison->type();
  amount = exactCast(std::move(amount), amountType);
  // The running engine owns the arithmetic formula and overflow behavior.
  // No saturation is legal: a bound below the domain is not its minimum key.
  const auto outputType = comparison->type();
  return pangrova::capability::bindRegisteredScalar(subtract ? "minus" : "plus",
      outputType, {std::move(comparison), std::move(amount)});
}

}  // namespace pangrova::velox
