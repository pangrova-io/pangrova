// Typed literal decoding shared by expressions, ARRAYs, and VirtualTable.
#include "velox/plan/VeloxPlanConverter.h"
#include "capability/CanonicalPhysicalTypes.h"

#include <limits>
#include <cstdlib>

#include "velox/buffer/Buffer.h"
#include "velox/core/Expressions.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {
namespace {

using Constant = facebook::velox::core::ConstantTypedExpr;

std::shared_ptr<const Constant> constant(
    facebook::velox::TypePtr type, facebook::velox::variant value) {
  return std::make_shared<Constant>(std::move(type), std::move(value));
}

int64_t rescalePrecisionUnit(int64_t value, int32_t from, int32_t to) {
  int64_t factor = 1;
  for (int32_t i = 0; i < std::abs(from - to); ++i) {
    factor *= 10;
  }
  if (from > to) {
    return value / factor;
  }
  const __int128 scaled = static_cast<__int128>(value) * factor;
  if (scaled < std::numeric_limits<int64_t>::min() ||
      scaled > std::numeric_limits<int64_t>::max()) {
    throw std::runtime_error("Substrait temporal literal overflows int64");
  }
  return static_cast<int64_t>(scaled);
}

facebook::velox::Timestamp toVeloxTimestamp(
    const substrait::Expression_Literal_PrecisionTimestamp& value) {
  const int32_t precision = value.precision();
  if (precision < 0 || precision > 9) {
    throw std::runtime_error("Substrait timestamp precision must be in [0, 9]");
  }
  int64_t units_per_second = 1;
  for (int32_t i = 0; i < precision; ++i) {
    units_per_second *= 10;
  }
  int64_t seconds = value.value() / units_per_second;
  int64_t remainder = value.value() % units_per_second;
  if (remainder < 0) {
    --seconds;
    remainder += units_per_second;
  }
  return facebook::velox::Timestamp(
      seconds, rescalePrecisionUnit(remainder, precision, 9));
}

const substrait::Expression_Literal& virtualLiteral(
    const substrait::ReadRel_VirtualTable& table, int row, int column) {
  const auto& expression = table.expressions(row).fields(column);
  if (!expression.has_literal()) {
    throw std::runtime_error("Substrait VirtualTable requires literal fields");
  }
  return expression.literal();
}

}  // namespace

facebook::velox::TypePtr VeloxPlanConverter::convertDecimalType(
    int32_t precision, int32_t scale) {
  return pangrova::capability::canonicalDecimalType(precision, scale);
}

facebook::velox::TypePtr VeloxPlanConverter::convertTimeType(int32_t precision) {
  return pangrova::capability::canonicalTimeType(precision);
}

std::shared_ptr<facebook::velox::memory::MemoryPool> VeloxPlanConverter::literalPool() {
  return facebook::velox::memory::MemoryManager::getInstance()->addLeafPool();
}

std::shared_ptr<const Constant> VeloxPlanConverter::convertDecimalLiteral(
    const substrait::Expression_Literal_Decimal& decimal) {
  auto type = convertDecimalType(decimal.precision(), decimal.scale());
  if (decimal.value().size() != 16) {
    throw std::runtime_error("Substrait DECIMAL value must be exactly 16 bytes");
  }
  __uint128_t bits = 0;
  for (size_t i = 0; i < 16; ++i) {
    bits |= static_cast<__uint128_t>(
        static_cast<uint8_t>(decimal.value()[i])) << (8 * i);
  }
  // Decode two's complement without host-endian memcpy or signed shifts.
  const facebook::velox::int128_t value = (bits >> 127)
      ? -1 - static_cast<facebook::velox::int128_t>(~bits)
      : static_cast<facebook::velox::int128_t>(bits);
  if (!facebook::velox::DecimalUtil::valueInPrecisionRange(
          value, decimal.precision())) {
    throw std::runtime_error("Substrait DECIMAL value exceeds declared precision");
  }
  return type->isShortDecimal()
      ? constant(type, facebook::velox::variant(static_cast<int64_t>(value)))
      : constant(type, facebook::velox::variant(value));
}

std::shared_ptr<const Constant> VeloxPlanConverter::arrayConstant(
    facebook::velox::VectorPtr elements) {
  auto* pool = elements->pool();
  auto offsets = facebook::velox::AlignedBuffer::allocate<int32_t>(1, pool);
  auto sizes = facebook::velox::AlignedBuffer::allocate<int32_t>(1, pool);
  offsets->asMutable<int32_t>()[0] = 0;
  sizes->asMutable<int32_t>()[0] = elements->size();
  auto type = facebook::velox::ARRAY(elements->type());
  auto array = std::make_shared<facebook::velox::ArrayVector>(
      pool, std::move(type), nullptr, 1,
      offsets, sizes, std::move(elements));
  return std::make_shared<Constant>(array);
}

std::shared_ptr<const Constant> VeloxPlanConverter::convertListLiteral(
    const substrait::Expression_Literal_List& list) {
  if (list.values_size() == 0) {
    throw std::runtime_error("empty list literal requires typed empty_list");
  }
  auto first = convertLiteral(list.values(0));
  auto owner = literalPool();
  auto* pool = owner.get();
  auto elements = facebook::velox::BaseVector::create(
      first->type(), list.values_size(), pool);
  for (int i = 0; i < list.values_size(); ++i) {
    auto value = i == 0 ? first : convertLiteral(list.values(i));
    if (!value->type()->equivalent(*elements->type())) {
      throw std::runtime_error(
          "Substrait ARRAY literal element types differ: expected=" +
          elements->type()->toString() + " actual=" + value->type()->toString());
    }
    auto source = value->toConstantVector(pool);
    elements->copy(source.get(), i, 0, 1);
  }
  return arrayConstant(std::move(elements));
}

std::shared_ptr<const Constant> VeloxPlanConverter::convertStructLiteral(
    const substrait::Expression_Literal_Struct& value) {
  auto owner = literalPool();
  std::vector<facebook::velox::VectorPtr> children;
  std::vector<facebook::velox::TypePtr> types;
  for (const auto& field : value.fields()) {
    auto converted = convertLiteral(field);
    types.push_back(converted->type());
    children.push_back(converted->toConstantVector(owner.get()));
  }
  auto type = pangrova::capability::canonicalComputedRowType(std::move(types));
  return std::make_shared<Constant>(std::make_shared<facebook::velox::RowVector>(
      owner.get(), std::move(type), nullptr, 1, std::move(children)));
}

std::shared_ptr<const Constant> VeloxPlanConverter::convertLiteral(
    const substrait::Expression_Literal& literal) {
  using namespace facebook::velox;
  switch (literal.literal_type_case()) {
    case substrait::Expression_Literal::kBoolean:
      return constant(BOOLEAN(), variant(literal.boolean()));
    case substrait::Expression_Literal::kI8:
      if (literal.i8() < -128 || literal.i8() > 127) {
        throw std::runtime_error("Substrait i8 literal exceeds signed 8-bit range");
      }
      return constant(TINYINT(), variant(static_cast<int8_t>(literal.i8())));
    case substrait::Expression_Literal::kI16:
      if (literal.i16() < -32768 || literal.i16() > 32767) {
        throw std::runtime_error("Substrait i16 literal exceeds signed 16-bit range");
      }
      return constant(SMALLINT(), variant(static_cast<int16_t>(literal.i16())));
    case substrait::Expression_Literal::kI32:
      return constant(INTEGER(), variant(literal.i32()));
    case substrait::Expression_Literal::kI64:
      return constant(BIGINT(), variant(literal.i64()));
    case substrait::Expression_Literal::kFp32:
      return constant(REAL(), variant(literal.fp32()));
    case substrait::Expression_Literal::kFp64:
      return constant(DOUBLE(), variant(literal.fp64()));
    case substrait::Expression_Literal::kString:
      return constant(VARCHAR(), variant(literal.string()));
    case substrait::Expression_Literal::kBinary:
      return constant(VARBINARY(), variant::binary(literal.binary()));
    case substrait::Expression_Literal::kDecimal:
      return convertDecimalLiteral(literal.decimal());
    case substrait::Expression_Literal::kDate:
      return constant(DATE(), variant(literal.date()));
    case substrait::Expression_Literal::kPrecisionTimestamp:
      return constant(TIMESTAMP(), variant(toVeloxTimestamp(literal.precision_timestamp())));
    case substrait::Expression_Literal::kPrecisionTimestampTz:
      return constant(TIMESTAMP(), variant(toVeloxTimestamp(literal.precision_timestamp_tz())));
    case substrait::Expression_Literal::kPrecisionTime: {
      const auto& time = literal.precision_time();
      auto type = convertTimeType(time.precision());
      return constant(type, variant(rescalePrecisionUnit(
          time.value(), time.precision(), time.precision() <= 3 ? 3 : 6)));
    }
    case substrait::Expression_Literal::kNull: {
      auto type = convertType(literal.null());
      return constant(type, variant::null(type->kind()));
    }
    case substrait::Expression_Literal::kList:
      return convertListLiteral(literal.list());
    case substrait::Expression_Literal::kStruct:
      return convertStructLiteral(literal.struct_());
    case substrait::Expression_Literal::kEmptyList: {
      auto owner = literalPool();
      return arrayConstant(BaseVector::create(
          convertType(literal.empty_list().type()), 0, owner.get()));
    }
    default:
      throw std::runtime_error(
          "Unsupported Substrait literal type: " +
          std::to_string(literal.literal_type_case()));
  }
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertVirtualTable(
    const substrait::ReadRel& read,
    const facebook::velox::RowTypePtr& output_type) {
  const auto& table = read.virtual_table();
  if (table.values_size() != 0) {
    throw std::runtime_error(
        "Substrait VirtualTable deprecated values encoding is forbidden; "
        "use expressions with an explicit schema");
  }
  const int rows = table.expressions_size();
  const int columns = static_cast<int>(output_type->size());
  auto owner = literalPool();
  auto* pool = owner.get();
  std::vector<facebook::velox::VectorPtr> children;
  for (int column = 0; column < columns; ++column) {
    children.push_back(facebook::velox::BaseVector::create(
        output_type->childAt(column), rows, pool));
  }
  for (int row = 0; row < rows; ++row) {
    const int width = table.expressions(row).fields_size();
    if (width != columns) {
      throw std::runtime_error("Substrait VirtualTable row width differs from schema");
    }
    for (int column = 0; column < columns; ++column) {
      auto value = convertLiteral(virtualLiteral(table, row, column));
      if (!value->type()->equivalent(*output_type->childAt(column))) {
        throw std::runtime_error(
            "Substrait VirtualTable literal type differs from schema: expected=" +
            output_type->childAt(column)->toString() + " actual=" +
            value->type()->toString());
      }
      auto source = value->toConstantVector(pool);
      children[column]->copy(source.get(), row, 0, 1);
    }
  }
  auto values = std::make_shared<facebook::velox::RowVector>(
      pool, output_type, nullptr, rows, std::move(children));
  return std::make_shared<facebook::velox::core::ValuesNode>(
      nextNodeId(), std::vector<facebook::velox::RowVectorPtr>{values});
}

}  // namespace pangrova::velox
