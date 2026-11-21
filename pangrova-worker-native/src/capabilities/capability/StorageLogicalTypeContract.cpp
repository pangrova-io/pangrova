#include "capabilities/capability/LogicalTypeContract.h"

#include <arrow/type.h>
#include <arrow/extension/uuid.h>
#include <arrow/array.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <map>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace pangrova::capability {
namespace {

using Constraint = protocol::v2::TypeConstraint;
using Constraints = std::map<std::string, const Constraint*>;
using Type = substrait::Type;

[[noreturn]] void mismatch() {
  throw std::invalid_argument("Storage logical declaration differs from its complete physical schema");
}

std::string constraintName(const std::string& name, const std::string& path) {
  return path.empty() ? name : name + "@" + path;
}

const Constraint& consume(
    const Constraints& constraints, std::set<std::string>& consumed, const std::string& name) {
  const auto value = constraints.find(name);
  if (value == constraints.end() || !consumed.insert(name).second) mismatch();
  return *value->second;
}

void requireNullability(const Type& type, const arrow::Field& field) {
  const auto* descriptor = type.GetDescriptor();
  const auto* kind = type.GetReflection()->GetOneofFieldDescriptor(
      type, descriptor->FindOneofByName("kind"));
  if (kind == nullptr) mismatch();
  const auto& value = type.GetReflection()->GetMessage(type, kind);
  const auto* nullability = value.GetDescriptor()->FindFieldByName("nullability");
  const auto* variation = value.GetDescriptor()->FindFieldByName("type_variation_reference");
  if (nullability == nullptr || variation == nullptr ||
      value.GetReflection()->GetUInt32(value, variation) != 0) mismatch();
  const int actual = value.GetReflection()->GetEnumValue(value, nullability);
  if (actual != (field.nullable() ? Type::NULLABILITY_NULLABLE : Type::NULLABILITY_REQUIRED)) mismatch();
}

std::string quoted(const std::string& name) {
  std::string result = "\"";
  for (char character : name) {
    if (character == '"') result.push_back('"');
    result.push_back(character);
  }
  return result + '"';
}

arrow::TimeUnit::type timeUnit(int32_t precision) {
  if (precision < 0 || precision > 9) mismatch();
  if (precision <= 3) return arrow::TimeUnit::MILLI;
  if (precision <= 6) return arrow::TimeUnit::MICRO;
  return arrow::TimeUnit::NANO;
}

arrow::TimeUnit::type timestampUnit(int32_t precision) {
  if (precision == 0) return arrow::TimeUnit::SECOND;
  return timeUnit(precision);
}

std::string boundedString(const Type& type, const arrow::DataType& physical) {
    const int32_t length = type.has_varchar() ? type.varchar().length() : type.fixed_char().length();
    if (length <= 0 || physical.id() != arrow::Type::STRING) mismatch();
    return std::string(type.has_varchar() ? "VARCHAR(" : "CHAR(") + std::to_string(length) + ')';
}

std::string decimal(const Type& type, const arrow::DataType& physical) {
    const auto& declaration = type.decimal();
    if (physical.id() != arrow::Type::DECIMAL128 || declaration.precision() < 1 ||
        declaration.precision() > 38 || declaration.scale() < 0 || declaration.scale() > declaration.precision()) mismatch();
    const auto& decimal = static_cast<const arrow::Decimal128Type&>(physical);
    if (decimal.precision() != declaration.precision() || decimal.scale() != declaration.scale()) mismatch();
    return "DECIMAL(" + std::to_string(declaration.precision()) + ',' + std::to_string(declaration.scale()) + ')';
}

std::string temporal(const Type& type, const arrow::DataType& physical) {
  if (type.has_precision_timestamp() || type.has_precision_timestamp_tz()) {
    if (physical.id() != arrow::Type::TIMESTAMP) mismatch();
    const bool zoned = type.has_precision_timestamp_tz();
    const int32_t precision = zoned ? type.precision_timestamp_tz().precision() : type.precision_timestamp().precision();
    const auto& timestamp = static_cast<const arrow::TimestampType&>(physical);
    if (timestamp.unit() != timestampUnit(precision) || timestamp.timezone().empty() == zoned) mismatch();
    return std::string(zoned ? "TIMESTAMP_TZ(" : "TIMESTAMP(") + std::to_string(precision) + ')';
  }
  if (type.has_precision_time()) {
    const auto unit = timeUnit(type.precision_time().precision());
    if (unit == arrow::TimeUnit::MILLI) {
      if (physical.id() != arrow::Type::TIME32 || static_cast<const arrow::Time32Type&>(physical).unit() != unit) mismatch();
    } else if (physical.id() != arrow::Type::TIME64 || static_cast<const arrow::Time64Type&>(physical).unit() != unit) mismatch();
    return "TIME(" + std::to_string(type.precision_time().precision()) + ')';
  }
  mismatch();
}

std::string scalar(const Type& type, const arrow::DataType& physical) {
  static const std::map<Type::KindCase, std::pair<arrow::Type::type, std::string>> primitives = {
      {Type::kBool, {arrow::Type::BOOL, "BOOLEAN"}}, {Type::kI8, {arrow::Type::INT8, "TINYINT"}},
      {Type::kI16, {arrow::Type::INT16, "SMALLINT"}}, {Type::kI32, {arrow::Type::INT32, "INTEGER"}},
      {Type::kI64, {arrow::Type::INT64, "BIGINT"}}, {Type::kFp32, {arrow::Type::FLOAT, "FLOAT"}},
      {Type::kFp64, {arrow::Type::DOUBLE, "DOUBLE"}}, {Type::kString, {arrow::Type::STRING, "VARCHAR(2147483647)"}},
      {Type::kBinary, {arrow::Type::BINARY, "VARBINARY"}}, {Type::kDate, {arrow::Type::DATE32, "DATE"}}};
  const auto primitive = primitives.find(type.kind_case());
  if (primitive != primitives.end()) {
    if (physical.id() != primitive->second.first) mismatch();
    return primitive->second.second;
  }
  if (type.has_varchar() || type.has_fixed_char()) return boundedString(type, physical);
  if (type.has_decimal()) return decimal(type, physical);
  if (type.has_uuid()) {
    if (!physical.Equals(arrow::extension::uuid())) mismatch();
    return "UUID";
  }
  return temporal(type, physical);
}

std::string validate(const Type& type, const arrow::Field& field, const std::string& path,
    const Constraints& constraints, std::set<std::string>& consumed, uint32_t depth);

std::string list(const Type& type, const arrow::Field& field, const std::string& path,
    const Constraints& constraints, std::set<std::string>& consumed, uint32_t depth) {
  if (!type.list().has_type()) mismatch();
  const auto& physical = *field.type();
  if (physical.id() != arrow::Type::LIST && physical.id() != arrow::Type::FIXED_SIZE_LIST) mismatch();
  const auto& child = *static_cast<const arrow::BaseListType&>(physical).value_field();
  const std::string childPath = path.empty() ? "0" : path + ".0";
  const std::string element = validate(type.list().type(), child, childPath, constraints, consumed, depth + 1);
  const auto dimensionName = constraintName("dimension", path);
  if (constraints.contains(dimensionName)) {
    const auto& dimension = consume(constraints, consumed, dimensionName);
    const std::string elementTypeName = constraintName("element_type", path);
    const auto& elementType = consume(constraints, consumed, elementTypeName);
    if (physical.id() != arrow::Type::FIXED_SIZE_LIST || !dimension.string_value().empty() ||
        dimension.integer_value() < 1 || dimension.integer_value() > 65535 ||
        static_cast<const arrow::FixedSizeListType&>(physical).list_size() != dimension.integer_value() ||
        elementType.integer_value() != 0) mismatch();
    if (element == "FLOAT" && elementType.string_value() == "FLOAT32") return "VECTOR(" + std::to_string(dimension.integer_value()) + ')';
    if (element == "DOUBLE" && elementType.string_value() == "FLOAT64") return "VECTOR(" + std::to_string(dimension.integer_value()) + ",DOUBLE)";
    mismatch();
  }
  return "ARRAY(" + element + ')';
}

std::string row(const Type& type, const arrow::Field& field, const std::string& path,
    const Constraints& constraints, std::set<std::string>& consumed, uint32_t depth) {
  if (field.type()->id() != arrow::Type::STRUCT) mismatch();
  const auto& physical = static_cast<const arrow::StructType&>(*field.type());
  if (physical.num_fields() != type.struct_().types_size() || physical.num_fields() == 0) mismatch();
  std::string result = "ROW(";
  std::set<std::string> names;
  for (int index = 0; index < physical.num_fields(); ++index) {
    const std::string childPath = path.empty() ? std::to_string(index) : path + '.' + std::to_string(index);
    const std::string fieldName = "field_name@" + childPath;
    const auto& name = consume(constraints, consumed, fieldName);
    const auto& child = *physical.field(index);
    if (name.integer_value() != 0 || name.string_value().empty() || name.string_value() != child.name() ||
        !names.insert(name.string_value()).second) mismatch();
    if (index != 0) result += ',';
    result += quoted(name.string_value()) + ' ' + validate(type.struct_().types(index), child, childPath,
        constraints, consumed, depth + 1);
  }
  return result + ')';
}

std::string validate(const Type& type, const arrow::Field& field, const std::string& path,
    const Constraints& constraints, std::set<std::string>& consumed, uint32_t depth) {
  if (depth > 64) mismatch();
  requireNullability(type, field);
  if (type.has_list()) return list(type, field, path, constraints, consumed, depth);
  if (type.has_struct_()) return row(type, field, path, constraints, consumed, depth);
  if (type.has_map()) {
    if (field.type()->id() != arrow::Type::MAP || !type.map().has_key() || !type.map().has_value()) mismatch();
    const auto& physical = static_cast<const arrow::MapType&>(*field.type());
    const std::string keyPath = path.empty() ? "0" : path + ".0";
    const std::string valuePath = path.empty() ? "1" : path + ".1";
    const auto key = validate(type.map().key(), *physical.key_field(), keyPath, constraints, consumed, depth + 1);
    const auto value = validate(type.map().value(), *physical.item_field(), valuePath, constraints, consumed, depth + 1);
    return "MAP(" + key + ',' + value + ')';
  }
  return scalar(type, *field.type());
}

}  // namespace

void validateStorageLogicalType(const protocol::v2::LogicalType& logical_type, const arrow::Field& field) {
  if (!logical_type.has_substrait_type() || logical_type.canonical_sql().empty()) mismatch();
  Constraints constraints;
  for (const auto& constraint : logical_type.constraints()) {
    if (!constraints.emplace(constraint.name(), &constraint).second) mismatch();
  }
  std::set<std::string> consumed;
  const auto canonical = validate(logical_type.substrait_type(), field, "", constraints, consumed, 0);
  if (canonical != logical_type.canonical_sql() || consumed.size() != constraints.size()) mismatch();
}

uint32_t requireStorageVectorDimension(const protocol::v2::LogicalType& logical_type) {
  if (!logical_type.has_substrait_type() || !logical_type.substrait_type().has_list()) mismatch();
  const auto& list = logical_type.substrait_type().list();
  const auto& child = list.type();
  if (!child.has_fp32() && !child.has_fp64()) mismatch();
  int64_t dimension = 0;
  for (const auto& constraint : logical_type.constraints()) {
    if (constraint.name() == "dimension") dimension = constraint.integer_value();
  }
  if (dimension < 1 || dimension > std::numeric_limits<int32_t>::max()) mismatch();
  const bool childNullable = (child.has_fp32() ? child.fp32().nullability() : child.fp64().nullability())
      == Type::NULLABILITY_NULLABLE;
  const auto element = arrow::field("item", child.has_fp32() ? arrow::float32() : arrow::float64(), childNullable);
  const auto field = arrow::field("vector", arrow::fixed_size_list(element, static_cast<int32_t>(dimension)),
      list.nullability() == Type::NULLABILITY_NULLABLE);
  // The existing sole storage validator owns canonical spelling, bounds,
  // element precision, variations, nullability and complete constraint coverage.
  validateStorageLogicalType(logical_type, *field);
  return static_cast<uint32_t>(dimension);
}

void validateVectorResultField(const protocol::v2::LogicalType& logical_type, const arrow::Field& field) {
  const auto dimension = requireStorageVectorDimension(logical_type);
  if (field.type()->id() != arrow::Type::LIST && field.type()->id() != arrow::Type::FIXED_SIZE_LIST) mismatch();
  const auto& physical = static_cast<const arrow::BaseListType&>(*field.type());
  if (field.type()->id() == arrow::Type::FIXED_SIZE_LIST &&
      static_cast<const arrow::FixedSizeListType&>(physical).list_size() != static_cast<int32_t>(dimension)) mismatch();
  // A result LIST carries a separately admitted fixed-dimension constraint.
  // Validate its actual child type and nullability without changing its buffers.
  validateStorageLogicalType(logical_type,
      *field.WithType(arrow::fixed_size_list(physical.value_field(), static_cast<int32_t>(dimension))));
}

void validateVectorResultArray(const protocol::v2::LogicalType& logical_type, const arrow::Array& array) {
  const auto dimension = requireStorageVectorDimension(logical_type);
  const auto& list = logical_type.substrait_type().list();
  validateVectorResultField(logical_type,
      *arrow::field("vector", array.type(), list.nullability() == Type::NULLABILITY_NULLABLE));
  if (list.nullability() == Type::NULLABILITY_REQUIRED && array.null_count() != 0) mismatch();
  const auto& child = list.type();
  const bool required = (child.has_fp32() ? child.fp32().nullability() : child.fp64().nullability())
      == Type::NULLABILITY_REQUIRED;
  auto check = [&](const auto& values) {
    for (int64_t row = 0; row < array.length(); ++row) {
      if (array.IsNull(row)) continue;
      if (static_cast<int64_t>(values.value_length(row)) != static_cast<int64_t>(dimension)) mismatch();
      if (required && values.values()->null_count() != 0) {
        const auto offset = values.value_offset(row);
        for (uint32_t index = 0; index < dimension; ++index) {
          if (values.values()->IsNull(offset + index)) mismatch();
        }
      }
    }
  };
  if (array.type_id() == arrow::Type::LIST) check(static_cast<const arrow::ListArray&>(array));
  else check(static_cast<const arrow::FixedSizeListArray&>(array));
}

}  // namespace pangrova::capability
