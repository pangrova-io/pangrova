#include "velox/connector/IcebergPartitionProjection.h"

#include <bit>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "capability/CanonicalPhysicalTypes.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

const folly::dynamic& nonNull(const folly::dynamic& schema) {
  if (!schema.isArray()) return schema;
  if (schema.size() != 2) throw std::runtime_error("Invalid partition Avro union");
  if (schema[0].isString() && schema[0].asString() == "null") return schema[1];
  if (schema[1].isString() && schema[1].asString() == "null") return schema[0];
  throw std::runtime_error("Partition Avro union requires one null branch");
}

std::string kind(const folly::dynamic& schema) {
  if (schema.isString()) return schema.asString();
  if (schema.isObject() && schema.count("type") && schema["type"].isString()) {
    return schema["type"].asString();
  }
  throw std::runtime_error("Invalid partition Avro scalar schema");
}

int integer(std::string_view text) {
  int result;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::runtime_error("Invalid partition type parameter");
  }
  return result;
}

bool logical(const folly::dynamic& schema, const char* name) {
  return schema.isObject() && schema.count("logicalType") &&
      schema["logicalType"].isString() && schema["logicalType"].asString() == name;
}

void validateScalar(const folly::dynamic& schema, const std::string& result,
    const TypePtr& output) {
  const auto encoded = kind(schema);
  const auto physical = output->kind();
  const bool plain = !schema.isObject() || !schema.count("logicalType");
  bool valid = false;
  if (result == "boolean") valid = plain && encoded == "boolean" && physical == TypeKind::BOOLEAN;
  else if (result == "int") valid = plain && encoded == "int" &&
      (output->equivalent(*INTEGER()) || output->equivalent(*BIGINT()));
  else if (result == "long") valid = plain && (encoded == "int" || encoded == "long") &&
      output->equivalent(*BIGINT());
  else if (result == "float") valid = plain && encoded == "float" &&
      (physical == TypeKind::REAL || physical == TypeKind::DOUBLE);
  else if (result == "double") valid = plain && (encoded == "float" || encoded == "double") &&
      physical == TypeKind::DOUBLE;
  else if (result == "string") valid = plain && encoded == "string" && physical == TypeKind::VARCHAR;
  else if (result == "binary") valid = plain && encoded == "bytes" && physical == TypeKind::VARBINARY;
  else if (result == "date") valid = encoded == "int" && logical(schema, "date") && output->isDate();
  else if (result == "time") valid = encoded == "long" && logical(schema, "time-micros") &&
      output->equivalent(*capability::canonicalTimeType(6));
  else if (result == "timestamp" || result == "timestamptz" ||
      result == "timestamp_ns" || result == "timestamptz_ns") {
    valid = encoded == "long" && physical == TypeKind::TIMESTAMP &&
        logical(schema, result.ends_with("_ns") ? "timestamp-nanos" : "timestamp-micros") &&
        schema.count("adjust-to-utc") && schema["adjust-to-utc"].isBool() &&
        schema["adjust-to-utc"].asBool() == result.starts_with("timestamptz");
  } else if (result == "uuid" || result.starts_with("fixed[")) {
    const auto width = result == "uuid" ? 16 :
        (result.ends_with(']') ? integer(std::string_view(result).substr(6, result.size() - 7)) : 0);
    valid = width > 0 && encoded == "fixed" && schema.count("size") &&
        schema["size"].isInt() && schema["size"].asInt() == width &&
        physical == TypeKind::VARBINARY && (result != "uuid" || logical(schema, "uuid"));
  } else if (result.starts_with("decimal(")) {
    const auto comma = result.find(',');
    if (comma == std::string::npos || !result.ends_with(')')) {
      throw std::runtime_error("Invalid partition decimal type");
    }
    const auto precision = integer(std::string_view(result).substr(8, comma - 8));
    auto scaleText = std::string_view(result).substr(comma + 1, result.size() - comma - 2);
    while (scaleText.starts_with(' ')) scaleText.remove_prefix(1);
    const auto scale = integer(scaleText);
    valid = (encoded == "fixed" || encoded == "bytes") && logical(schema, "decimal") &&
        schema.count("precision") && schema["precision"].isInt() &&
        schema.count("scale") && schema["scale"].isInt() && output->isDecimal() &&
        precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision &&
        schema["precision"].asInt() >= 1 && schema["precision"].asInt() <= precision &&
        schema["scale"].asInt() == scale && getDecimalPrecisionScale(*output).second == scale &&
        getDecimalPrecisionScale(*output).first >= precision;
  }
  if (!valid) throw std::runtime_error("Incompatible Iceberg partition projection: " + result);
}

int128_t decimalValue(const folly::dynamic& value, const folly::dynamic& schema) {
  if (!value.isString()) throw std::runtime_error("Invalid Avro partition decimal");
  const auto& bytes = value.asString();
  if (bytes.empty() || bytes.size() > 16 ||
      (kind(schema) == "fixed" && (!schema.count("size") ||
       !schema["size"].isInt() || schema["size"].asInt() != static_cast<int64_t>(bytes.size())))) {
    throw std::runtime_error("Invalid Avro partition decimal width");
  }
  __uint128_t bits = 0;
  for (const auto byte : bytes) bits = (bits << 8) | static_cast<uint8_t>(byte);
  if (static_cast<uint8_t>(bytes[0]) >= 128 && bytes.size() < 16) {
    bits |= (~__uint128_t{0}) << (bytes.size() * 8);
  }
  const auto result = std::bit_cast<int128_t>(bits);
  int128_t limit = 1;
  for (int64_t i = 0; i < schema["precision"].asInt(); ++i) limit *= 10;
  if (result <= -limit || result >= limit) throw std::runtime_error("Partition decimal exceeds precision");
  return result;
}

void setValue(const VectorPtr& vector, vector_size_t row,
    const folly::dynamic& value, const folly::dynamic& schema) {
  if (value.isNull()) { vector->setNull(row, true); return; }
  const auto type = vector->type();
  if (type->isDecimal()) {
    const auto decimal = decimalValue(value, schema);
    if (type->isShortDecimal()) vector->asFlatVector<int64_t>()->set(row, static_cast<int64_t>(decimal));
    else vector->asFlatVector<int128_t>()->set(row, decimal);
    return;
  }
  if (kind(schema) == "int" && (!value.isInt() ||
      value.asInt() < std::numeric_limits<int32_t>::min() ||
      value.asInt() > std::numeric_limits<int32_t>::max())) {
    throw std::runtime_error("Avro partition int32 overflow");
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      if (!value.isBool()) break;
      vector->asFlatVector<bool>()->set(row, value.asBool()); return;
    case TypeKind::INTEGER:
      if (!value.isInt()) break;
      vector->asFlatVector<int32_t>()->set(row, value.asInt()); return;
    case TypeKind::BIGINT:
      if (!value.isInt()) break;
      if (logical(schema, "time-micros") && (value.asInt() < 0 || value.asInt() >= 86'400'000'000)) break;
      vector->asFlatVector<int64_t>()->set(row, value.asInt()); return;
    case TypeKind::REAL:
      if (!value.isDouble()) break;
      vector->asFlatVector<float>()->set(row, value.asDouble()); return;
    case TypeKind::DOUBLE:
      if (!value.isDouble()) break;
      vector->asFlatVector<double>()->set(row, value.asDouble()); return;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      if (!value.isString()) break;
      const auto text = value.asString();
      if (kind(schema) == "fixed" &&
          static_cast<int64_t>(text.size()) != schema["size"].asInt()) break;
      vector->asFlatVector<StringView>()->set(row, StringView(text)); return;
    }
    case TypeKind::TIMESTAMP:
      if (!value.isInt()) break;
      vector->asFlatVector<Timestamp>()->set(row, logical(schema, "timestamp-nanos")
          ? Timestamp::fromNanos(value.asInt()) : Timestamp::fromMicros(value.asInt())); return;
    default: break;
  }
  throw std::runtime_error("Invalid typed Iceberg partition value");
}
}  // namespace

IcebergPartitionProjection::IcebergPartitionProjection(
    const folly::dynamic& writerSchema, int32_t specId,
    const std::vector<split::IcebergManifestPartitionSpec>& specs, RowTypePtr outputType)
    : output_type_(std::move(outputType)) {
  if (!output_type_ || kind(writerSchema) != "record" ||
      !writerSchema.count("fields") || !writerSchema["fields"].isArray()) {
    throw std::runtime_error("Invalid Iceberg partition tuple schema");
  }
  std::unordered_map<std::string, int32_t> unified;
  std::unordered_map<int32_t, std::string> canonicalNames;
  const split::IcebergManifestPartitionSpec* selected = nullptr;
  for (const auto& spec : specs) {
    if (spec.spec_id == specId) {
      if (selected) throw std::runtime_error("Duplicate bound partition spec");
      selected = &spec;
    }
    for (const auto& field : spec.fields) {
      if (field.name.empty()) continue;
      auto [entry, inserted] = unified.emplace(field.name, field.field_id);
      auto [canonical, newId] = canonicalNames.emplace(field.field_id, field.name);
      if (field.field_id <= 0 || (!inserted && entry->second != field.field_id) ||
          (!newId && canonical->second != field.name)) {
        throw std::runtime_error("Ambiguous unified Iceberg partition field");
      }
    }
  }
  if (!selected) throw std::runtime_error("Iceberg partition spec is unbound");
  std::unordered_map<int32_t, const folly::dynamic*> writerFields;
  std::unordered_set<std::string> writerNames;
  for (const auto& field : writerSchema["fields"]) {
    if (!field.isObject() || !field.count("field-id") || !field["field-id"].isInt() ||
        !field.count("name") || !field["name"].isString() || field["name"].asString().empty() ||
        !writerNames.insert(field["name"].asString()).second || !field.count("type") ||
        field["field-id"].asInt() <= 0 || field["field-id"].asInt() > std::numeric_limits<int32_t>::max() ||
        !writerFields.emplace(field["field-id"].asInt(), &field).second) {
      throw std::runtime_error("Invalid partition Avro field identity");
    }
  }
  if (writerFields.size() != selected->fields.size()) throw std::runtime_error("Partition spec width differs");
  std::unordered_map<int32_t, const split::IcebergManifestPartitionField*> selectedFields;
  for (const auto& field : selected->fields) {
    if (!writerFields.contains(field.field_id) || !selectedFields.emplace(field.field_id, &field).second) {
      throw std::runtime_error("Partition writer field differs from its bound spec");
    }
  }
  for (size_t index = 0; index < output_type_->size(); ++index) {
    const auto found = unified.find(output_type_->nameOf(index));
    if (found == unified.end()) throw std::runtime_error("Unbound partition output field");
    const auto field = selectedFields.find(found->second);
    if (field == selectedFields.end()) { fields_.push_back({{}, nullptr}); continue; }
    const auto& writer = *writerFields.at(found->second);
    const auto& schema = nonNull(writer["type"]);
    validateScalar(schema, field->second->result_type, output_type_->childAt(index));
    fields_.push_back({writer["name"].asString(), schema});
  }
}

RowVectorPtr IcebergPartitionProjection::project(
    const std::vector<const folly::dynamic*>& rows, facebook::velox::memory::MemoryPool* pool) const {
  std::vector<VectorPtr> columns;
  for (size_t column = 0; column < fields_.size(); ++column) {
    auto vector = BaseVector::create(output_type_->childAt(column), rows.size(), pool);
    const auto& field = fields_[column];
    for (size_t row = 0; row < rows.size(); ++row) {
      if (!rows[row] || !rows[row]->isObject()) throw std::runtime_error("Invalid Avro partition tuple");
      if (field.writerName.empty()) { vector->setNull(row, true); continue; }
      if (!rows[row]->count(field.writerName)) throw std::runtime_error("Missing Avro partition field");
      setValue(vector, row, (*rows[row])[field.writerName], field.schema);
    }
    columns.push_back(std::move(vector));
  }
  return std::make_shared<RowVector>(pool, output_type_, nullptr, rows.size(), std::move(columns));
}
}  // namespace pangrova::velox
