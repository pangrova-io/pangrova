// SchemaEvolution: Column mapping and type promotion on RowVector.
// Shared between Iceberg and Paimon schema evolution paths.
// Validates: Requirements R6.6, R13.4

#include "velox/connector/SchemaEvolution.h"

#include <algorithm>
#include <functional>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/type/DecimalUtil.h"

namespace pangrova::velox {

namespace {

// Resolve Velox type from a string type name used in TypePromotionConfig.
// Uses a lookup map for O(1) dispatch instead of if/else chain.
facebook::velox::TypePtr resolveType(const std::string& type_name) {
  // Normalize to lowercase for case-insensitive matching.
  std::string lower = type_name;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

  // Static lookup table for simple type names.
  static const std::unordered_map<std::string, std::function<facebook::velox::TypePtr()>>
      kTypeMap = {
    {"int",       [] { return facebook::velox::INTEGER(); }},
    {"integer",   [] { return facebook::velox::INTEGER(); }},
    {"long",      [] { return facebook::velox::BIGINT(); }},
    {"bigint",    [] { return facebook::velox::BIGINT(); }},
    {"float",     [] { return facebook::velox::REAL(); }},
    {"real",      [] { return facebook::velox::REAL(); }},
    {"double",    [] { return facebook::velox::DOUBLE(); }},
    {"string",    [] { return facebook::velox::VARCHAR(); }},
    {"varchar",   [] { return facebook::velox::VARCHAR(); }},
    {"boolean",   [] { return facebook::velox::BOOLEAN(); }},
    {"smallint",  [] { return facebook::velox::SMALLINT(); }},
    {"tinyint",   [] { return facebook::velox::TINYINT(); }},
    {"date",      [] { return facebook::velox::DATE(); }},
    {"timestamp", [] { return facebook::velox::TIMESTAMP(); }},
    {"varbinary", [] { return facebook::velox::VARBINARY(); }},
  };

  auto it = kTypeMap.find(lower);
  if (it != kTypeMap.end()) {
    return it->second();
  }

  // DECIMAL: expect format "decimal(precision,scale)".
  if (lower.size() > 8 && lower.substr(0, 8) == "decimal(") {
    // Validate closing parenthesis exists.
    if (lower.back() != ')') {
      throw std::invalid_argument(
          "SchemaEvolution: malformed decimal type '" + type_name +
          "' — missing closing parenthesis");
    }
    auto inner = lower.substr(8, lower.size() - 9);
    auto comma = inner.find(',');
    if (comma != std::string::npos) {
      int precision = std::stoi(inner.substr(0, comma));
      int scale = std::stoi(inner.substr(comma + 1));
      return facebook::velox::DECIMAL(precision, scale);
    }
  }

  throw std::invalid_argument(
      "SchemaEvolution: unsupported type name '" + type_name + "'");
}

// Type-dispatch helper: read source value as SourceCppType, widen to
// TargetCppType, write into target FlatVector.
template <typename SourceCppType, typename TargetCppType>
void widenValues(
    const facebook::velox::VectorPtr& source,
    const facebook::velox::VectorPtr& target) {
  facebook::velox::DecodedVector src(*source);
  auto dst = target->asFlatVector<TargetCppType>();
  auto size = source->size();
  for (facebook::velox::vector_size_t i = 0; i < size; ++i) {
    if (src.isNullAt(i)) {
      target->setNull(i, true);
    } else {
      dst->set(i, static_cast<TargetCppType>(src.valueAt<SourceCppType>(i)));
    }
  }
}

// Cast a single column vector from source type to target type.
// Uses a dispatch map keyed by (source, target) TypeKind pair for O(1) lookup.
facebook::velox::VectorPtr castColumn(
    const facebook::velox::VectorPtr& source,
    const facebook::velox::TypePtr& target_type,
    facebook::velox::memory::MemoryPool* pool) {
  auto size = source->size();
  auto result = facebook::velox::BaseVector::create(target_type, size, pool);

  if (source->type()->isDecimal() && target_type->isDecimal()) {
    const auto [source_precision, source_scale] = facebook::velox::getDecimalPrecisionScale(*source->type());
    const auto [target_precision, target_scale] = facebook::velox::getDecimalPrecisionScale(*target_type);
    if (target_scale < source_scale || target_precision - target_scale < source_precision - source_scale) {
      throw std::invalid_argument("Physical decimal schema evolution must preserve precision and scale");
    }
    facebook::velox::DecodedVector decoded(*source);
    for (facebook::velox::vector_size_t row = 0; row < size; ++row) {
      if (decoded.isNullAt(row)) {
        result->setNull(row, true);
        continue;
      }
      facebook::velox::int128_t value = source->type()->isShortDecimal()
          ? decoded.valueAt<int64_t>(row) : decoded.valueAt<facebook::velox::int128_t>(row);
      if (__builtin_mul_overflow(value,
              facebook::velox::DecimalUtil::kPowersOfTen[target_scale - source_scale], &value)
          || !facebook::velox::DecimalUtil::valueInPrecisionRange(value, target_precision)) {
        throw std::overflow_error("Physical decimal schema evolution exceeds target precision");
      }
      if (target_type->isShortDecimal()) result->asFlatVector<int64_t>()->set(row, static_cast<int64_t>(value));
      else result->asFlatVector<facebook::velox::int128_t>()->set(row, value);
    }
    return result;
  }

  using TK = facebook::velox::TypeKind;
  using WidenFn = void(*)(const facebook::velox::VectorPtr&,
                           const facebook::velox::VectorPtr&);

  // Encode (src, dst) as a single uint16_t key for map lookup.
  auto makeKey = [](TK src, TK dst) -> uint16_t {
    return (static_cast<uint16_t>(src) << 8) | static_cast<uint16_t>(dst);
  };

  static const std::unordered_map<uint16_t, WidenFn> kDispatch = {
    {makeKey(TK::TINYINT,  TK::SMALLINT), &widenValues<int8_t, int16_t>},
    {makeKey(TK::TINYINT,  TK::INTEGER),  &widenValues<int8_t, int32_t>},
    {makeKey(TK::TINYINT,  TK::BIGINT),   &widenValues<int8_t, int64_t>},
    {makeKey(TK::TINYINT,  TK::REAL),     &widenValues<int8_t, float>},
    {makeKey(TK::TINYINT,  TK::DOUBLE),   &widenValues<int8_t, double>},
    {makeKey(TK::SMALLINT, TK::INTEGER),  &widenValues<int16_t, int32_t>},
    {makeKey(TK::SMALLINT, TK::BIGINT),   &widenValues<int16_t, int64_t>},
    {makeKey(TK::SMALLINT, TK::REAL),     &widenValues<int16_t, float>},
    {makeKey(TK::SMALLINT, TK::DOUBLE),   &widenValues<int16_t, double>},
    {makeKey(TK::INTEGER,  TK::BIGINT),   &widenValues<int32_t, int64_t>},
    {makeKey(TK::INTEGER,  TK::REAL),     &widenValues<int32_t, float>},
    {makeKey(TK::INTEGER,  TK::DOUBLE),   &widenValues<int32_t, double>},
    {makeKey(TK::BIGINT,   TK::DOUBLE),   &widenValues<int64_t, double>},
    {makeKey(TK::BIGINT,   TK::REAL),     &widenValues<int64_t, float>},
    {makeKey(TK::REAL,     TK::DOUBLE),   &widenValues<float, double>},
  };

  auto key = makeKey(source->typeKind(), target_type->kind());
  auto it = kDispatch.find(key);
  if (it != kDispatch.end()) {
    it->second(source, result);
    return result;
  }

  throw std::invalid_argument(
      "SchemaEvolution: unsupported type promotion from " +
      source->type()->toString() + " to " + target_type->toString());
}

// Create a constant vector filled with the given default value string,
// parsed according to the target Velox type. Returns nullptr if the
// default_value string is empty or the type is unsupported (caller
// should fall back to null constant).
facebook::velox::VectorPtr createDefaultConstantVector(
    const std::string& default_value,
    const facebook::velox::TypePtr& type,
    facebook::velox::vector_size_t num_rows,
    facebook::velox::memory::MemoryPool* pool) {
  if (default_value.empty()) {
    return nullptr;
  }

  using TK = facebook::velox::TypeKind;
  using facebook::velox::variant;

  switch (type->kind()) {
    case TK::BOOLEAN: {
      std::string lower = default_value;
      std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
      bool val = (lower == "true" || lower == "1");
      return facebook::velox::BaseVector::createConstant(
          type, variant(val), num_rows, pool);
    }
    case TK::TINYINT:
      return facebook::velox::BaseVector::createConstant(
          type, variant(static_cast<int8_t>(std::stoi(default_value))),
          num_rows, pool);
    case TK::SMALLINT:
      return facebook::velox::BaseVector::createConstant(
          type, variant(static_cast<int16_t>(std::stoi(default_value))),
          num_rows, pool);
    case TK::INTEGER:
      return facebook::velox::BaseVector::createConstant(
          type, variant(static_cast<int32_t>(std::stoi(default_value))),
          num_rows, pool);
    case TK::BIGINT:
      return facebook::velox::BaseVector::createConstant(
          type, variant(static_cast<int64_t>(std::stoll(default_value))),
          num_rows, pool);
    case TK::REAL:
      return facebook::velox::BaseVector::createConstant(
          type, variant(static_cast<float>(std::stof(default_value))),
          num_rows, pool);
    case TK::DOUBLE:
      return facebook::velox::BaseVector::createConstant(
          type, variant(static_cast<double>(std::stod(default_value))),
          num_rows, pool);
    case TK::VARCHAR:
    case TK::VARBINARY:
      return facebook::velox::BaseVector::createConstant(
          type, variant(default_value), num_rows, pool);
    default:
      // Unsupported type for default value — fall back to null.
      return nullptr;
  }
}

}  // namespace

facebook::velox::TypePtr resolveSchemaEvolutionType(
    const std::string& type_name) {
  return resolveType(type_name);
}

// Resolve missing column: check default_value from mapping config,
// then fall back to null constant.
facebook::velox::VectorPtr resolveMissingColumn(
    const split::ColumnMappingConfig* mapping,
    const facebook::velox::TypePtr& target_col_type,
    facebook::velox::vector_size_t num_rows,
    facebook::velox::memory::MemoryPool* pool) {
  if (mapping && !mapping->default_value.empty()) {
    auto default_vector = createDefaultConstantVector(
        mapping->default_value, target_col_type, num_rows, pool);
    if (default_vector) {
      return default_vector;
    }
  }
  return facebook::velox::BaseVector::createNullConstant(
      target_col_type, num_rows, pool);
}

facebook::velox::RowVectorPtr applyColumnMapping(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<split::ColumnMappingConfig>& column_mappings,
    const facebook::velox::RowTypePtr& output_type,
    facebook::velox::memory::MemoryPool* pool) {
  if (column_mappings.empty() &&
      input->type()->equivalent(*output_type)) {
    return input;
  }

  auto num_rows = input->size();
  auto num_output_cols = output_type->size();

  // Build source_name → child index lookup from input RowVector.
  // Normalize column names to lowercase for case-insensitive matching.
  const auto& input_type = input->type()->asRow();
  std::unordered_map<std::string, facebook::velox::column_index_t>
      input_name_to_index;
  auto toLower = [](const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
  };
  for (facebook::velox::column_index_t i = 0; i < input_type.size(); ++i) {
    input_name_to_index[toLower(input_type.nameOf(i))] = i;
  }

  // Build target_name → ColumnMappingConfig pointer for default_value access.
  std::unordered_map<std::string, const split::ColumnMappingConfig*>
      target_to_mapping;
  for (const auto& mapping : column_mappings) {
    target_to_mapping[toLower(mapping.target_name)] = &mapping;
  }

  // Assemble output children: for each output column, find the source
  // column, or fill with default value / null constant.
  std::vector<facebook::velox::VectorPtr> output_children;
  output_children.reserve(num_output_cols);

  for (size_t i = 0; i < num_output_cols; ++i) {
    const auto& target_name = output_type->nameOf(i);
    const auto& target_col_type = output_type->childAt(i);
    auto lower_target = toLower(target_name);

    auto mapping_it = target_to_mapping.find(lower_target);
    const split::ColumnMappingConfig* mapping =
        (mapping_it != target_to_mapping.end()) ? mapping_it->second : nullptr;

    // Try mapped source column.
    if (mapping) {
      if (mapping->source_id < 0) {
        output_children.push_back(resolveMissingColumn(mapping, target_col_type, num_rows, pool));
        continue;
      }
      auto source_it = input_name_to_index.find(toLower(mapping->source_name));
      if (source_it != input_name_to_index.end()) {
        output_children.push_back(input->childAt(source_it->second));
        continue;
      }
    }

    // Try direct name match (column exists in both schemas unchanged).
    auto direct_it = input_name_to_index.find(lower_target);
    if (direct_it != input_name_to_index.end()) {
      output_children.push_back(input->childAt(direct_it->second));
      continue;
    }

    // Column not found — use default value or null.
    output_children.push_back(
        resolveMissingColumn(mapping, target_col_type, num_rows, pool));
  }

  return std::make_shared<facebook::velox::RowVector>(
      pool, output_type, facebook::velox::BufferPtr(nullptr),
      num_rows, std::move(output_children));
}

facebook::velox::RowVectorPtr applyTypePromotion(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<split::TypePromotionConfig>& type_promotions,
    facebook::velox::memory::MemoryPool* pool) {
  // No-op when no promotions configured.
  if (type_promotions.empty()) {
    return input;
  }

  const auto& input_type = input->type()->asRow();

  // P0-TC-6: Build name-based promotion lookup using column_name from proto.
  // Falls back to field_id as array index only when column_name is empty
  // (legacy backward compatibility).
  std::unordered_map<std::string, const split::TypePromotionConfig*>
      name_to_promotion;
  for (const auto& promo : type_promotions) {
    if (!promo.column_name.empty()) {
      // Preferred: use explicit column name from proto metadata.
      name_to_promotion[promo.column_name] = &promo;
    } else if (promo.field_id >= 0 &&
               static_cast<size_t>(promo.field_id) < input_type.size()) {
      // Legacy fallback: use field_id as positional index.
      // This is incorrect when field IDs don't match column positions,
      // but preserved for backward compatibility with old proto senders.
      name_to_promotion[input_type.nameOf(promo.field_id)] = &promo;
    }
  }

  // Build new children with promoted types where needed.
  auto num_cols = input_type.size();
  std::vector<facebook::velox::VectorPtr> new_children;
  std::vector<std::string> new_names;
  std::vector<facebook::velox::TypePtr> new_types;
  new_children.reserve(num_cols);
  new_names.reserve(num_cols);
  new_types.reserve(num_cols);

  bool any_promoted = false;

  for (size_t i = 0; i < num_cols; ++i) {
    const auto& col_name = input_type.nameOf(i);
    auto promo_it = name_to_promotion.find(col_name);

    if (promo_it != name_to_promotion.end()) {
      auto target_type = resolveType(promo_it->second->target_type);
      auto source_child = input->childAt(i);

      // Only cast if types actually differ.
      if (!source_child->type()->equivalent(*target_type)) {
        new_children.push_back(castColumn(source_child, target_type, pool));
        any_promoted = true;
      } else {
        new_children.push_back(source_child);
      }
      new_names.push_back(col_name);
      new_types.push_back(target_type);
    } else {
      new_children.push_back(input->childAt(i));
      new_names.push_back(col_name);
      new_types.push_back(input_type.childAt(i));
    }
  }

  // If nothing was actually promoted, return input unchanged.
  if (!any_promoted) {
    return input;
  }

  auto new_row_type = facebook::velox::ROW(
      std::move(new_names), std::move(new_types));

  return std::make_shared<facebook::velox::RowVector>(
      pool, new_row_type, facebook::velox::BufferPtr(nullptr),
      input->size(), std::move(new_children));
}

}  // namespace pangrova::velox
