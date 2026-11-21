// PartitionColumnInjector: Partition column injection on RowVector.
// Shared between Iceberg and Paimon partition column injection paths.
// Validates: Requirements R4.5, R12.4

#include "engine/velox/connector/PartitionColumnInjector.h"

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/type/Timestamp.h"

#include <algorithm>

namespace pangrova::velox {

namespace {

using namespace facebook::velox;

// Helper: parse a string to a typed value and create a constant vector.
// Reduces repetitive try-catch blocks in parsePartitionValue.
template <typename T, typename ParseFn>
VectorPtr parseAndCreateConstant(
    const std::string& value,
    const TypePtr& target_type,
    vector_size_t num_rows,
    memory::MemoryPool* pool,
    ParseFn parse_fn,
    const char* type_name) {
  try {
    auto parsed = parse_fn(value);
    return BaseVector::createConstant(
        target_type, variant(static_cast<T>(parsed)), num_rows, pool);
  } catch (const std::exception& e) {
    throw std::invalid_argument(
        std::string("PartitionColumnInjector: failed to parse ") +
        type_name + " value '" + value + "': " + e.what());
  }
}

// Parse a string partition value into a Velox constant vector of the target
// type. Supports: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE,
// VARCHAR, DATE, TIMESTAMP.
VectorPtr parsePartitionValue(
    const std::string& value,
    const TypePtr& target_type,
    vector_size_t num_rows,
    memory::MemoryPool* pool) {
  switch (target_type->kind()) {
    case TypeKind::BOOLEAN: {
      std::string lower_val = value;
      std::transform(lower_val.begin(), lower_val.end(),
                     lower_val.begin(), ::tolower);
      bool parsed = (lower_val == "true" || lower_val == "1");
      return BaseVector::createConstant(
          target_type, variant(parsed), num_rows, pool);
    }
    case TypeKind::TINYINT:
      return parseAndCreateConstant<int8_t>(
          value, target_type, num_rows, pool,
          [](const std::string& v) { return std::stoi(v); }, "TINYINT");
    case TypeKind::SMALLINT:
      return parseAndCreateConstant<int16_t>(
          value, target_type, num_rows, pool,
          [](const std::string& v) { return std::stoi(v); }, "SMALLINT");
    case TypeKind::INTEGER:
      return parseAndCreateConstant<int32_t>(
          value, target_type, num_rows, pool,
          [](const std::string& v) { return std::stoi(v); }, "INTEGER");
    case TypeKind::BIGINT:
      return parseAndCreateConstant<int64_t>(
          value, target_type, num_rows, pool,
          [](const std::string& v) { return std::stoll(v); }, "BIGINT");
    case TypeKind::REAL:
      return parseAndCreateConstant<float>(
          value, target_type, num_rows, pool,
          [](const std::string& v) { return std::stof(v); }, "REAL");
    case TypeKind::DOUBLE:
      return parseAndCreateConstant<double>(
          value, target_type, num_rows, pool,
          [](const std::string& v) { return std::stod(v); }, "DOUBLE");
    case TypeKind::VARCHAR:
      return BaseVector::createConstant(
          target_type, variant(value), num_rows, pool);
    case TypeKind::TIMESTAMP: {
      try {
        auto micros = static_cast<int64_t>(std::stoll(value));
        return BaseVector::createConstant(
            target_type,
            variant(facebook::velox::Timestamp::fromMicros(micros)),
            num_rows, pool);
      } catch (const std::exception& e) {
        throw std::invalid_argument(
            "PartitionColumnInjector: failed to parse TIMESTAMP value '" +
            value + "': " + e.what());
      }
    }
    default:
      throw std::invalid_argument(
          "PartitionColumnInjector: unsupported partition column type '" +
          target_type->toString() + "' for value '" + value + "'");
  }
}

}  // namespace

facebook::velox::RowVectorPtr injectPartitionColumns(
    const facebook::velox::RowVectorPtr& input,
    const std::unordered_map<std::string, std::string>& partition_values,
    const facebook::velox::RowTypePtr& output_type,
    facebook::velox::memory::MemoryPool* pool) {
  // Fast path: no partition values → return input unchanged.
  if (partition_values.empty()) {
    return input;
  }

  auto num_rows = input->size();
  auto num_output_cols = output_type->size();

  // Build input column name → child index lookup.
  const auto& input_type = input->type()->asRow();
  std::unordered_map<std::string, column_index_t> input_name_to_index;
  for (column_index_t i = 0; i < input_type.size(); ++i) {
    input_name_to_index[input_type.nameOf(i)] = i;
  }

  // Check if injection is actually needed: all output columns already
  // present in input means no work to do.
  bool needs_injection = false;
  for (size_t i = 0; i < num_output_cols; ++i) {
    if (input_name_to_index.find(output_type->nameOf(i)) ==
        input_name_to_index.end()) {
      needs_injection = true;
      break;
    }
  }
  if (!needs_injection) {
    return input;
  }

  // Assemble output children.
  std::vector<VectorPtr> output_children;
  output_children.reserve(num_output_cols);

  for (size_t i = 0; i < num_output_cols; ++i) {
    const auto& col_name = output_type->nameOf(i);
    const auto& col_type = output_type->childAt(i);

    // 1. Column exists in input → keep it.
    auto input_it = input_name_to_index.find(col_name);
    if (input_it != input_name_to_index.end()) {
      output_children.push_back(input->childAt(input_it->second));
      continue;
    }

    // 2. Column missing from input but present in partition_values →
    //    synthesize ConstantVector with type conversion.
    auto pv_it = partition_values.find(col_name);
    if (pv_it != partition_values.end()) {
      // Treat empty string as null for non-VARCHAR types.
      // Partition values are serialized as strings on the Java side;
      // an empty string for a numeric/boolean/date column means "no value"
      // rather than a valid empty string literal.
      if (pv_it->second.empty() && col_type->kind() != TypeKind::VARCHAR) {
        output_children.push_back(
            BaseVector::createNullConstant(col_type, num_rows, pool));
        continue;
      }
      output_children.push_back(
          parsePartitionValue(pv_it->second, col_type, num_rows, pool));
      continue;
    }

    // 3. Column missing from both → null constant.
    output_children.push_back(
        BaseVector::createNullConstant(col_type, num_rows, pool));
  }

  return std::make_shared<RowVector>(
      pool, output_type, BufferPtr(nullptr),
      num_rows, std::move(output_children));
}

}  // namespace pangrova::velox
