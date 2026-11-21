// MergeReaderFactory implementation.
// Validates: Requirements R8.1, R10.4, R22.2

#include "engine/merge/MergeReaderFactory.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

#include "engine/executor/NativeUnsupportedFeatureException.h"
#include "engine/merge/DeduplicateMergeReader.h"
#include "engine/merge/FirstRowMergeReader.h"
#include "engine/merge/PartialUpdateMergeReader.h"

namespace pangrova::merge {

namespace {

// Case-insensitive engine name normalization.
std::string toLower(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

}  // namespace

std::unique_ptr<MergeReader> MergeReaderFactory::create(
    const split::PaimonConfig& config,
    const facebook::velox::RowTypePtr& output_type) {
  // COW / read-optimized: no merge needed.
  if (!config.needs_merge) {
    return nullptr;
  }
  // MOW mode with deletion vectors: data already deduplicated at write time.
  if (config.deletion_vectors_enabled) {
    return nullptr;
  }

  // Defensive: validate primary_keys does not contain empty strings.
  for (const auto& key : config.primary_keys) {
    if (key.empty()) {
      throw std::invalid_argument(
          "primary_keys contains empty string. config.primary_keys=[" +
          [&]() {
            std::ostringstream oss;
            for (size_t i = 0; i < config.primary_keys.size(); ++i) {
              if (i > 0) oss << ", ";
              oss << "'" << config.primary_keys[i] << "'";
            }
            return oss.str();
          }() +
          "], output_type=" + output_type->toString());
    }
  }

  auto key_indices = resolveKeyIndices(config.primary_keys, output_type);
  auto seq_index = config.sequence_column.empty()
      ? kNoSequenceColumn
      : resolveColumnIndex(config.sequence_column, output_type);

  const auto engine = toLower(config.merge_engine);
  const MergeDeletePolicy delete_policy{config.ignore_delete, config.remove_record_on_delete};
  if (engine == "deduplicate") {
    return std::make_unique<DeduplicateMergeReader>(
        std::move(key_indices), seq_index, output_type, delete_policy);
  }
  if (engine == "partial_update") {
    return std::make_unique<PartialUpdateMergeReader>(
        std::move(key_indices), seq_index, output_type, delete_policy);
  }
  if (engine == "aggregation") {
    // Defensive: validate aggregation_specs keys do not contain empty strings.
    for (const auto& [col_name, func_name] : config.aggregation_specs) {
      if (col_name.empty()) {
        throw std::invalid_argument(
            "aggregation_specs contains empty key. config.aggregation_specs=[" +
            [&]() {
              std::ostringstream oss;
              bool first = true;
              for (const auto& [k, v] : config.aggregation_specs) {
                if (!first) oss << ", ";
                first = false;
                oss << "'" << k << "': '" << v << "'";
              }
              return oss.str();
            }() +
            "], output_type=" + output_type->toString());
      }
    }
    auto agg_specs = resolveAggSpecs(config.aggregation_specs, output_type);
    return std::make_unique<AggregationMergeReader>(
        std::move(key_indices), seq_index, output_type, std::move(agg_specs), delete_policy);
  }
  if (engine == "first_row") {
    return std::make_unique<FirstRowMergeReader>(
        std::move(key_indices), seq_index, output_type, delete_policy);
  }

  throw executor::NativeUnsupportedFeatureException(
      "merge_engine=" + config.merge_engine, "paimon", /*split_id=*/"");
}

std::vector<facebook::velox::column_index_t>
MergeReaderFactory::resolveKeyIndices(
    const std::vector<std::string>& primary_keys,
    const facebook::velox::RowTypePtr& output_type) {
  std::vector<facebook::velox::column_index_t> indices;
  indices.reserve(primary_keys.size());
  for (const auto& key : primary_keys) {
    indices.push_back(resolveColumnIndex(key, output_type));
  }
  return indices;
}

facebook::velox::column_index_t MergeReaderFactory::resolveColumnIndex(
    const std::string& column_name,
    const facebook::velox::RowTypePtr& output_type) {
  // Defensive: empty column names should never happen - fail with clear error.
  if (column_name.empty()) {
    throw std::invalid_argument(
        "Empty column_name passed to resolveColumnIndex. output_type=" +
        output_type->toString());
  }
  if (output_type->containsChild(column_name)) {
    return output_type->getChildIdx(column_name);
  }
  throw std::invalid_argument(
      "Column not found in output type: '" + column_name +
      "'. output_type=[" + output_type->toString() +
      "], available_columns=" + std::to_string(output_type->size()));
}

std::unordered_map<facebook::velox::column_index_t, AggFunction>
MergeReaderFactory::resolveAggSpecs(
    const std::unordered_map<std::string, std::string>& agg_specs,
    const facebook::velox::RowTypePtr& output_type) {
  std::unordered_map<facebook::velox::column_index_t, AggFunction> resolved;
  resolved.reserve(agg_specs.size());
  for (const auto& [col_name, func_name] : agg_specs) {
    auto col_idx = resolveColumnIndex(col_name, output_type);
    auto function = parseAggFunction(func_name);
    const auto& type = output_type->childAt(col_idx);
    if (function == AggFunction::PRODUCT && type->isDecimal()) {
      throw std::invalid_argument("Paimon Native PRODUCT does not represent decimal scale semantics");
    }
    if ((function == AggFunction::COLLECT || function == AggFunction::NESTED_UPDATE)
        && type->kind() != facebook::velox::TypeKind::ARRAY) {
      throw std::invalid_argument("Paimon collect/nested_update requires ARRAY");
    }
    if (function == AggFunction::NESTED_UPDATE && type->childAt(0)->kind() != facebook::velox::TypeKind::ROW) {
      throw std::invalid_argument("Paimon nested_update requires ARRAY<ROW>");
    }
    if (function == AggFunction::MERGE_MAP && type->kind() != facebook::velox::TypeKind::MAP) {
      throw std::invalid_argument("Paimon merge_map requires MAP");
    }
    resolved[col_idx] = function;
  }
  return resolved;
}

}  // namespace pangrova::merge
