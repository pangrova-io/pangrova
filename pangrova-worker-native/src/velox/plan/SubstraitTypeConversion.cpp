// SubstraitTypeConversion: Type and expression conversion methods for
// VeloxPlanConverter. Split from VeloxPlanConverter.cpp for maintainability.
//
// Contains: convertType, convertNamedStruct, convertExpression,
// convertFieldReference, convertDecimalLiteral, alignArgumentTypes,
// alignComparisonArguments, convertVirtualTable, and supporting free functions.
#include "velox/plan/VeloxPlanConverter.h"

#include <cstring>
#include <unordered_set>

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

#include "substrait/algebra.pb.h"

namespace pangrova::velox {

// ---------------------------------------------------------------------------
// Decimal literal conversion
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertDecimalLiteral(
    const substrait::Expression::Literal::Decimal& dec, bool nullable) {
  auto precision = static_cast<uint8_t>(dec.precision());
  auto scale = static_cast<uint8_t>(dec.scale());
  auto type = facebook::velox::DECIMAL(precision, scale);

  // Substrait stores decimal as 16-byte little-endian two's complement.
  const auto& raw = dec.value();
  if (precision <= 18) {
    int64_t unscaled = 0;
    size_t bytes_to_read = std::min(raw.size(), sizeof(int64_t));
    std::memcpy(&unscaled, raw.data(), bytes_to_read);
    // Sign-extend if the value is negative (high bit of last byte set).
    if (!raw.empty() && (raw[raw.size() - 1] & 0x80)) {
      for (size_t i = bytes_to_read; i < sizeof(int64_t); ++i) {
        reinterpret_cast<uint8_t*>(&unscaled)[i] = 0xFF;
      }
    }
    return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
        type, facebook::velox::variant(unscaled));
  }
  // Long decimal (precision > 18): read as int128_t.
  __int128_t unscaled = 0;
  size_t bytes_to_read = std::min(raw.size(), sizeof(__int128_t));
  std::memcpy(&unscaled, raw.data(), bytes_to_read);
  if (!raw.empty() && (raw[raw.size() - 1] & 0x80)) {
    for (size_t i = bytes_to_read; i < sizeof(__int128_t); ++i) {
      reinterpret_cast<uint8_t*>(&unscaled)[i] = 0xFF;
    }
  }
  return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
      type, facebook::velox::variant(static_cast<folly::int128_t>(unscaled)));
}

// ---------------------------------------------------------------------------
// List literal conversion (ARRAY[1.0, 2.0, ...] → Velox ArrayVector constant)
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertListLiteral(
    const substrait::Expression::Literal::List& list, bool nullable) {
  int num_elements = list.values_size();
  if (num_elements == 0) {
    // Empty array: create ARRAY(DOUBLE) with zero elements.
    auto array_type = facebook::velox::ARRAY(facebook::velox::DOUBLE());
    return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
        array_type, facebook::velox::variant::null(array_type->kind()));
  }

  // Infer element type from the first value.
  const auto& first = list.values(0);
  facebook::velox::TypePtr element_type;
  if (first.has_fp64()) {
    element_type = facebook::velox::DOUBLE();
  } else if (first.has_fp32()) {
    element_type = facebook::velox::REAL();
  } else if (first.has_i64()) {
    element_type = facebook::velox::BIGINT();
  } else if (first.has_i32()) {
    element_type = facebook::velox::INTEGER();
  } else if (first.has_boolean()) {
    element_type = facebook::velox::BOOLEAN();
  } else if (first.has_string()) {
    element_type = facebook::velox::VARCHAR();
  } else if (first.has_decimal()) {
    // Calcite may produce DECIMAL literals for numeric array elements.
    // Treat as DOUBLE for vector distance computation.
    element_type = facebook::velox::DOUBLE();
  } else {
    // Fallback: treat as DOUBLE (common for vector embeddings).
    element_type = facebook::velox::DOUBLE();
  }

  auto pool_holder = facebook::velox::memory::MemoryManager::getInstance()
                         ->addLeafPool("list_literal_converter");
  auto* pool = pool_holder.get();
  values_pool_holders_.push_back(std::move(pool_holder));

  // Build the elements FlatVector.
  auto elements = facebook::velox::BaseVector::create(
      element_type, num_elements, pool);
  for (int i = 0; i < num_elements; ++i) {
    const auto& val = list.values(i);
    switch (element_type->kind()) {
      case facebook::velox::TypeKind::DOUBLE:
        elements->asFlatVector<double>()->set(i, val.fp64());
        break;
      case facebook::velox::TypeKind::REAL:
        elements->asFlatVector<float>()->set(i, val.fp32());
        break;
      case facebook::velox::TypeKind::BIGINT:
        elements->asFlatVector<int64_t>()->set(i, val.i64());
        break;
      case facebook::velox::TypeKind::INTEGER:
        elements->asFlatVector<int32_t>()->set(i, val.i32());
        break;
      case facebook::velox::TypeKind::BOOLEAN:
        elements->asFlatVector<bool>()->set(i, val.boolean());
        break;
      case facebook::velox::TypeKind::VARCHAR:
        elements->asFlatVector<facebook::velox::StringView>()->set(
            i, facebook::velox::StringView(val.string()));
        break;
      default:
        break;
    }
  }

  // Build offsets and sizes buffers for a single-element ArrayVector
  // (one array containing all elements).
  auto offsets = facebook::velox::AlignedBuffer::allocate<int32_t>(1, pool);
  auto sizes = facebook::velox::AlignedBuffer::allocate<int32_t>(1, pool);
  offsets->asMutable<int32_t>()[0] = 0;
  sizes->asMutable<int32_t>()[0] = num_elements;

  auto array_type = facebook::velox::ARRAY(element_type);
  auto array_vector = std::make_shared<facebook::velox::ArrayVector>(
      pool, array_type, nullptr, 1, offsets, sizes, elements);

  return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
      facebook::velox::BaseVector::wrapInConstant(1, 0, array_vector));
}

// ---------------------------------------------------------------------------
// Implicit type coercion for Velox function arguments
// ---------------------------------------------------------------------------

// Classifies a Substrait/Velox function name into a coercion family.
// Returns: "arithmetic", "comparison", "string", "boolean", or "none".
std::string classifyFunctionFamily(const std::string& func_name) {
  // Arithmetic: operands should be widened to a common numeric type
  static const std::unordered_set<std::string> kArithmetic = {
      "plus", "minus", "multiply", "divide", "mod", "negate",
      "abs", "power", "sqrt", "add", "subtract", "modulus",
      "sum", "sum0", "avg", "min", "max", "count",
      "coalesce",
  };
  // Rounding: heterogeneous args — the value arg already matches the output
  // type, but the precision arg MUST stay integral. Aligning it to the output
  // type (double) breaks Velox signature resolution: round(double, integer)
  // exists, round(double, double) does not.
  static const std::unordered_set<std::string> kRounding = {
      "round", "ceil", "floor", "truncate",
  };
  // Comparison: operands should be cast to common type of the two operands
  static const std::unordered_set<std::string> kComparison = {
      "eq", "neq", "lt", "gt", "lte", "gte",
      "equal", "not_equal", "is_not_null", "is_null",
      "between", "in",
  };
  // String: preserve argument types (position/length args are integers)
  static const std::unordered_set<std::string> kString = {
      "concat", "length", "upper", "lower", "substr", "substring",
      "like", "trim", "char_length", "replace", "split",
  };
  // Boolean: no coercion needed
  static const std::unordered_set<std::string> kBoolean = {
      "and", "or", "not",
  };
  // Collection: subscript/contains have heterogeneous arg types — must NOT align
  static const std::unordered_set<std::string> kCollection = {
      "subscript", "contains", "element_at", "array_contains",
  };
  // Datetime: input types differ from output type (e.g. year(date)->bigint)
  // — arguments must NOT be aligned to the output type.
  static const std::unordered_set<std::string> kDatetime = {
      "year", "month", "day", "hour", "minute", "second",
      "quarter", "day_of_week", "day_of_year", "week",
      "date_diff", "date_add", "date_sub", "date_trunc",
      "from_unixtime", "to_unixtime",
  };
  if (kArithmetic.count(func_name)) return "arithmetic";
  if (kRounding.count(func_name)) return "rounding";
  if (kComparison.count(func_name)) return "comparison";
  if (kString.count(func_name)) return "string";
  if (kBoolean.count(func_name)) return "boolean";
  if (kDatetime.count(func_name)) return "datetime";
  if (kCollection.count(func_name)) return "collection";
  return "none";
}

void VeloxPlanConverter::alignArgumentTypes(
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args,
    const facebook::velox::TypePtr& target_type) {
  for (auto& arg : args) {
    if (arg->type()->kindEquals(target_type)) {
      continue;
    }
    // Skip boolean arguments (conditions in IF/AND/OR should not be cast).
    if (arg->type()->kind() == facebook::velox::TypeKind::BOOLEAN) {
      continue;
    }
    // Skip complex types (ARRAY, MAP, ROW) — they cannot be cast to scalar.
    if (arg->type()->kind() == facebook::velox::TypeKind::ARRAY ||
        arg->type()->kind() == facebook::velox::TypeKind::MAP ||
        arg->type()->kind() == facebook::velox::TypeKind::ROW) {
      continue;
    }
    arg = std::make_shared<facebook::velox::core::CastTypedExpr>(
        target_type,
        std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>{
            arg},
        /*nullOnFailure=*/false);
  }
}

// Coerces comparison function arguments to their common type (not output type).
void VeloxPlanConverter::alignComparisonArguments(
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args) {
  if (args.size() < 2) return;

  // Explicit type hierarchy for numeric widening.
  // TypeKind enum ordinals are NOT guaranteed to follow numeric width order,
  // so we use an explicit priority map instead of ordinal comparison.
  auto typePriority = [](facebook::velox::TypeKind kind) -> int {
    switch (kind) {
      case facebook::velox::TypeKind::BOOLEAN: return 0;
      case facebook::velox::TypeKind::TINYINT: return 1;
      case facebook::velox::TypeKind::SMALLINT: return 2;
      case facebook::velox::TypeKind::INTEGER: return 3;
      case facebook::velox::TypeKind::BIGINT: return 4;
      case facebook::velox::TypeKind::REAL: return 5;
      case facebook::velox::TypeKind::DOUBLE: return 6;
      default: return -1;
    }
  };

  auto common_type = args[0]->type();
  int common_priority = typePriority(common_type->kind());
  for (size_t i = 1; i < args.size(); ++i) {
    auto arg_type = args[i]->type();
    int arg_priority = typePriority(arg_type->kind());
    if (arg_priority > common_priority) {
      common_type = arg_type;
      common_priority = arg_priority;
    }
  }
  alignArgumentTypes(args, common_type);
}

// ---------------------------------------------------------------------------
// Type conversion
// ---------------------------------------------------------------------------

facebook::velox::TypePtr VeloxPlanConverter::convertType(
    const substrait::Type& type) {
  switch (type.kind_case()) {
    case substrait::Type::kBool:
      return facebook::velox::BOOLEAN();
    case substrait::Type::kI8:
      return facebook::velox::TINYINT();
    case substrait::Type::kI16:
      return facebook::velox::SMALLINT();
    case substrait::Type::kI32:
      return facebook::velox::INTEGER();
    case substrait::Type::kI64:
      return facebook::velox::BIGINT();
    case substrait::Type::kFp32:
      return facebook::velox::REAL();
    case substrait::Type::kFp64:
      return facebook::velox::DOUBLE();
    case substrait::Type::kString:
      return facebook::velox::VARCHAR();
    case substrait::Type::kBinary:
      return facebook::velox::VARBINARY();
    case substrait::Type::kDate:
      return facebook::velox::DATE();
    case substrait::Type::kTimestamp:
      return facebook::velox::TIMESTAMP();
    case substrait::Type::kStruct: {
      std::vector<facebook::velox::TypePtr> children;
      children.reserve(type.struct_().types_size());
      for (const auto& child : type.struct_().types()) {
        children.push_back(convertType(child));
      }
      // Struct without names — use positional names.
      std::vector<std::string> names;
      names.reserve(children.size());
      for (size_t i = 0; i < children.size(); ++i) {
        names.push_back("col_" + std::to_string(i));
      }
      return facebook::velox::ROW(std::move(names), std::move(children));
    }
    case substrait::Type::kDecimal: {
      auto precision = static_cast<uint8_t>(type.decimal().precision());
      auto scale = static_cast<uint8_t>(type.decimal().scale());
      return facebook::velox::DECIMAL(precision, scale);
    }
    case substrait::Type::kList: {
      auto elementType = convertType(type.list().type());
      return facebook::velox::ARRAY(std::move(elementType));
    }
    case substrait::Type::kMap: {
      auto keyType = convertType(type.map().key());
      auto valueType = convertType(type.map().value());
      return facebook::velox::MAP(std::move(keyType), std::move(valueType));
    }
    case substrait::Type::kIntervalYear:
      return facebook::velox::INTERVAL_YEAR_MONTH();
    case substrait::Type::kIntervalDay:
      return facebook::velox::INTERVAL_DAY_TIME();
    case substrait::Type::kTimestampTz:
      // Velox represents timestamp with timezone as TIMESTAMP
      // (timezone handled at expression level).
      return facebook::velox::TIMESTAMP();
    default:
      // P0-CONV-2: Unsupported types must throw — silently falling back to
      // VARCHAR produces incorrect data interpretation and corrupted results.
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported Substrait type kind: " +
          std::to_string(static_cast<int>(type.kind_case())));
  }
}

facebook::velox::TypePtr VeloxPlanConverter::convertTypeConsumingNames(
    const substrait::Type& type,
    const google::protobuf::RepeatedPtrField<std::string>& names,
    int& idx) {
  // Substrait NamedStruct flattens nested struct field names in pre-order
  // DFS: for a top-level (id BIGINT, info STRUCT(name, age, city)) the
  // names array is ["id", "info", "name", "age", "city"]. To rebuild a
  // Velox RowType that preserves nested field names (required for
  // Parquet/Iceberg readers to match against the file schema by name), we
  // consume the names array in DFS order alongside the type tree.
  if (type.kind_case() == substrait::Type::kStruct) {
    const auto& sub = type.struct_();
    std::vector<std::string> child_names;
    std::vector<facebook::velox::TypePtr> child_types;
    child_names.reserve(sub.types_size());
    child_types.reserve(sub.types_size());
    for (int i = 0; i < sub.types_size(); ++i) {
      std::string child_name;
      if (idx < names.size()) {
        child_name = names.Get(idx++);
      } else {
        child_name = "col_" + std::to_string(i);
      }
      auto child_type = convertTypeConsumingNames(sub.types(i), names, idx);
      child_names.push_back(std::move(child_name));
      child_types.push_back(std::move(child_type));
    }
    return facebook::velox::ROW(std::move(child_names), std::move(child_types));
  }
  // ARRAY<STRUCT<...>>: recurse into the element type so nested struct
  // field names are consumed from the flattened names array.
  if (type.kind_case() == substrait::Type::kList) {
    auto elem = convertTypeConsumingNames(type.list().type(), names, idx);
    return facebook::velox::ARRAY(std::move(elem));
  }
  // MAP<K, STRUCT<...>>: recurse into both key and value types.
  if (type.kind_case() == substrait::Type::kMap) {
    auto key = convertTypeConsumingNames(type.map().key(), names, idx);
    auto val = convertTypeConsumingNames(type.map().value(), names, idx);
    return facebook::velox::MAP(std::move(key), std::move(val));
  }
  // Scalar types do not consume any name; fall back to the basic converter.
  return convertType(type);
}

facebook::velox::RowTypePtr VeloxPlanConverter::convertNamedStruct(
    const substrait::NamedStruct& named_struct) {
  const auto& struct_type = named_struct.struct_();
  const auto& names = named_struct.names();
  std::vector<std::string> top_names;
  std::vector<facebook::velox::TypePtr> top_types;
  int type_count = struct_type.types_size();
  top_names.reserve(type_count);
  top_types.reserve(type_count);

  int idx = 0;
  for (int i = 0; i < type_count; ++i) {
    std::string name;
    if (idx < names.size()) {
      name = names.Get(idx++);
    } else {
      name = "col_" + std::to_string(i);
    }
    auto child_type =
        convertTypeConsumingNames(struct_type.types(i), names, idx);
    top_names.push_back(std::move(name));
    top_types.push_back(std::move(child_type));
  }
  return facebook::velox::ROW(std::move(top_names), std::move(top_types));
}

// ---------------------------------------------------------------------------
// Expression conversion
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertFieldReference(
  const substrait::Expression_FieldReference& field_ref,
    const facebook::velox::RowTypePtr& input_type) {
  if (field_ref.has_direct_reference() &&
      field_ref.direct_reference().has_struct_field()) {
    int field_idx = field_ref.direct_reference().struct_field().field();
    if (field_idx >= 0 &&
        field_idx < static_cast<int>(input_type->size())) {

      // Check for nested struct field access (e.g., info.age)
      // Proto: struct_field { field: 1, child { struct_field { field: 1 } } }
      const auto& top_struct = field_ref.direct_reference().struct_field();
      if (top_struct.has_child() && top_struct.child().has_struct_field()) {
        int child_idx = top_struct.child().struct_field().field();
        auto parent_type = input_type->childAt(field_idx);
        auto row_parent = std::dynamic_pointer_cast<const facebook::velox::RowType>(parent_type);
        if (!row_parent) {
          throw std::runtime_error(
              "VeloxPlanConverter: nested struct_field but parent type at index " +
              std::to_string(field_idx) + " is not ROW");
        }
        if (child_idx < 0 || child_idx >= static_cast<int>(row_parent->size())) {
          throw std::runtime_error(
              "VeloxPlanConverter: nested field index " +
              std::to_string(child_idx) + " out of bounds for struct with " +
              std::to_string(row_parent->size()) + " fields");
        }
        // FieldAccessTypedExpr for the parent struct column
        auto parent_expr = std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            parent_type, input_type->nameOf(field_idx));
        // DereferenceTypedExpr for the nested field
        return std::make_shared<facebook::velox::core::DereferenceTypedExpr>(
            row_parent->childAt(child_idx), parent_expr, child_idx);
      }

      return std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
          input_type->childAt(field_idx),
          input_type->nameOf(field_idx));
    }
    // P0-CONV-1: Out-of-bounds field reference must throw, not silently
    // return column 0 — returning wrong column produces incorrect results.
    throw std::runtime_error(
        "VeloxPlanConverter: field reference index " +
        std::to_string(field_idx) + " out of bounds for schema with " +
        std::to_string(input_type->size()) + " columns");
  }
  // Malformed reference (no direct_reference or struct_field).
  throw std::runtime_error(
      "VeloxPlanConverter: malformed field reference — "
      "expected direct_reference.struct_field");
}

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertExpression(
    const substrait::Expression& expr,
    const facebook::velox::RowTypePtr& input_type) {
  switch (expr.rex_type_case()) {
    case substrait::Expression::kSelection:
      return convertFieldReference(expr.selection(), input_type);

    case substrait::Expression::kLiteral: {
      const auto& lit = expr.literal();
      if (lit.has_boolean()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::BOOLEAN(),
            facebook::velox::variant(lit.boolean()));
      }
      if (lit.has_i8()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::TINYINT(),
            facebook::velox::variant(static_cast<int8_t>(lit.i8())));
      }
      if (lit.has_i16()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::SMALLINT(),
            facebook::velox::variant(static_cast<int16_t>(lit.i16())));
      }
      if (lit.has_i32()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::INTEGER(),
            facebook::velox::variant(static_cast<int32_t>(lit.i32())));
      }
      if (lit.has_i64()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::BIGINT(),
            facebook::velox::variant(static_cast<int64_t>(lit.i64())));
      }
      if (lit.has_fp64()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::DOUBLE(),
            facebook::velox::variant(lit.fp64()));
      }
      if (lit.has_string()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::VARCHAR(),
            facebook::velox::variant(lit.string()));
      }
      if (lit.has_fp32()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::REAL(),
            facebook::velox::variant(lit.fp32()));
      }
      if (lit.has_decimal()) {
        return convertDecimalLiteral(lit.decimal(), lit.nullable());
      }
      if (lit.has_date()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::DATE(),
            facebook::velox::variant(static_cast<int32_t>(lit.date())));
      }
      if (lit.has_timestamp()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::TIMESTAMP(),
            facebook::velox::variant(
                facebook::velox::Timestamp::fromMicros(lit.timestamp())));
      }
      if (lit.has_binary()) {
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            facebook::velox::VARBINARY(),
            facebook::velox::variant(lit.binary()));
      }
      if (lit.has_null()) {
        auto null_type = convertType(lit.null());
        return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            null_type, facebook::velox::variant::null(null_type->kind()));
      }
      if (lit.has_list()) {
        return convertListLiteral(lit.list(), lit.nullable());
      }
      throw std::runtime_error(
          "Unsupported Substrait literal type: " +
          std::to_string(lit.literal_type_case()));
    }

    case substrait::Expression::kScalarFunction: {
      const auto& func = expr.scalar_function();
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          args;
      args.reserve(func.arguments_size());
      for (const auto& arg : func.arguments()) {
        if (arg.has_value()) {
          args.push_back(convertExpression(arg.value(), input_type));
        }
      }
      auto output_type = convertType(func.output_type());
      std::string func_name;
      auto it = function_map_.find(func.function_reference());
      if (it != function_map_.end()) {
        func_name = it->second;
      } else {
        func_name = "func_" + std::to_string(func.function_reference());
      }
      // Velox Presto function set registers "is_null" but NOT "is_not_null".
      // Rewrite is_not_null(x) → not(is_null(x)).
      if (func_name == "is_not_null") {
        auto is_null_expr =
            std::make_shared<facebook::velox::core::CallTypedExpr>(
                output_type, std::move(args), "is_null");
        return std::make_shared<facebook::velox::core::CallTypedExpr>(
            output_type,
            std::vector<std::shared_ptr<
                const facebook::velox::core::ITypedExpr>>{is_null_expr},
            "not");
      }
      auto family = classifyFunctionFamily(func_name);
      // "rounding" keeps heterogeneous args as-is (value + integral precision).
      if (family == "arithmetic" || family == "none") {
        alignArgumentTypes(args, output_type);
      } else if (family == "comparison") {
        alignComparisonArguments(args);
      }
      return std::make_shared<facebook::velox::core::CallTypedExpr>(
          output_type, std::move(args), func_name);
    }

    case substrait::Expression::kCast: {
      const auto& cast = expr.cast();
      auto cast_input = convertExpression(cast.input(), input_type);
      auto target_type = convertType(cast.type());
      return std::make_shared<facebook::velox::core::CastTypedExpr>(
          target_type,
          std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>{
              cast_input},
          /*nullOnFailure=*/false);
    }

    case substrait::Expression::kIfThen: {
      const auto& if_then = expr.if_then();
      int clause_count = if_then.ifs_size();
      if (clause_count == 0) {
        if (if_then.has_else_()) {
          return convertExpression(if_then.else_(), input_type);
        }
        // P1-BUG-6: IfThen with no clauses and no else is a degenerate plan.
        // Previously returned BOOLEAN null which would cause type mismatch
        // if the expected column type is non-boolean.
        throw std::runtime_error(
            "VeloxPlanConverter: IfThen expression has no clauses and no else");
      }

      // Convert all then-branches first to determine the dominant result type.
      // Velox requires all branches of if/switch to have identical types.
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          then_exprs;
      then_exprs.reserve(clause_count);
      for (int i = 0; i < clause_count; ++i) {
        then_exprs.push_back(
            convertExpression(if_then.ifs(i).then(), input_type));
      }

      // Determine result_type from the first then-branch.
      auto result_type = then_exprs[0]->type();

      std::shared_ptr<const facebook::velox::core::ITypedExpr> else_expr;
      if (if_then.has_else_()) {
        else_expr = convertExpression(if_then.else_(), input_type);
      } else {
        else_expr = std::make_shared<facebook::velox::core::ConstantTypedExpr>(
            result_type,
            facebook::velox::variant::null(result_type->kind()));
      }

      // Helper: cast expr to result_type if types differ.
      auto coerceToResult = [&](
          std::shared_ptr<const facebook::velox::core::ITypedExpr> e)
          -> std::shared_ptr<const facebook::velox::core::ITypedExpr> {
        if (!e->type()->kindEquals(result_type)) {
          return std::make_shared<facebook::velox::core::CastTypedExpr>(
              result_type,
              std::vector<std::shared_ptr<
                  const facebook::velox::core::ITypedExpr>>{e},
              /*nullOnFailure=*/false);
        }
        return e;
      };

      auto current = coerceToResult(else_expr);
      for (int i = clause_count - 1; i >= 0; --i) {
        auto condition = convertExpression(
            if_then.ifs(i).if_(), input_type);
        auto then_expr = coerceToResult(then_exprs[i]);
        current = std::make_shared<facebook::velox::core::CallTypedExpr>(
            result_type,
            std::vector<std::shared_ptr<
                const facebook::velox::core::ITypedExpr>>{
                condition, then_expr, current},
            "if");
      }
      return current;
    }

    case substrait::Expression::kWindowFunction: {
      const auto& wf = expr.window_function();
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          args;
      for (const auto& arg : wf.arguments()) {
        if (arg.has_value()) {
          args.push_back(convertExpression(arg.value(), input_type));
        }
      }
      auto output_type = convertType(wf.output_type());
      std::string func_name;
      auto it = function_map_.find(wf.function_reference());
      if (it != function_map_.end()) {
        func_name = it->second;
      } else {
        func_name =
            "window_func_" + std::to_string(wf.function_reference());
      }
      return std::make_shared<facebook::velox::core::CallTypedExpr>(
          output_type, std::move(args), func_name);
    }

    default:
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported Substrait expression type: " +
          std::to_string(static_cast<int>(expr.rex_type_case())));
  }
}

// ---------------------------------------------------------------------------
// VirtualTable conversion (inline literal data → ValuesNode)
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertVirtualTable(
    const substrait::ReadRel& read_rel,
    const facebook::velox::RowTypePtr& output_type) {
  auto pool_holder = facebook::velox::memory::MemoryManager::getInstance()
                         ->addLeafPool("values_converter");
  auto* pool = pool_holder.get();
  values_pool_holders_.push_back(std::move(pool_holder));

  const auto& vt = read_rel.virtual_table();
  int num_rows = vt.values_size();
  int num_cols = static_cast<int>(output_type->size());

  if (num_rows == 0) {
    // P3-BUG-24: Create a true empty ValuesNode (0 rows) instead of a
    // ghost row with zero values. The previous behavior of forcing 1 row
    // could corrupt aggregation counts and LIMIT results.
    std::vector<facebook::velox::VectorPtr> children;
    children.reserve(num_cols);
    for (int c = 0; c < num_cols; ++c) {
      auto child = facebook::velox::BaseVector::create(
          output_type->childAt(c), 0, pool);
      children.push_back(std::move(child));
    }
    auto row_vector = std::make_shared<facebook::velox::RowVector>(
        pool, output_type, nullptr, 0, std::move(children));
    return std::make_shared<facebook::velox::core::ValuesNode>(
        nextNodeId(), std::vector<facebook::velox::RowVectorPtr>{row_vector});
  }

  // Non-empty virtual table: convert each Literal.Struct row.
  std::vector<facebook::velox::VectorPtr> children;
  children.reserve(num_cols);
  for (int c = 0; c < num_cols; ++c) {
    children.push_back(facebook::velox::BaseVector::create(
        output_type->childAt(c), num_rows, pool));
  }

  for (int r = 0; r < num_rows; ++r) {
    const auto& row_struct = vt.values(r);
    for (int c = 0; c < num_cols && c < row_struct.fields_size(); ++c) {
      const auto& lit = row_struct.fields(c);
      auto kind = output_type->childAt(c)->kind();
      switch (kind) {
        case facebook::velox::TypeKind::BIGINT:
          children[c]->asFlatVector<int64_t>()->set(r, lit.i64());
          break;
        case facebook::velox::TypeKind::INTEGER:
          if (lit.has_date()) {
            children[c]->asFlatVector<int32_t>()->set(r, lit.date());
          } else {
            children[c]->asFlatVector<int32_t>()->set(r, lit.i32());
          }
          break;
        case facebook::velox::TypeKind::SMALLINT:
          children[c]->asFlatVector<int16_t>()->set(
              r, static_cast<int16_t>(lit.i32()));
          break;
        case facebook::velox::TypeKind::TINYINT:
          children[c]->asFlatVector<int8_t>()->set(
              r, static_cast<int8_t>(lit.i32()));
          break;
        case facebook::velox::TypeKind::BOOLEAN:
          children[c]->asFlatVector<bool>()->set(r, lit.boolean());
          break;
        case facebook::velox::TypeKind::DOUBLE:
          children[c]->asFlatVector<double>()->set(r, lit.fp64());
          break;
        case facebook::velox::TypeKind::REAL:
          children[c]->asFlatVector<float>()->set(r, lit.fp32());
          break;
        case facebook::velox::TypeKind::VARCHAR:
          children[c]->asFlatVector<facebook::velox::StringView>()->set(
              r, facebook::velox::StringView(lit.string()));
          break;
        case facebook::velox::TypeKind::VARBINARY:
          children[c]->asFlatVector<facebook::velox::StringView>()->set(
              r, facebook::velox::StringView(lit.string()));
          break;
        case facebook::velox::TypeKind::TIMESTAMP: {
          auto ts = facebook::velox::Timestamp::fromMicros(lit.timestamp());
          children[c]->asFlatVector<facebook::velox::Timestamp>()->set(r, ts);
          break;
        }
        case facebook::velox::TypeKind::HUGEINT: {
          const auto& dec = lit.decimal();
          const auto& dec_bytes = dec.value();
          facebook::velox::int128_t val = 0;
          if (!dec_bytes.empty()) {
            size_t bytes_to_read = std::min(dec_bytes.size(), sizeof(val));
            std::memcpy(&val, dec_bytes.data(), bytes_to_read);
            // Sign-extend negative values (high bit of last byte set).
            if (dec_bytes[dec_bytes.size() - 1] & 0x80) {
              for (size_t i = bytes_to_read; i < sizeof(val); ++i) {
                reinterpret_cast<uint8_t*>(&val)[i] = 0xFF;
              }
            }
          }
          children[c]->asFlatVector<facebook::velox::int128_t>()->set(r, val);
          break;
        }
        default:
          // Unsupported literal type in VirtualTable: leave zero-initialized.
          break;
      }
    }
  }

  auto row_vector = std::make_shared<facebook::velox::RowVector>(
      pool, output_type, nullptr, num_rows, std::move(children));
  return std::make_shared<facebook::velox::core::ValuesNode>(
      nextNodeId(), std::vector<facebook::velox::RowVectorPtr>{row_vector});
}

}  // namespace pangrova::velox
