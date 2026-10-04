#include "columnar/CanonicalArrowValidation.h"

#include <cstdint>

#include <arrow/array.h>
#include <arrow/type.h>

namespace pangrova::columnar {
namespace {

arrow::Status validateValue(const arrow::Array& values, const arrow::Field& field,
    int64_t row, int depth);

arrow::Status validateMap(const arrow::MapArray& map, const arrow::Field& field,
    int64_t row, int depth) {
  const auto& entry = *field.type()->field(0);
  const auto keyField = entry.type()->field(0);
  const auto key = keyField->nullable() ? keyField->WithNullable(false) : keyField;
  const auto start = map.value_offset(row);
  const auto length = map.value_length(row);
  for (int64_t offset = 0; offset < length; ++offset) {
    const auto index = start + offset;
    if (map.values()->IsNull(index)) return arrow::Status::Invalid("Canonical Arrow MAP entry contains NULL");
    ARROW_RETURN_NOT_OK(validateValue(*map.keys(), *key, index, depth + 1));
    ARROW_RETURN_NOT_OK(validateValue(*map.items(), *entry.type()->field(1), index, depth + 1));
  }
  return arrow::Status::OK();
}

template <typename List>
arrow::Status validateList(const List& list, const arrow::Field& field,
    int64_t row, int depth) {
  const auto start = list.value_offset(row);
  const auto length = list.value_length(row);
  for (int64_t offset = 0; offset < length; ++offset) {
    ARROW_RETURN_NOT_OK(validateValue(*list.values(), *field.type()->field(0), start + offset, depth + 1));
  }
  return arrow::Status::OK();
}

arrow::Status validateValue(const arrow::Array& values, const arrow::Field& field,
    int64_t row, int depth) {
  if (depth > 64 || row < 0 || row >= values.length()) {
    return arrow::Status::Invalid("Canonical Arrow value traversal is outside its admitted bounds");
  }
  if (values.IsNull(row)) {
    return field.nullable() ? arrow::Status::OK()
        : arrow::Status::Invalid("Canonical Arrow required field contains NULL: ", field.name());
  }
  switch (values.type_id()) {
    case arrow::Type::DICTIONARY: {
      const auto& dictionary = static_cast<const arrow::DictionaryArray&>(values);
      const auto valueField = field.WithType(dictionary.dictionary()->type());
      return validateValue(*dictionary.dictionary(), *valueField, dictionary.GetValueIndex(row), depth + 1);
    }
    case arrow::Type::STRUCT: {
      const auto& structure = static_cast<const arrow::StructArray&>(values);
      for (int column = 0; column < field.type()->num_fields(); ++column) {
        ARROW_RETURN_NOT_OK(validateValue(*structure.field(column), *field.type()->field(column), row, depth + 1));
      }
      return arrow::Status::OK();
    }
    case arrow::Type::LIST:
      return validateList(static_cast<const arrow::ListArray&>(values), field, row, depth);
    case arrow::Type::LARGE_LIST:
      return validateList(static_cast<const arrow::LargeListArray&>(values), field, row, depth);
    case arrow::Type::FIXED_SIZE_LIST:
      return validateList(static_cast<const arrow::FixedSizeListArray&>(values), field, row, depth);
    case arrow::Type::MAP:
      return validateMap(static_cast<const arrow::MapArray&>(values), field, row, depth);
    default:
      if (field.type()->num_fields() != 0) {
        return arrow::Status::NotImplemented("Canonical Arrow nested value has no validation traversal: ",
            field.type()->ToString());
      }
      return arrow::Status::OK();
  }
}

bool scalarWithoutDictionary(const arrow::DataType& type) {
  return type.id() != arrow::Type::DICTIONARY && type.num_fields() == 0;
}

}  // namespace

arrow::Status validateCanonicalArrowBatch(
    const arrow::RecordBatch& batch, const arrow::Schema& admittedSchema) {
  if (!batch.schema()->Equals(admittedSchema, true)) {
    return arrow::Status::TypeError("Canonical Arrow wire schema differs from its admitted schema");
  }
  ARROW_RETURN_NOT_OK(batch.ValidateFull());
  for (int column = 0; column < batch.num_columns(); ++column) {
    const auto& values = *batch.column(column);
    const auto& field = *admittedSchema.field(column);
    if (scalarWithoutDictionary(*field.type()) && (field.nullable() || values.null_count() == 0)) continue;
    for (int64_t row = 0; row < batch.num_rows(); ++row) {
      ARROW_RETURN_NOT_OK(validateValue(values, field, row, 0));
    }
  }
  return arrow::Status::OK();
}

}  // namespace pangrova::columnar
