#include "engine/velox/serde/CanonicalArrowSchema.h"

#include <stdexcept>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/type.h>

#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "engine/velox/serde/CanonicalResultNullability.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::velox {
namespace {

struct ExportedSchema {
  ArrowSchema schema{};
  ~ExportedSchema() { if (schema.release) schema.release(&schema); }
};

std::shared_ptr<arrow::DataType> nestedType(const substrait::Type& type,
    const std::shared_ptr<arrow::DataType>& physical,
    const google::protobuf::RepeatedPtrField<std::string>& names, int& ordinal);

std::shared_ptr<arrow::Field> valueField(const substrait::Type& type,
    const std::shared_ptr<arrow::Field>& physical,
    const google::protobuf::RepeatedPtrField<std::string>& names, int& ordinal,
    bool mapKey = false) {
  return physical->WithType(nestedType(type, physical->type(), names, ordinal))
      ->WithNullable(!mapKey && schema::nullable(type));
}

std::shared_ptr<arrow::DataType> nestedType(const substrait::Type& type,
    const std::shared_ptr<arrow::DataType>& physical,
    const google::protobuf::RepeatedPtrField<std::string>& names, int& ordinal) {
  if (type.has_struct_()) {
    if (physical->id() != arrow::Type::STRUCT ||
        physical->num_fields() != type.struct_().types_size()) {
      throw std::invalid_argument("Canonical Arrow ROW physical arity differs");
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int index = 0; index < type.struct_().types_size(); ++index) {
      if (ordinal >= names.size()) throw std::invalid_argument("Canonical Arrow schema is missing DFS names");
      auto field = physical->field(index)->WithName(names.Get(ordinal++));
      fields.push_back(valueField(type.struct_().types(index), field, names, ordinal));
    }
    return arrow::struct_(std::move(fields));
  }
  if (type.has_list()) {
    if (physical->id() != arrow::Type::LIST) throw std::invalid_argument("Canonical Arrow LIST physical type differs");
    return arrow::list(valueField(type.list().type(), physical->field(0), names, ordinal));
  }
  if (type.has_map()) {
    if (physical->id() != arrow::Type::MAP) throw std::invalid_argument("Canonical Arrow MAP physical type differs");
    const auto& entries = physical->field(0);
    auto key = valueField(type.map().key(), entries->type()->field(0), names, ordinal, true);
    auto value = valueField(type.map().value(), entries->type()->field(1), names, ordinal);
    auto entry = entries->WithType(arrow::struct_({std::move(key), std::move(value)}))->WithNullable(false);
    const auto& map = static_cast<const arrow::MapType&>(*physical);
    return std::make_shared<arrow::MapType>(std::move(entry), map.keys_sorted());
  }
  return physical;
}

std::shared_ptr<arrow::ArrayData> bindData(const std::shared_ptr<arrow::ArrayData>& data,
    const std::shared_ptr<arrow::DataType>& type) {
  if (!data || !type || data->type->id() != type->id() ||
      data->child_data.size() != static_cast<size_t>(type->num_fields())) {
    throw std::invalid_argument("Canonical Arrow schema cannot change a buffer layout");
  }
  if (type->num_fields() == 0 && !data->type->Equals(type)) {
    throw std::invalid_argument("Canonical Arrow schema cannot change a scalar physical type");
  }
  auto rebound = data->Copy();
  rebound->type = type;
  for (int index = 0; index < type->num_fields(); ++index) {
    rebound->child_data[index] = bindData(data->child_data[index], type->field(index)->type());
  }
  return rebound;
}

}  // namespace

std::shared_ptr<arrow::Schema> canonicalArrowSchema(
    const substrait::NamedStruct& declared,
    facebook::velox::memory::MemoryPool* pool) {
  if (!pool) throw std::invalid_argument("Canonical Arrow schema requires its task pool");
  auto vector = facebook::velox::BaseVector::create(admittedCanonicalResultType(declared), 0, pool);
  ExportedSchema exported;
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  facebook::velox::exportToArrow(vector, exported.schema, options);
  auto imported = arrow::ImportSchema(&exported.schema);
  if (!imported.ok()) throw std::runtime_error(imported.status().ToString());
  substrait::Type root;
  *root.mutable_struct_() = declared.struct_();
  int ordinal = 0;
  auto row = nestedType(root, arrow::struct_((*imported)->fields()), declared.names(), ordinal);
  if (ordinal != declared.names_size()) throw std::invalid_argument("Canonical Arrow schema has unused DFS names");
  return arrow::schema(row->fields());
}

std::shared_ptr<arrow::RecordBatch> bindCanonicalArrowSchema(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& schema) {
  if (!batch || !schema || batch->num_columns() != schema->num_fields()) {
    throw std::invalid_argument("Canonical Arrow batch schema has a different arity");
  }
  std::vector<std::shared_ptr<arrow::Array>> columns;
  for (int index = 0; index < batch->num_columns(); ++index) {
    columns.push_back(arrow::MakeArray(bindData(batch->column(index)->data(), schema->field(index)->type())));
  }
  auto result = arrow::RecordBatch::Make(schema, batch->num_rows(), std::move(columns));
  const auto status = result->ValidateFull();
  if (!status.ok()) throw std::runtime_error(status.ToString());
  return result;
}

std::shared_ptr<arrow::Schema> physicalArrowSchema(
    const std::shared_ptr<arrow::Schema>& schema,
    const substrait::NamedStruct& declared,
    const facebook::velox::RowTypePtr& expectedType) {
  const auto canonical = admittedCanonicalResultType(declared);
  if (!schema || !expectedType || expectedType->size() != canonical->size() ||
      static_cast<size_t>(schema->num_fields()) != canonical->size()) {
    throw std::invalid_argument("Arrow task schema has a different canonical arity");
  }
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (int ordinal = 0; ordinal < schema->num_fields(); ++ordinal) {
    if (*expectedType->childAt(ordinal) != *canonical->childAt(ordinal)) {
      throw std::invalid_argument("Arrow task schema differs from its canonical nested types or names");
    }
    fields.push_back(schema->field(ordinal)->WithName(expectedType->nameOf(ordinal)));
  }
  return arrow::schema(std::move(fields));
}

}  // namespace pangrova::velox
