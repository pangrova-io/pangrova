#include "write/PhysicalWriteArrowExport.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <stdexcept>
#include <vector>

namespace pangrova::write {
namespace {

std::shared_ptr<arrow::ArrayData> bindData(
    const std::shared_ptr<arrow::ArrayData>& source,
    const std::shared_ptr<arrow::DataType>& target);

std::shared_ptr<arrow::Field> bindChild(
    const std::shared_ptr<arrow::ArrayData>& source,
    const std::shared_ptr<arrow::Field>& target,
    std::shared_ptr<arrow::ArrayData>& output) {
  output = bindData(source, target->type());
  return target->WithType(output->type);
}

void bindStruct(
    arrow::ArrayData& output, const arrow::StructType& target) {
  if (output.child_data.size() != static_cast<size_t>(target.num_fields())) {
    throw std::invalid_argument("Physical ROW child count differs from output");
  }
  arrow::FieldVector fields;
  for (int i = 0; i < target.num_fields(); ++i) {
    fields.push_back(bindChild(output.child_data[i], target.field(i),
        output.child_data[i]));
  }
  output.type = arrow::struct_(std::move(fields));
}

void bindList(arrow::ArrayData& output,
    const arrow::BaseListType& target) {
  if (output.child_data.size() != 1) {
    throw std::invalid_argument("Physical ARRAY must retain its one value child");
  }
  auto field = bindChild(output.child_data[0], target.value_field(),
      output.child_data[0]);
  if (output.type->id() == arrow::Type::LIST) {
    output.type = arrow::list(std::move(field));
  } else {
    const auto& source = static_cast<const arrow::FixedSizeListType&>(*output.type);
    if (target.id() != arrow::Type::FIXED_SIZE_LIST || source.list_size() !=
        static_cast<const arrow::FixedSizeListType&>(target).list_size()) {
      throw std::invalid_argument("Physical fixed ARRAY dimension/layout differs");
    }
    output.type = arrow::fixed_size_list(std::move(field), source.list_size());
  }
}

void bindMap(arrow::ArrayData& output, const arrow::MapType& target) {
  const auto& source = static_cast<const arrow::MapType&>(*output.type);
  if (source.keys_sorted() != target.keys_sorted() || output.child_data.size() != 1) {
    throw std::invalid_argument("Physical MAP layout/order differs from output");
  }
  auto entries = bindChild(output.child_data[0], target.value_field(),
      output.child_data[0]);
  auto result = arrow::MapType::Make(std::move(entries), target.keys_sorted());
  if (!result.ok()) {
    throw std::invalid_argument("Invalid physical MAP metadata: " + result.status().ToString());
  }
  output.type = *result;
}

std::shared_ptr<arrow::ArrayData> bindData(
    const std::shared_ptr<arrow::ArrayData>& source,
    const std::shared_ptr<arrow::DataType>& target) {
  // Copy() retains the original buffers, offsets, lengths, validity bitmap and
  // child owners. No values are flattened, numerically cast, or reconstructed.
  auto output = source->Copy();
  if (source->type->id() == arrow::Type::STRUCT && target->id() == arrow::Type::STRUCT) {
    bindStruct(*output, static_cast<const arrow::StructType&>(*target));
  } else if ((source->type->id() == arrow::Type::LIST ||
      source->type->id() == arrow::Type::FIXED_SIZE_LIST) &&
      (target->id() == arrow::Type::LIST || target->id() == arrow::Type::FIXED_SIZE_LIST)) {
    bindList(*output, static_cast<const arrow::BaseListType&>(*target));
  } else if (source->type->id() == arrow::Type::MAP && target->id() == arrow::Type::MAP) {
    bindMap(*output, static_cast<const arrow::MapType&>(*target));
  } else if (!source->type->Equals(target)) {
    throw std::invalid_argument("Physical scalar layout differs; coercion is forbidden");
  }
  return output;
}

}  // namespace

std::shared_ptr<arrow::RecordBatch> bindPhysicalWriteMetadata(
    const arrow::RecordBatch& source, const arrow::Schema& target) {
  if (source.num_columns() != target.num_fields() || source.num_columns() == 0) {
    throw std::invalid_argument("Physical write requires the complete allocated output schema");
  }
  arrow::FieldVector fields;
  arrow::ArrayVector columns;
  for (int i = 0; i < source.num_columns(); ++i) {
    auto data = bindData(source.column(i)->data(), target.field(i)->type());
    fields.push_back(target.field(i)->WithType(data->type));
    columns.push_back(arrow::MakeArray(std::move(data)));
  }
  auto output = arrow::RecordBatch::Make(
      arrow::schema(std::move(fields), target.metadata()), source.num_rows(), std::move(columns));
  auto status = output->ValidateFull();
  if (!status.ok()) {
    throw std::invalid_argument("Invalid physical write buffers: " + status.ToString());
  }
  return output;
}

}  // namespace pangrova::write
