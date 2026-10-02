#include "columnar/ColumnarImportLayout.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/type.h>
#include <arrow/util/bitmap_ops.h>

#include <limits>

namespace pangrova::columnar {
namespace {
arrow::Result<std::shared_ptr<arrow::DataType>> normalizeType(
    const std::shared_ptr<arrow::DataType>& type) {
  arrow::FieldVector fields;
  for (const auto& field : type->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto child, normalizeType(field->type()));
    fields.push_back(field->WithType(std::move(child)));
  }
  switch (type->id()) {
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::LIST:
      return arrow::list(fields.at(0));
    case arrow::Type::STRUCT:
      return arrow::struct_(std::move(fields));
    case arrow::Type::MAP:
      return arrow::MapType::Make(fields.at(0),
          static_cast<const arrow::MapType&>(*type).keys_sorted());
    case arrow::Type::DICTIONARY: {
      const auto& dictionary = static_cast<const arrow::DictionaryType&>(*type);
      ARROW_ASSIGN_OR_RAISE(auto values, normalizeType(dictionary.value_type()));
      return arrow::dictionary(dictionary.index_type(), std::move(values), dictionary.ordered());
    }
    default:
      return type;
  }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> alignedBitmap(
    const std::shared_ptr<arrow::Buffer>& source, int64_t offset, int64_t length,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage) {
  if (!source || offset == 0) return source;
  ARROW_ASSIGN_OR_RAISE(auto copied,
      arrow::internal::CopyBitmap(pool.get(), source->data(), offset, length));
  usage.copiedBytes += copied->size();
  return memory::retainColumnarBuffer(std::move(copied), pool);
}

arrow::Result<std::shared_ptr<arrow::ArrayData>> normalizeData(
    const std::shared_ptr<arrow::ArrayData>& source,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage);

arrow::Status normalizeFixedList(
    const std::shared_ptr<arrow::ArrayData>& source, arrow::ArrayData& output,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage) {
  const auto dimension = static_cast<const arrow::FixedSizeListType&>(*source->type).list_size();
  if (dimension < 0 || (dimension > 0 &&
      source->length > std::numeric_limits<int32_t>::max() / dimension)) {
    return arrow::Status::CapacityError("Fixed list exceeds task list offset range");
  }
  const auto bytes = (source->length + 1) * sizeof(int32_t);
  ARROW_ASSIGN_OR_RAISE(auto offsets, arrow::AllocateBuffer(bytes, pool.get()));
  auto* values = reinterpret_cast<int32_t*>(offsets->mutable_data());
  for (int64_t row = 0; row <= source->length; ++row) values[row] = row * dimension;
  usage.allocatedOffsetBytes += offsets->capacity();
  output.buffers = {output.buffers.at(0),
      memory::retainColumnarBuffer(std::move(offsets), pool)};
  auto child = source->child_data.at(0)->Slice(source->offset * dimension,
      source->length * dimension);
  ARROW_ASSIGN_OR_RAISE(output.child_data.at(0), normalizeData(child, pool, usage));
  return arrow::Status::OK();
}

arrow::Status normalizeOffsetBuffers(
    const arrow::ArrayData& source, arrow::ArrayData& output,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage) {
  if (source.offset == 0 || source.type->id() == arrow::Type::NA) return arrow::Status::OK();
  if (source.type->id() == arrow::Type::BOOL) {
    ARROW_ASSIGN_OR_RAISE(output.buffers.at(1),
        alignedBitmap(source.buffers.at(1), source.offset, source.length, pool, usage));
    return arrow::Status::OK();
  }
  int64_t width = 0;
  int64_t length = source.length;
  switch (source.type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::MAP:
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
      width = sizeof(int32_t);
      ++length;
      break;
    case arrow::Type::LARGE_STRING:
    case arrow::Type::LARGE_BINARY:
      width = sizeof(int64_t);
      ++length;
      break;
    case arrow::Type::STRUCT:
    case arrow::Type::FIXED_SIZE_LIST:
      return arrow::Status::OK();
    default: {
      auto physical = source.type;
      if (physical->id() == arrow::Type::DICTIONARY) {
        physical = static_cast<const arrow::DictionaryType&>(*physical).index_type();
      }
      auto fixed = std::dynamic_pointer_cast<arrow::FixedWidthType>(physical);
      if (!fixed || fixed->bit_width() % 8 != 0) {
        return arrow::Status::NotImplemented("Sliced Arrow layout cannot be imported: ", physical->ToString());
      }
      width = fixed->bit_width() / 8;
    }
  }
  if (!source.buffers.at(1) && length == 0) return arrow::Status::OK();
  output.buffers.at(1) = arrow::SliceBuffer(source.buffers.at(1), source.offset * width, length * width);
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::ArrayData>> normalizeData(
    const std::shared_ptr<arrow::ArrayData>& source,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage) {
  auto output = source->Copy();
  ARROW_ASSIGN_OR_RAISE(output->type, normalizeType(source->type));
  if (!output->buffers.empty()) {
    ARROW_ASSIGN_OR_RAISE(output->buffers.at(0),
        alignedBitmap(source->buffers.at(0), source->offset, source->length, pool, usage));
  }
  ARROW_RETURN_NOT_OK(normalizeOffsetBuffers(*source, *output, pool, usage));
  if (source->type->id() == arrow::Type::FIXED_SIZE_LIST) {
    ARROW_RETURN_NOT_OK(normalizeFixedList(source, *output, pool, usage));
  } else {
    for (size_t index = 0; index < source->child_data.size(); ++index) {
      auto child = source->child_data[index];
      if (source->type->id() == arrow::Type::STRUCT) child = child->Slice(source->offset, source->length);
      ARROW_ASSIGN_OR_RAISE(output->child_data[index], normalizeData(child, pool, usage));
    }
  }
  if (source->dictionary) {
    ARROW_ASSIGN_OR_RAISE(output->dictionary, normalizeData(source->dictionary, pool, usage));
  }
  output->offset = 0;
  return output;
}
}  // namespace

arrow::Result<std::shared_ptr<arrow::Schema>> normalizeImportSchema(const arrow::Schema& schema) {
  arrow::FieldVector fields;
  for (const auto& field : schema.fields()) {
    ARROW_ASSIGN_OR_RAISE(auto type, normalizeType(field->type()));
    fields.push_back(field->WithType(std::move(type)));
  }
  return arrow::schema(std::move(fields), schema.metadata());
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> normalizeImportLayout(
    const arrow::RecordBatch& batch,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage) {
  if (!pool) return arrow::Status::Invalid("Columnar layout conversion requires its task allocator owner");
  ARROW_RETURN_NOT_OK(batch.ValidateFull());
  ARROW_ASSIGN_OR_RAISE(auto schema, normalizeImportSchema(*batch.schema()));
  arrow::ArrayVector columns;
  for (const auto& column : batch.columns()) {
    ARROW_ASSIGN_OR_RAISE(auto data, normalizeData(column->data(), pool, usage));
    columns.push_back(arrow::MakeArray(std::move(data)));
  }
  auto output = arrow::RecordBatch::Make(std::move(schema), batch.num_rows(), std::move(columns));
  ARROW_RETURN_NOT_OK(output->ValidateFull());
  return output;
}
}  // namespace pangrova::columnar
