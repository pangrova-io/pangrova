#pragma once

#include <memory>

#include "arrow/record_batch.h"
#include "substrait/type.pb.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"

namespace pangrova::velox {

// Uses the sole physical type projection and actual SDK Bridge representation.
std::shared_ptr<arrow::Schema> canonicalArrowSchema(
    const substrait::NamedStruct& declared,
    facebook::velox::memory::MemoryPool* pool);

// Rebinds schema metadata without copying any payload buffer.
std::shared_ptr<arrow::RecordBatch> bindCanonicalArrowSchema(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& schema);

// Logical wire names are authoritative; only task-local top-level identities change.
std::shared_ptr<arrow::Schema> physicalArrowSchema(
    const std::shared_ptr<arrow::Schema>& schema,
    const substrait::NamedStruct& declared,
    const facebook::velox::RowTypePtr& expectedType);

}  // namespace pangrova::velox
