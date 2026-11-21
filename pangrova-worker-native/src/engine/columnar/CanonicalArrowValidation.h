#pragma once

#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace pangrova::columnar {

// Validates actual Arrow values against an admitted exact wire schema.
// No data, schema or output owner is modified on failure.
arrow::Status validateCanonicalArrowBatch(
    const arrow::RecordBatch& batch, const arrow::Schema& admittedSchema);

}  // namespace pangrova::columnar
