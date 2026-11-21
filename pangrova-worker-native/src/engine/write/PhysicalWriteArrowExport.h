#pragma once

#include <memory>

namespace arrow {
class RecordBatch;
class Schema;
}  // namespace arrow

namespace pangrova::write {

// Call only after the canonical logical declaration and runtime ordinal layout
// have been validated. Rebind metadata, not values: variable lists stay variable
// until the sole Rust fixed-list adapter validates dimensions and NULL masks.
std::shared_ptr<arrow::RecordBatch> bindPhysicalWriteMetadata(
    const arrow::RecordBatch& source, const arrow::Schema& target);

}  // namespace pangrova::write
