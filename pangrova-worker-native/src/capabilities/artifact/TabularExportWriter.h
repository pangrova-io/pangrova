// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "velox/vector/ComplexVector.h"

namespace arrow::io {
class FileOutputStream;
}
namespace arrow::ipc {
class RecordBatchWriter;
}
namespace facebook::velox::memory {
class MemoryPool;
}

namespace pangrova::artifact {

/** Bounded single-threaded Arrow IPC stream for one immutable export page. */
class TabularExportWriter final {
 public:
  TabularExportWriter(
      std::string path,
      facebook::velox::memory::MemoryPool* pool,
      const facebook::velox::RowTypePtr& row_type);
  ~TabularExportWriter();

  TabularExportWriter(const TabularExportWriter&) = delete;
  TabularExportWriter& operator=(const TabularExportWriter&) = delete;

  void write(const facebook::velox::RowVectorPtr& batch);
  void finish();

  uint64_t rowsWritten() const { return rows_written_; }
  uint64_t estimatedInputBytes() const { return input_bytes_; }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
  facebook::velox::memory::MemoryPool* pool_;
  std::shared_ptr<arrow::io::FileOutputStream> sink_;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_;
  uint64_t rows_written_{0};
  uint64_t input_bytes_{0};
  bool finished_{false};
};

}  // namespace pangrova::artifact
