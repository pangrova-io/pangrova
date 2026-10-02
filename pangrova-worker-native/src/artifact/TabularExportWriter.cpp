// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "artifact/TabularExportWriter.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <stdexcept>

#include "velox/vector/LazyVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::artifact {

TabularExportWriter::TabularExportWriter(
    std::string path,
    facebook::velox::memory::MemoryPool* pool,
    const facebook::velox::RowTypePtr& row_type)
    : path_(std::move(path)), pool_(pool) {
  if (path_.empty() || pool_ == nullptr || !row_type) {
    throw std::invalid_argument(
        "Tabular export path, memory pool and row type are required");
  }
  auto sink = arrow::io::FileOutputStream::Open(path_);
  if (!sink.ok()) {
    throw std::runtime_error(
        "Failed to open tabular export file: " + sink.status().ToString());
  }
  sink_ = *sink;

  auto empty = facebook::velox::BaseVector::create(row_type, 0, pool_);
  ArrowSchema schema;
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  facebook::velox::exportToArrow(empty, schema, options);
  auto imported_schema = arrow::ImportSchema(&schema);
  if (!imported_schema.ok()) {
    throw std::runtime_error(
        "Failed to convert the Native export schema to Arrow: " +
        imported_schema.status().ToString());
  }
  auto writer = arrow::ipc::MakeStreamWriter(sink_, *imported_schema);
  if (!writer.ok()) {
    throw std::runtime_error(
        "Failed to create Arrow IPC export writer: " +
        writer.status().ToString());
  }
  writer_ = *writer;
}

TabularExportWriter::~TabularExportWriter() {
  if (writer_ && !finished_) {
    (void)writer_->Close();
  }
  if (sink_) {
    (void)sink_->Close();
  }
}

void TabularExportWriter::write(
    const facebook::velox::RowVectorPtr& batch) {
  if (!batch || batch->size() == 0 || finished_) {
    throw std::invalid_argument(
        "Tabular export requires a non-empty batch before finish");
  }
  facebook::velox::LazyVector::ensureLoadedRows(
      batch, facebook::velox::SelectivityVector(batch->size()));
  ArrowArray array;
  ArrowSchema schema;
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  facebook::velox::exportToArrow(batch, array, pool_, options);
  facebook::velox::exportToArrow(batch, schema, options);
  auto imported = arrow::ImportRecordBatch(&array, &schema);
  if (!imported.ok()) {
    throw std::runtime_error(
        "Failed to convert Native rows to Arrow IPC: " +
        imported.status().ToString());
  }
  auto status = writer_->WriteRecordBatch(**imported);
  if (!status.ok()) {
    throw std::runtime_error(
        "Failed to write Arrow IPC export batch: " + status.ToString());
  }
  rows_written_ += static_cast<uint64_t>(batch->size());
  input_bytes_ += batch->estimateFlatSize();
}

void TabularExportWriter::finish() {
  if (!writer_ || !sink_ || finished_) {
    throw std::runtime_error(
        "Tabular export cannot finish without exactly one open stream");
  }
  auto writer_status = writer_->Close();
  if (!writer_status.ok()) {
    throw std::runtime_error(
        "Failed to finish Arrow IPC export: " + writer_status.ToString());
  }
  auto sink_status = sink_->Close();
  if (!sink_status.ok()) {
    throw std::runtime_error(
        "Failed to close Arrow IPC export: " + sink_status.ToString());
  }
  finished_ = true;
}

}  // namespace pangrova::artifact
