// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace pangrova::text::bm25 {

/** Generation-wide BM25 statistics produced by bounded fan-in Native reduction. */
class Bm25CorpusStats final {
 public:
  ~Bm25CorpusStats();
  Bm25CorpusStats(const Bm25CorpusStats&) = delete;
  Bm25CorpusStats& operator=(const Bm25CorpusStats&) = delete;
  Bm25CorpusStats(Bm25CorpusStats&& other) noexcept;
  Bm25CorpusStats& operator=(Bm25CorpusStats&& other) noexcept;

  static void writeShard(
      const std::string& path,
      uint64_t document_count,
      uint64_t total_document_length,
      std::vector<std::pair<std::string, uint64_t>> document_frequencies);

  static void mergeSortedShards(
      const std::vector<std::string>& input_paths,
      const std::string& output_path);

  static Bm25CorpusStats load(
      const std::string& path,
      uint64_t max_resident_bytes);

  static uint64_t documentCountFromDisk(const std::string& path);
  static uint64_t residentBytesFromDisk(const std::string& path);

  uint64_t documentCount() const;
  uint64_t totalDocumentLength() const;
  double averageDocumentLength() const;
  uint64_t documentFrequency(std::string_view term) const;
  uint64_t residentBytes() const;

 private:
  struct BlockEntry final {
    std::string first_term;
    uint64_t file_offset;
    uint64_t first_term_ordinal;
  };

  Bm25CorpusStats() = default;
  void release() noexcept;

  uint64_t document_count_{0};
  uint64_t total_document_length_{0};
  uint64_t term_count_{0};
  uint64_t resident_bytes_{0};
  int file_descriptor_{-1};
  const uint8_t* mapped_data_{nullptr};
  size_t mapped_size_{0};
  std::vector<BlockEntry> blocks_;
};

}  // namespace pangrova::text::bm25
