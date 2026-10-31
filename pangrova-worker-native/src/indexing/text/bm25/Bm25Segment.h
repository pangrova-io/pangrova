// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace pangrova::text::bm25 {

struct Bm25Posting final {
  uint32_t document{0};
  uint32_t term_frequency{0};
};

struct Bm25SearchResult final {
  uint32_t document{0};
  double score{0};
};

/** Incremental builder for one immutable source-object postings segment. */
class Bm25SegmentBuilder final {
 public:
  void addDocument(std::string_view text);
  void write(const std::string& path) const;
  void writeStatistics(const std::string& path) const;
  uint64_t documentCount() const;
  uint64_t estimatedMemoryBytes() const;

 private:
  std::vector<uint32_t> document_lengths_;
  std::unordered_map<std::string, std::vector<Bm25Posting>> postings_;
  uint64_t token_bytes_{0};
  uint64_t posting_count_{0};
};

/** Compact read-only BM25 segment used by the Native retrieval data plane. */
class Bm25Segment final {
 public:
  struct TermEntry final {
    std::string term;
    uint64_t posting_offset{0};
    uint32_t posting_count{0};
  };

  static Bm25Segment load(
      const std::string& path,
      uint64_t max_resident_bytes);
  static uint64_t residentBytesFromDisk(const std::string& path);

  std::vector<Bm25SearchResult> search(
      const std::vector<std::string>& query_tokens,
      uint64_t limit,
      double k1,
      double b,
      const class Bm25CorpusStats& corpus_stats) const;

  uint64_t documentCount() const;
  uint64_t residentBytes() const;

 private:
  std::vector<uint32_t> document_lengths_;
  std::vector<TermEntry> terms_;
  std::vector<Bm25Posting> postings_;
  uint64_t total_document_length_{0};
  uint64_t resident_bytes_{0};
};

}  // namespace pangrova::text::bm25
