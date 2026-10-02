// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "text/bm25/Bm25Segment.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <queue>
#include <stdexcept>
#include <unordered_set>

#include "text/bm25/Bm25Analyzer.h"
#include "text/bm25/Bm25CorpusStats.h"

namespace pangrova::text::bm25 {
namespace {

constexpr uint64_t kMagic = 0x5047424D32350001ULL;
constexpr uint32_t kAnalyzerId = 1;
constexpr uint64_t kMaxTermBytes = 1ULL << 20;

uint64_t checkedAdd(uint64_t left, uint64_t right, const char* context) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    throw std::runtime_error(std::string("BM25 size overflow: ") + context);
  }
  return left + right;
}

uint64_t checkedMultiply(uint64_t left, uint64_t right, const char* context) {
  if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
    throw std::runtime_error(std::string("BM25 size overflow: ") + context);
  }
  return left * right;
}

void writeU32(std::ostream& output, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    output.put(static_cast<char>((value >> shift) & 0xff));
  }
}

void writeU64(std::ostream& output, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    output.put(static_cast<char>((value >> shift) & 0xff));
  }
}

uint32_t readU32(std::istream& input, const char* context) {
  uint32_t value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    const int byte = input.get();
    if (byte == std::char_traits<char>::eof()) {
      throw std::runtime_error(std::string("Truncated BM25 segment: ") + context);
    }
    value |= static_cast<uint32_t>(static_cast<unsigned char>(byte)) << shift;
  }
  return value;
}

uint64_t readU64(std::istream& input, const char* context) {
  uint64_t value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    const int byte = input.get();
    if (byte == std::char_traits<char>::eof()) {
      throw std::runtime_error(std::string("Truncated BM25 segment: ") + context);
    }
    value |= static_cast<uint64_t>(static_cast<unsigned char>(byte)) << shift;
  }
  return value;
}

struct Header final {
  uint64_t document_count;
  uint64_t term_count;
  uint64_t posting_count;
  uint64_t total_document_length;
  uint64_t resident_bytes;
};

Header readHeader(std::istream& input) {
  if (readU64(input, "magic") != kMagic ||
      readU64(input, "analyzer") != kAnalyzerId) {
    throw std::runtime_error("BM25 segment format identity is invalid");
  }
  Header header{
      readU64(input, "document count"),
      readU64(input, "term count"),
      readU64(input, "posting count"),
      readU64(input, "total document length"),
      readU64(input, "resident bytes")};
  if (header.document_count > std::numeric_limits<uint32_t>::max() ||
      header.term_count > std::numeric_limits<uint32_t>::max() ||
      header.posting_count > std::numeric_limits<uint32_t>::max() ||
      header.resident_bytes < sizeof(Bm25Segment)) {
    throw std::runtime_error("BM25 segment header exceeds safe bounds");
  }
  return header;
}

uint64_t structuralBytes(
    uint64_t documents,
    uint64_t terms,
    uint64_t postings,
    uint64_t term_bytes) {
  uint64_t bytes = sizeof(Bm25Segment);
  bytes = checkedAdd(bytes, checkedMultiply(documents, sizeof(uint32_t), "documents"), "documents");
  bytes = checkedAdd(bytes, checkedMultiply(terms, sizeof(Bm25Segment::TermEntry), "terms"), "terms");
  bytes = checkedAdd(bytes, checkedMultiply(postings, sizeof(Bm25Posting), "postings"), "postings");
  return checkedAdd(bytes, term_bytes, "term bytes");
}

}  // namespace

void Bm25SegmentBuilder::addDocument(std::string_view text) {
  if (document_lengths_.size() >= std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("BM25 segment document count exceeds uint32 range");
  }
  auto tokens = Bm25Analyzer::tokenize(text);
  if (tokens.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("BM25 document token count exceeds uint32 range");
  }
  const uint32_t document = static_cast<uint32_t>(document_lengths_.size());
  document_lengths_.push_back(static_cast<uint32_t>(tokens.size()));
  std::unordered_map<std::string, uint32_t> frequencies;
  for (auto& token : tokens) {
    auto [entry, inserted] = frequencies.try_emplace(std::move(token), 0);
    if (entry->second == std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error("BM25 term frequency exceeds uint32 range");
    }
    ++entry->second;
    if (inserted) {
      token_bytes_ = checkedAdd(token_bytes_, entry->first.size(), "tokens");
    }
  }
  for (auto& [term, frequency] : frequencies) {
    postings_[std::move(term)].push_back({document, frequency});
    posting_count_ = checkedAdd(posting_count_, 1, "posting count");
  }
}

void Bm25SegmentBuilder::write(const std::string& path) const {
  std::vector<std::pair<std::string_view, const std::vector<Bm25Posting>*>> terms;
  terms.reserve(postings_.size());
  uint64_t term_bytes = 0;
  for (const auto& [term, postings] : postings_) {
    terms.emplace_back(term, &postings);
    term_bytes = checkedAdd(term_bytes, term.size(), "term bytes");
  }
  std::sort(terms.begin(), terms.end(), [](const auto& left, const auto& right) {
    return left.first < right.first;
  });
  uint64_t total_length = 0;
  for (uint32_t length : document_lengths_) {
    total_length = checkedAdd(total_length, length, "document lengths");
  }
  const uint64_t resident = structuralBytes(
      document_lengths_.size(), terms.size(), posting_count_, term_bytes);
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("Cannot create BM25 segment: " + path);
  }
  for (uint64_t value : {kMagic, static_cast<uint64_t>(kAnalyzerId),
                         static_cast<uint64_t>(document_lengths_.size()),
                         static_cast<uint64_t>(terms.size()), posting_count_,
                         total_length, resident}) {
    writeU64(output, value);
  }
  for (uint32_t length : document_lengths_) {
    writeU32(output, length);
  }
  uint64_t offset = 0;
  for (const auto& [term, postings] : terms) {
    if (term.empty() || term.size() > kMaxTermBytes ||
        term.size() > std::numeric_limits<uint32_t>::max() ||
        postings->size() > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error("BM25 term metadata exceeds safe bounds");
    }
    writeU32(output, static_cast<uint32_t>(term.size()));
    output.write(term.data(), static_cast<std::streamsize>(term.size()));
    writeU64(output, offset);
    writeU32(output, static_cast<uint32_t>(postings->size()));
    offset = checkedAdd(offset, postings->size(), "posting offset");
  }
  for (const auto& [term, postings] : terms) {
    static_cast<void>(term);
    for (const auto& posting : *postings) {
      writeU32(output, posting.document);
      writeU32(output, posting.term_frequency);
    }
  }
  if (!output) {
    throw std::runtime_error("Cannot finish BM25 segment: " + path);
  }
}

void Bm25SegmentBuilder::writeStatistics(const std::string& path) const {
  uint64_t total_length = 0;
  for (uint32_t length : document_lengths_) {
    total_length = checkedAdd(total_length, length, "document lengths");
  }
  std::vector<std::pair<std::string, uint64_t>> frequencies;
  frequencies.reserve(postings_.size());
  for (const auto& [term, postings] : postings_) {
    frequencies.emplace_back(term, postings.size());
  }
  Bm25CorpusStats::writeShard(
      path, document_lengths_.size(), total_length, std::move(frequencies));
}

uint64_t Bm25SegmentBuilder::documentCount() const {
  return document_lengths_.size();
}

uint64_t Bm25SegmentBuilder::estimatedMemoryBytes() const {
  return structuralBytes(
      document_lengths_.size(), postings_.size(), posting_count_, token_bytes_);
}

Bm25Segment Bm25Segment::load(
    const std::string& path,
    uint64_t max_resident_bytes) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open BM25 segment: " + path);
  }
  const Header header = readHeader(input);
  if (header.resident_bytes > max_resident_bytes) {
    throw std::runtime_error("BM25 segment exceeds its resident memory reservation");
  }
  Bm25Segment result;
  result.total_document_length_ = header.total_document_length;
  result.resident_bytes_ = header.resident_bytes;
  result.document_lengths_.reserve(header.document_count);
  uint64_t computed_length = 0;
  for (uint64_t index = 0; index < header.document_count; ++index) {
    uint32_t length = readU32(input, "document length");
    result.document_lengths_.push_back(length);
    computed_length = checkedAdd(computed_length, length, "document length");
  }
  if (computed_length != header.total_document_length) {
    throw std::runtime_error("BM25 document statistics are inconsistent");
  }
  result.terms_.reserve(header.term_count);
  uint64_t term_bytes = 0;
  uint64_t expected_offset = 0;
  std::string previous;
  for (uint64_t index = 0; index < header.term_count; ++index) {
    const uint32_t length = readU32(input, "term length");
    if (length == 0 || length > kMaxTermBytes) {
      throw std::runtime_error("BM25 term length is outside safe bounds");
    }
    std::string term(length, '\0');
    input.read(term.data(), length);
    if (!input || (!previous.empty() && term <= previous)) {
      throw std::runtime_error("BM25 term dictionary is invalid or unsorted");
    }
    const uint64_t offset = readU64(input, "posting offset");
    const uint32_t count = readU32(input, "posting count");
    if (count == 0 || offset != expected_offset ||
        count > header.posting_count -
            std::min(header.posting_count, offset)) {
      throw std::runtime_error("BM25 posting range is invalid");
    }
    result.terms_.push_back({term, offset, count});
    previous = std::move(term);
    term_bytes = checkedAdd(term_bytes, length, "term bytes");
    expected_offset = checkedAdd(expected_offset, count, "posting offset");
  }
  if (expected_offset != header.posting_count ||
      structuralBytes(header.document_count, header.term_count,
                      header.posting_count, term_bytes) !=
          header.resident_bytes) {
    throw std::runtime_error("BM25 segment resident-size declaration is invalid");
  }
  result.postings_.reserve(header.posting_count);
  for (uint64_t index = 0; index < header.posting_count; ++index) {
    Bm25Posting posting{readU32(input, "posting document"),
                        readU32(input, "posting frequency")};
    if (posting.document >= header.document_count ||
        posting.term_frequency == 0) {
      throw std::runtime_error("BM25 posting value is invalid");
    }
    result.postings_.push_back(posting);
  }
  if (input.peek() != std::char_traits<char>::eof()) {
    throw std::runtime_error("BM25 segment contains trailing bytes");
  }
  return result;
}

uint64_t Bm25Segment::residentBytesFromDisk(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open BM25 segment: " + path);
  }
  return readHeader(input).resident_bytes;
}

std::vector<Bm25SearchResult> Bm25Segment::search(
    const std::vector<std::string>& query_tokens,
    uint64_t limit,
    double k1,
    double b,
    const Bm25CorpusStats& corpus_stats) const {
  if (limit == 0 || !std::isfinite(k1) || k1 <= 0 ||
      !std::isfinite(b) || b < 0 || b > 1) {
    throw std::invalid_argument("BM25 search parameters are outside safe bounds");
  }
  if (document_lengths_.empty() || query_tokens.empty()) {
    return {};
  }
  if (corpus_stats.documentCount() < document_lengths_.size()) {
    throw std::invalid_argument(
        "BM25 corpus statistics contain fewer documents than the segment");
  }
  const double average_length = corpus_stats.averageDocumentLength();
  std::unordered_map<uint32_t, double> scores;
  std::unordered_set<std::string> unique_terms(query_tokens.begin(), query_tokens.end());
  for (const auto& query : unique_terms) {
    auto term = std::lower_bound(
        terms_.begin(), terms_.end(), query,
        [](const TermEntry& entry, const std::string& value) {
          return entry.term < value;
        });
    if (term == terms_.end() || term->term != query) {
      continue;
    }
    const double document_frequency = corpus_stats.documentFrequency(query);
    if (document_frequency < term->posting_count ||
        document_frequency > corpus_stats.documentCount()) {
      throw std::runtime_error(
          "BM25 corpus statistics differ from the postings segment");
    }
    const double idf = std::log1p(
        (corpus_stats.documentCount() - document_frequency + 0.5) /
        (document_frequency + 0.5));
    for (uint64_t index = 0; index < term->posting_count; ++index) {
      const auto& posting = postings_.at(term->posting_offset + index);
      const double frequency = posting.term_frequency;
      const double length = document_lengths_.at(posting.document);
      const double denominator = frequency + k1 *
          (1.0 - b + b * length / std::max(1.0, average_length));
      scores[posting.document] += idf * frequency * (k1 + 1.0) / denominator;
    }
  }
  auto worse = [](const Bm25SearchResult& left, const Bm25SearchResult& right) {
    return left.score > right.score ||
        (left.score == right.score && left.document < right.document);
  };
  std::priority_queue<
      Bm25SearchResult, std::vector<Bm25SearchResult>, decltype(worse)> top(worse);
  for (const auto& [document, score] : scores) {
    Bm25SearchResult candidate{document, score};
    if (top.size() < limit) {
      top.push(candidate);
    } else if (score > top.top().score ||
               (score == top.top().score && document < top.top().document)) {
      top.pop();
      top.push(candidate);
    }
  }
  std::vector<Bm25SearchResult> result;
  result.reserve(top.size());
  while (!top.empty()) {
    result.push_back(top.top());
    top.pop();
  }
  std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
    return left.score > right.score ||
        (left.score == right.score && left.document < right.document);
  });
  return result;
}

uint64_t Bm25Segment::documentCount() const {
  return document_lengths_.size();
}

uint64_t Bm25Segment::residentBytes() const {
  return resident_bytes_;
}

}  // namespace pangrova::text::bm25
