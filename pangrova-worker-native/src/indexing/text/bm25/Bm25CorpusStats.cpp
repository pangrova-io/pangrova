// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "indexing/text/bm25/Bm25CorpusStats.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pangrova::text::bm25 {
namespace {

constexpr uint64_t kMagic = 0x5047424D53544102ULL;
constexpr uint64_t kAnalyzerId = 1;
constexpr uint64_t kIndexStride = 256;
constexpr uint64_t kHeaderBytes = 8 * sizeof(uint64_t);
constexpr uint64_t kMaxTermBytes = 1ULL << 20;
constexpr size_t kMaxMergeFanIn = 64;

struct Header final {
  uint64_t document_count{0};
  uint64_t total_document_length{0};
  uint64_t term_count{0};
  uint64_t index_offset{0};
  uint64_t block_count{0};
  uint64_t index_stride{0};
};

struct IndexEntry final {
  std::string first_term;
  uint64_t file_offset;
};

struct MappedTerm final {
  std::string_view term;
  uint64_t frequency;
};

uint64_t checkedAdd(uint64_t left, uint64_t right, const char* context) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    throw std::runtime_error(std::string("BM25 statistics overflow: ") + context);
  }
  return left + right;
}

uint64_t expectedBlockCount(uint64_t term_count) {
  return term_count == 0 ? 0 : 1 + (term_count - 1) / kIndexStride;
}

void validateHeader(const Header& header) {
  if (header.document_count == 0 ||
      header.term_count > std::numeric_limits<uint32_t>::max() ||
      header.index_stride != kIndexStride ||
      header.block_count != expectedBlockCount(header.term_count) ||
      header.index_offset < kHeaderBytes) {
    throw std::runtime_error("BM25 statistics header is outside safe bounds");
  }
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
      throw std::runtime_error(std::string("Truncated BM25 statistics: ") + context);
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
      throw std::runtime_error(std::string("Truncated BM25 statistics: ") + context);
    }
    value |= static_cast<uint64_t>(static_cast<unsigned char>(byte)) << shift;
  }
  return value;
}

void writeHeader(
    std::ostream& output,
    uint64_t document_count,
    uint64_t total_document_length,
    uint64_t term_count,
    uint64_t index_offset,
    uint64_t block_count) {
  for (uint64_t value : {kMagic, kAnalyzerId, document_count,
                         total_document_length, term_count, index_offset,
                         block_count, kIndexStride}) {
    writeU64(output, value);
  }
}

Header readHeader(std::istream& input) {
  if (readU64(input, "magic") != kMagic ||
      readU64(input, "analyzer") != kAnalyzerId) {
    throw std::runtime_error("BM25 statistics format identity is invalid");
  }
  Header header{
      readU64(input, "document count"),
      readU64(input, "total document length"),
      readU64(input, "term count"),
      readU64(input, "index offset"),
      readU64(input, "block count"),
      readU64(input, "index stride")};
  validateHeader(header);
  return header;
}

uint64_t streamOffset(std::istream& input, const char* context) {
  const std::streampos position = input.tellg();
  if (position < 0) {
    throw std::runtime_error(std::string("Cannot locate BM25 statistics: ") + context);
  }
  return static_cast<uint64_t>(position);
}

uint64_t streamOffset(std::ostream& output, const char* context) {
  const std::streampos position = output.tellp();
  if (position < 0) {
    throw std::runtime_error(std::string("Cannot locate BM25 statistics: ") + context);
  }
  return static_cast<uint64_t>(position);
}

std::pair<std::string, uint64_t> readTerm(
    std::istream& input,
    const std::string& previous,
    uint64_t document_count) {
  const uint32_t length = readU32(input, "term length");
  if (length == 0 || length > kMaxTermBytes) {
    throw std::runtime_error("BM25 statistics term length is outside safe bounds");
  }
  std::string term(length, '\0');
  input.read(term.data(), static_cast<std::streamsize>(length));
  const uint64_t frequency = readU64(input, "document frequency");
  if (!input || (!previous.empty() && term <= previous) ||
      frequency == 0 || frequency > document_count) {
    throw std::runtime_error("BM25 statistics term entry is invalid");
  }
  return {std::move(term), frequency};
}

void writeTerm(
    std::ostream& output,
    const std::string& term,
    uint64_t frequency) {
  if (term.empty() || term.size() > kMaxTermBytes ||
      term.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("BM25 statistics term exceeds safe bounds");
  }
  writeU32(output, static_cast<uint32_t>(term.size()));
  output.write(term.data(), static_cast<std::streamsize>(term.size()));
  writeU64(output, frequency);
}

void writeIndex(std::ostream& output, const std::vector<IndexEntry>& entries) {
  for (const auto& entry : entries) {
    writeU32(output, static_cast<uint32_t>(entry.first_term.size()));
    output.write(
        entry.first_term.data(),
        static_cast<std::streamsize>(entry.first_term.size()));
    writeU64(output, entry.file_offset);
  }
}

void finalizeFile(
    std::ostream& output,
    uint64_t document_count,
    uint64_t total_document_length,
    uint64_t term_count,
    const std::vector<IndexEntry>& index) {
  const uint64_t index_offset = streamOffset(output, "index offset");
  writeIndex(output, index);
  output.seekp(0, std::ios::beg);
  writeHeader(
      output, document_count, total_document_length, term_count,
      index_offset, index.size());
  output.flush();
  if (!output) {
    throw std::runtime_error("Cannot finish indexed BM25 statistics");
  }
}

class ShardReader final {
 public:
  explicit ShardReader(const std::string& path)
      : input_(path, std::ios::binary) {
    if (!input_) {
      throw std::runtime_error("Cannot open BM25 statistics shard: " + path);
    }
    header_ = readHeader(input_);
    blocks_.reserve(header_.block_count);
    advance();
  }

  const Header& header() const { return header_; }
  bool hasValue() const { return current_.has_value(); }
  const std::pair<std::string, uint64_t>& value() const {
    return current_.value();
  }

  void advance() {
    if (read_count_ == header_.term_count) {
      current_.reset();
      validateSerializedIndex();
      return;
    }
    const uint64_t offset = streamOffset(input_, "term entry");
    current_ = readTerm(input_, previous_, header_.document_count);
    if (read_count_ % kIndexStride == 0) {
      blocks_.push_back(IndexEntry{current_->first, offset});
    }
    previous_ = current_->first;
    ++read_count_;
  }

 private:
  void validateSerializedIndex() {
    if (validated_) {
      return;
    }
    if (streamOffset(input_, "term section") != header_.index_offset ||
        blocks_.size() != header_.block_count) {
      throw std::runtime_error("BM25 statistics sparse index offset is invalid");
    }
    for (const auto& expected : blocks_) {
      const uint32_t length = readU32(input_, "index term length");
      if (length == 0 || length > kMaxTermBytes) {
        throw std::runtime_error("BM25 statistics sparse index term is invalid");
      }
      std::string term(length, '\0');
      input_.read(term.data(), static_cast<std::streamsize>(length));
      const uint64_t offset = readU64(input_, "index file offset");
      if (!input_ || term != expected.first_term || offset != expected.file_offset) {
        throw std::runtime_error("BM25 statistics sparse index is inconsistent");
      }
    }
    if (input_.peek() != std::char_traits<char>::eof()) {
      throw std::runtime_error("BM25 statistics shard contains trailing bytes");
    }
    validated_ = true;
  }

  std::ifstream input_;
  Header header_;
  uint64_t read_count_{0};
  std::string previous_;
  std::optional<std::pair<std::string, uint64_t>> current_;
  std::vector<IndexEntry> blocks_;
  bool validated_{false};
};

uint64_t readMappedInteger(
    const uint8_t* data,
    size_t size,
    uint64_t& offset,
    size_t width,
    const char* context) {
  if (width > sizeof(uint64_t) || offset > size || width > size - offset) {
    throw std::runtime_error(std::string("Truncated BM25 statistics: ") + context);
  }
  uint64_t value = 0;
  for (size_t index = 0; index < width; ++index) {
    value |= static_cast<uint64_t>(data[offset + index]) << (index * 8);
  }
  offset += width;
  return value;
}

Header readMappedHeader(const uint8_t* data, size_t size) {
  uint64_t offset = 0;
  if (readMappedInteger(data, size, offset, 8, "magic") != kMagic ||
      readMappedInteger(data, size, offset, 8, "analyzer") != kAnalyzerId) {
    throw std::runtime_error("BM25 statistics format identity is invalid");
  }
  Header header{
      readMappedInteger(data, size, offset, 8, "document count"),
      readMappedInteger(data, size, offset, 8, "total document length"),
      readMappedInteger(data, size, offset, 8, "term count"),
      readMappedInteger(data, size, offset, 8, "index offset"),
      readMappedInteger(data, size, offset, 8, "block count"),
      readMappedInteger(data, size, offset, 8, "index stride")};
  validateHeader(header);
  if (header.index_offset > size) {
    throw std::runtime_error("BM25 statistics index lies outside its file");
  }
  return header;
}

MappedTerm readMappedTerm(
    const uint8_t* data,
    size_t limit,
    uint64_t& offset,
    uint64_t document_count) {
  const uint64_t length = readMappedInteger(
      data, limit, offset, 4, "term length");
  if (length == 0 || length > kMaxTermBytes ||
      offset > limit || length > limit - offset) {
    throw std::runtime_error("BM25 statistics term length is outside safe bounds");
  }
  const char* term_data = reinterpret_cast<const char*>(data + offset);
  std::string_view term(term_data, static_cast<size_t>(length));
  offset += length;
  const uint64_t frequency = readMappedInteger(
      data, limit, offset, 8, "document frequency");
  if (frequency == 0 || frequency > document_count) {
    throw std::runtime_error("BM25 statistics document frequency is invalid");
  }
  return MappedTerm{term, frequency};
}

std::runtime_error systemFailure(const std::string& action) {
  return std::runtime_error(action + ": " + std::strerror(errno));
}

}  // namespace

void Bm25CorpusStats::writeShard(
    const std::string& path,
    uint64_t document_count,
    uint64_t total_document_length,
    std::vector<std::pair<std::string, uint64_t>> document_frequencies) {
  if (document_count == 0 ||
      document_frequencies.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::invalid_argument("BM25 statistics shard is outside safe bounds");
  }
  std::sort(document_frequencies.begin(), document_frequencies.end());
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("Cannot create BM25 statistics shard: " + path);
  }
  writeHeader(output, document_count, total_document_length,
              document_frequencies.size(), 0, 0);
  std::vector<IndexEntry> index;
  index.reserve(expectedBlockCount(document_frequencies.size()));
  std::string previous;
  uint64_t ordinal = 0;
  for (const auto& [term, frequency] : document_frequencies) {
    if ((!previous.empty() && term == previous) || frequency == 0 ||
        frequency > document_count) {
      throw std::invalid_argument("BM25 statistics shard contains an invalid term");
    }
    if (ordinal % kIndexStride == 0) {
      index.push_back(IndexEntry{term, streamOffset(output, "term entry")});
    }
    writeTerm(output, term, frequency);
    previous = term;
    ++ordinal;
  }
  finalizeFile(
      output, document_count, total_document_length,
      document_frequencies.size(), index);
}

void Bm25CorpusStats::mergeSortedShards(
    const std::vector<std::string>& input_paths,
    const std::string& output_path) {
  if (input_paths.empty() || input_paths.size() > kMaxMergeFanIn) {
    throw std::invalid_argument("BM25 statistics merge fan-in must be between 1 and 64");
  }
  std::vector<std::unique_ptr<ShardReader>> readers;
  readers.reserve(input_paths.size());
  uint64_t document_count = 0;
  uint64_t total_document_length = 0;
  for (const auto& path : input_paths) {
    auto reader = std::make_unique<ShardReader>(path);
    document_count = checkedAdd(
        document_count, reader->header().document_count, "document count");
    total_document_length = checkedAdd(
        total_document_length, reader->header().total_document_length,
        "document length");
    readers.push_back(std::move(reader));
  }

  using QueueEntry = std::pair<std::string, size_t>;
  std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<>> queue;
  for (size_t index = 0; index < readers.size(); ++index) {
    if (readers[index]->hasValue()) {
      queue.emplace(readers[index]->value().first, index);
    }
  }
  std::ofstream output(output_path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("Cannot create merged BM25 statistics: " + output_path);
  }
  writeHeader(output, document_count, total_document_length, 0, 0, 0);
  std::vector<IndexEntry> index;
  uint64_t term_count = 0;
  while (!queue.empty()) {
    const std::string term = queue.top().first;
    uint64_t frequency = 0;
    while (!queue.empty() && queue.top().first == term) {
      const size_t reader_index = queue.top().second;
      queue.pop();
      frequency = checkedAdd(
          frequency, readers[reader_index]->value().second,
          "document frequency");
      readers[reader_index]->advance();
      if (readers[reader_index]->hasValue()) {
        queue.emplace(readers[reader_index]->value().first, reader_index);
      }
    }
    if (frequency > document_count) {
      throw std::runtime_error("Merged BM25 document frequency exceeds corpus size");
    }
    if (term_count % kIndexStride == 0) {
      index.push_back(IndexEntry{term, streamOffset(output, "term entry")});
    }
    writeTerm(output, term, frequency);
    term_count = checkedAdd(term_count, 1, "term count");
  }
  finalizeFile(
      output, document_count, total_document_length, term_count, index);
}

Bm25CorpusStats Bm25CorpusStats::load(
    const std::string& path,
    uint64_t max_resident_bytes) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (descriptor < 0) {
    throw systemFailure("Cannot open BM25 corpus statistics");
  }
  struct stat status {};
  if (::fstat(descriptor, &status) != 0 || status.st_size <= 0 ||
      static_cast<uint64_t>(status.st_size) >
          std::numeric_limits<size_t>::max()) {
    const int saved_errno = errno;
    ::close(descriptor);
    errno = saved_errno;
    throw systemFailure("Cannot size BM25 corpus statistics");
  }
  const size_t size = static_cast<size_t>(status.st_size);
  void* mapping = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, descriptor, 0);
  if (mapping == MAP_FAILED) {
    const int saved_errno = errno;
    ::close(descriptor);
    errno = saved_errno;
    throw systemFailure("Cannot map BM25 corpus statistics");
  }

  Bm25CorpusStats result;
  result.file_descriptor_ = descriptor;
  result.mapped_data_ = static_cast<const uint8_t*>(mapping);
  result.mapped_size_ = size;
  try {
    const Header header = readMappedHeader(result.mapped_data_, size);
    if (header.block_count >
        std::numeric_limits<uint64_t>::max() / sizeof(BlockEntry)) {
      throw std::runtime_error("BM25 sparse index size overflows");
    }
    uint64_t resident = checkedAdd(
        sizeof(Bm25CorpusStats),
        header.block_count * sizeof(BlockEntry), "sparse index entries");
    if (resident > max_resident_bytes ||
        header.block_count > std::numeric_limits<size_t>::max()) {
      throw std::runtime_error("BM25 corpus statistics exceed memory reservation");
    }
    result.blocks_.reserve(static_cast<size_t>(header.block_count));
    uint64_t offset = kHeaderBytes;
    std::string_view previous;
    for (uint64_t ordinal = 0; ordinal < header.term_count; ++ordinal) {
      const uint64_t term_offset = offset;
      MappedTerm entry = readMappedTerm(
          result.mapped_data_, header.index_offset, offset,
          header.document_count);
      if (!previous.empty() && entry.term <= previous) {
        throw std::runtime_error("BM25 statistics terms are not strictly ordered");
      }
      if (ordinal % kIndexStride == 0) {
        resident = checkedAdd(resident, entry.term.size(), "sparse index term");
        if (resident > max_resident_bytes) {
          throw std::runtime_error(
              "BM25 corpus statistics exceed memory reservation");
        }
        result.blocks_.push_back(BlockEntry{
            std::string(entry.term), term_offset, ordinal});
      }
      previous = entry.term;
    }
    if (offset != header.index_offset ||
        result.blocks_.size() != header.block_count) {
      throw std::runtime_error("BM25 statistics term section is inconsistent");
    }
    for (const auto& expected : result.blocks_) {
      const uint64_t length = readMappedInteger(
          result.mapped_data_, size, offset, 4, "index term length");
      if (length == 0 || length > kMaxTermBytes ||
          offset > size || length > size - offset) {
        throw std::runtime_error("BM25 statistics sparse index term is invalid");
      }
      std::string_view term(
          reinterpret_cast<const char*>(result.mapped_data_ + offset),
          static_cast<size_t>(length));
      offset += length;
      const uint64_t file_offset = readMappedInteger(
          result.mapped_data_, size, offset, 8, "index file offset");
      if (term != expected.first_term ||
          file_offset != expected.file_offset) {
        throw std::runtime_error("BM25 statistics sparse index is inconsistent");
      }
    }
    if (offset != size) {
      throw std::runtime_error("BM25 corpus statistics contain trailing bytes");
    }
    result.document_count_ = header.document_count;
    result.total_document_length_ = header.total_document_length;
    result.term_count_ = header.term_count;
    result.resident_bytes_ = resident;
    ::madvise(mapping, size, MADV_RANDOM);
    return result;
  } catch (...) {
    result.release();
    throw;
  }
}

uint64_t Bm25CorpusStats::documentCountFromDisk(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open BM25 corpus statistics: " + path);
  }
  return readHeader(input).document_count;
}

uint64_t Bm25CorpusStats::residentBytesFromDisk(const std::string& path) {
  return Bm25CorpusStats::load(
      path, std::numeric_limits<uint64_t>::max()).residentBytes();
}

Bm25CorpusStats::~Bm25CorpusStats() {
  release();
}

Bm25CorpusStats::Bm25CorpusStats(Bm25CorpusStats&& other) noexcept {
  *this = std::move(other);
}

Bm25CorpusStats& Bm25CorpusStats::operator=(Bm25CorpusStats&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  release();
  document_count_ = other.document_count_;
  total_document_length_ = other.total_document_length_;
  term_count_ = other.term_count_;
  resident_bytes_ = other.resident_bytes_;
  file_descriptor_ = other.file_descriptor_;
  mapped_data_ = other.mapped_data_;
  mapped_size_ = other.mapped_size_;
  blocks_ = std::move(other.blocks_);
  other.document_count_ = 0;
  other.total_document_length_ = 0;
  other.term_count_ = 0;
  other.resident_bytes_ = 0;
  other.file_descriptor_ = -1;
  other.mapped_data_ = nullptr;
  other.mapped_size_ = 0;
  return *this;
}

void Bm25CorpusStats::release() noexcept {
  if (mapped_data_ != nullptr && mapped_size_ > 0) {
    ::munmap(const_cast<uint8_t*>(mapped_data_), mapped_size_);
  }
  if (file_descriptor_ >= 0) {
    ::close(file_descriptor_);
  }
  mapped_data_ = nullptr;
  mapped_size_ = 0;
  file_descriptor_ = -1;
}

uint64_t Bm25CorpusStats::documentCount() const {
  return document_count_;
}

uint64_t Bm25CorpusStats::totalDocumentLength() const {
  return total_document_length_;
}

double Bm25CorpusStats::averageDocumentLength() const {
  return static_cast<double>(total_document_length_) /
      static_cast<double>(document_count_);
}

uint64_t Bm25CorpusStats::documentFrequency(std::string_view term) const {
  if (term.empty() || blocks_.empty() || mapped_data_ == nullptr) {
    return 0;
  }
  auto upper = std::upper_bound(
      blocks_.begin(), blocks_.end(), term,
      [](std::string_view value, const BlockEntry& block) {
        return value < block.first_term;
      });
  if (upper == blocks_.begin()) {
    return 0;
  }
  --upper;
  uint64_t offset = upper->file_offset;
  const uint64_t terms_in_block = std::min(
      kIndexStride, term_count_ - upper->first_term_ordinal);
  for (uint64_t ordinal = 0; ordinal < terms_in_block; ++ordinal) {
    MappedTerm entry = readMappedTerm(
        mapped_data_, mapped_size_, offset, document_count_);
    if (entry.term == term) {
      return entry.frequency;
    }
    if (entry.term > term) {
      return 0;
    }
  }
  return 0;
}

uint64_t Bm25CorpusStats::residentBytes() const {
  return resident_bytes_;
}

}  // namespace pangrova::text::bm25
