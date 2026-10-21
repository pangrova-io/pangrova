#include "velox/connector/AvroContainerReader.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

#include "folly/json.h"
#include "openssl/crypto.h"
#include "openssl/evp.h"
#include "velox/common/file/File.h"
#include "zlib.h"
#include "zstd.h"

namespace pangrova::velox {
namespace {

constexpr uint64_t kHashChunkBytes = 64 * 1024;
constexpr uint64_t kMaxHeaderBytes = 1024 * 1024;
constexpr uint64_t kMaxBlockBytes = 64 * 1024 * 1024;
constexpr uint64_t kMaxBlockRecords = 1'000'000;
constexpr uint64_t kMaxNestedItems = 1'000'000;
constexpr int kMaxSchemaDepth = 32;

std::string text(const folly::dynamic& value) {
  if (!value.isString()) {
    throw std::runtime_error("Invalid Avro schema text");
  }
  const auto& string = value.asString();
  return std::string(string.data(), string.size());
}

class BinaryDecoder final {
 public:
  explicit BinaryDecoder(std::string_view bytes, folly::CancellationToken cancellation)
      : cancellation_(std::move(cancellation)), bytes_(bytes) {}

  uint64_t position() const { return position_; }
  uint64_t size() const { return bytes_.size(); }

  std::string readExact(uint64_t count) {
    if (count > bytes_.size() - position_) {
      throw std::runtime_error("Truncated Avro record");
    }
    std::string result(bytes_.substr(position_, count));
    position_ += count;
    return result;
  }

  uint8_t readByte() {
    if (position_ == bytes_.size()) {
      throw std::runtime_error("Truncated Avro record");
    }
    return static_cast<uint8_t>(bytes_[position_++]);
  }

  int64_t readLong() {
    uint64_t encoded = 0;
    for (int index = 0; index < 10; ++index) {
      const auto byte = readByte();
      if (index == 9 && byte > 1) {
        throw std::runtime_error("Avro varint exceeds 64 bits");
      }
      encoded |= static_cast<uint64_t>(byte & 0x7f) << (7 * index);
      if ((byte & 0x80) == 0) {
        return static_cast<int64_t>((encoded >> 1) ^
            (uint64_t{0} - (encoded & 1)));
      }
    }
    throw std::runtime_error("Unterminated Avro varint");
  }

  std::string readBytes() {
    const auto length = readLong();
    if (length < 0 || static_cast<uint64_t>(length) > kMaxBlockBytes) {
      throw std::runtime_error("Invalid Avro byte-string length");
    }
    return readExact(length);
  }

  folly::dynamic decode(const folly::dynamic& schema, int depth = 0) {
    if (cancellation_.isCancellationRequested()) throw std::runtime_error("Metadata decode cancelled");
    if (depth > kMaxSchemaDepth) {
      throw std::runtime_error("Avro schema nesting limit exceeded");
    }
    if (schema.isArray()) {
      const auto branch = readLong();
      if (branch < 0 || static_cast<size_t>(branch) >= schema.size()) {
        throw std::runtime_error("Invalid Avro union branch");
      }
      return decode(schema[branch], depth + 1);
    }
    const auto kind = schema.isString() ? text(schema) : text(schema["type"]);
    if (kind == "null") return nullptr;
    if (kind == "boolean") {
      const auto value = readByte();
      if (value > 1) throw std::runtime_error("Invalid Avro boolean");
      return value == 1;
    }
    if (kind == "int") {
      const auto value = readLong();
      if (value < std::numeric_limits<int32_t>::min() ||
          value > std::numeric_limits<int32_t>::max()) {
        throw std::runtime_error("Avro int exceeds 32 bits");
      }
      return value;
    }
    if (kind == "long") return readLong();
    if (kind == "string" || kind == "bytes") return readBytes();
    if (kind == "float") {
      const auto bytes = readExact(sizeof(uint32_t));
      uint32_t bits = 0;
      for (size_t index = 0; index < bytes.size(); ++index) {
        bits |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[index])) << (8 * index);
      }
      return static_cast<double>(std::bit_cast<float>(bits));
    }
    if (kind == "double") {
      const auto bytes = readExact(sizeof(uint64_t));
      uint64_t bits = 0;
      for (size_t index = 0; index < bytes.size(); ++index) {
        bits |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[index])) << (8 * index);
      }
      return std::bit_cast<double>(bits);
    }
    if (kind == "fixed") {
      const auto length = schema["size"].asInt();
      if (length < 0 || static_cast<uint64_t>(length) > kMaxBlockBytes) {
        throw std::runtime_error("Invalid Avro fixed size");
      }
      return readExact(length);
    }
    if (kind == "enum") {
      const auto index = readLong();
      const auto& symbols = schema["symbols"];
      if (index < 0 || static_cast<size_t>(index) >= symbols.size()) {
        throw std::runtime_error("Invalid Avro enum symbol");
      }
      return text(symbols[index]);
    }
    if (kind == "record") {
      folly::dynamic record = folly::dynamic::object;
      for (const auto& field : schema["fields"]) {
        record[text(field["name"])] = decode(field["type"], depth + 1);
      }
      return record;
    }
    if (kind == "array" || kind == "map") {
      folly::dynamic result = kind == "array"
          ? folly::dynamic(folly::dynamic::array)
          : folly::dynamic(folly::dynamic::object);
      uint64_t total = 0;
      while (true) {
        auto count = readLong();
        if (count == 0) break;
        if (count == std::numeric_limits<int64_t>::min()) {
          throw std::runtime_error("Invalid Avro collection count");
        }
        std::optional<uint64_t> blockSize;
        if (count < 0) {
          count = -count;
          const auto encodedSize = readLong();
          if (encodedSize < 0) throw std::runtime_error("Invalid Avro collection block");
          blockSize = encodedSize;
        }
        if (static_cast<uint64_t>(count) > kMaxNestedItems - total) {
          throw std::runtime_error("Avro collection exceeds bounded read limit");
        }
        total += static_cast<uint64_t>(count);
        const auto before = position_;
        for (int64_t index = 0; index < count; ++index) {
          if (kind == "array") {
            result.push_back(decode(schema["items"], depth + 1));
          } else {
            auto key = readBytes();
            result[std::move(key)] = decode(schema["values"], depth + 1);
          }
        }
        if (blockSize.has_value() && position_ - before != *blockSize) {
          throw std::runtime_error("Avro collection block size changed");
        }
      }
      return result;
    }
    throw std::runtime_error("Unsupported Avro writer schema type: " + kind);
  }

 private:
  folly::CancellationToken cancellation_;
  std::string_view bytes_;
  uint64_t position_ = 0;
};

std::string inflateDeflate(std::string_view compressed) {
  z_stream stream{};
  if (inflateInit2(&stream, -MAX_WBITS) != Z_OK) {
    throw std::runtime_error("Cannot initialize Avro deflate decoder");
  }
  struct Guard {
    z_stream* stream;
    ~Guard() { inflateEnd(stream); }
  } guard{&stream};
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed.data()));
  stream.avail_in = static_cast<uInt>(compressed.size());
  std::string output;
  std::array<char, kHashChunkBytes> chunk{};
  int status = Z_OK;
  while (status != Z_STREAM_END) {
    stream.next_out = reinterpret_cast<Bytef*>(chunk.data());
    stream.avail_out = static_cast<uInt>(chunk.size());
    status = inflate(&stream, Z_NO_FLUSH);
    if (status != Z_OK && status != Z_STREAM_END) {
      throw std::runtime_error("Invalid Avro deflate block");
    }
    const auto produced = chunk.size() - stream.avail_out;
    if (produced > kMaxBlockBytes - output.size()) {
      throw std::runtime_error("Avro block expands beyond bounded read limit");
    }
    output.append(chunk.data(), produced);
  }
  if (stream.avail_in != 0) {
    throw std::runtime_error("Avro deflate block contains trailing bytes");
  }
  return output;
}

std::string inflateZstandard(const std::string& compressed) {
  std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context(ZSTD_createDCtx(), ZSTD_freeDCtx);
  if (!context || ZSTD_isError(ZSTD_DCtx_setParameter(context.get(), ZSTD_d_windowLogMax, 26))) {
    throw std::runtime_error("Cannot initialize bounded Avro Zstandard decoder");
  }
  ZSTD_inBuffer input{compressed.data(), compressed.size(), 0};
  std::array<char, kHashChunkBytes> chunk{};
  std::string output;
  size_t remaining = 1;
  while (input.pos < input.size || remaining != 0) {
    const auto before = input.pos;
    ZSTD_outBuffer decoded{chunk.data(), chunk.size(), 0};
    remaining = ZSTD_decompressStream(context.get(), &decoded, &input);
    if (ZSTD_isError(remaining)) throw std::runtime_error("Invalid Avro Zstandard block");
    if (decoded.pos > kMaxBlockBytes - output.size()) {
      throw std::runtime_error("Avro block expands beyond bounded read limit");
    }
    output.append(chunk.data(), decoded.pos);
    if (remaining != 0 && input.pos == before && decoded.pos == 0) {
      throw std::runtime_error("Truncated Avro Zstandard block");
    }
  }
  return output;
}

}  // namespace

AvroContainerReader::AvroContainerReader(
    std::shared_ptr<facebook::velox::ReadFile> file,
    uint64_t expectedLength, std::string expectedSha256, folly::CancellationToken cancellation)
    : cancellation_(std::move(cancellation)), file_(std::move(file)) {
  if (!file_ || expectedLength == 0 || file_->size() != expectedLength ||
      expectedSha256.size() != 32) {
    throw std::invalid_argument("Invalid pinned Avro object identity");
  }
  file_size_ = expectedLength;
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
  if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1) {
    throw std::runtime_error("Cannot initialize Avro metadata digest");
  }
  std::array<char, kHashChunkBytes> chunk{};
  for (uint64_t offset = 0; offset < file_size_;) {
    const auto length = std::min<uint64_t>(chunk.size(), file_size_ - offset);
    file_->pread(offset, length, chunk.data());
    if (EVP_DigestUpdate(digest.get(), chunk.data(), length) != 1) {
      throw std::runtime_error("Cannot hash Avro metadata object");
    }
    offset += length;
  }
  std::array<unsigned char, 32> actual{};
  unsigned int digestSize = 0;
  if (EVP_DigestFinal_ex(digest.get(), actual.data(), &digestSize) != 1 ||
      digestSize != actual.size() ||
      CRYPTO_memcmp(actual.data(), expectedSha256.data(), actual.size()) != 0) {
    throw std::runtime_error("Pinned Avro metadata SHA-256 changed");
  }
  parseHeader();
}

std::string AvroContainerReader::readExact(uint64_t size) {
  if (size > file_size_ - file_position_) {
    throw std::runtime_error("Truncated Iceberg Avro container");
  }
  std::string bytes(size, '\0');
  if (size > 0) {
    file_->pread(file_position_, size, bytes.data());
  }
  file_position_ += size;
  return bytes;
}

int64_t AvroContainerReader::readFileLong() {
  uint64_t encoded = 0;
  for (int index = 0; index < 10; ++index) {
    const auto byte = static_cast<uint8_t>(readExact(1)[0]);
    if (index == 9 && byte > 1) {
      throw std::runtime_error("Avro container varint exceeds 64 bits");
    }
    encoded |= static_cast<uint64_t>(byte & 0x7f) << (7 * index);
    if ((byte & 0x80) == 0) {
      return static_cast<int64_t>((encoded >> 1) ^
          (uint64_t{0} - (encoded & 1)));
    }
  }
  throw std::runtime_error("Unterminated Avro container varint");
}

std::string AvroContainerReader::readFileBytes() {
  const auto length = readFileLong();
  if (length < 0 || static_cast<uint64_t>(length) > kMaxHeaderBytes) {
    throw std::runtime_error("Invalid Avro container header field");
  }
  return readExact(length);
}

void AvroContainerReader::parseHeader() {
  if (readExact(4) != std::string("Obj\x01", 4)) {
    throw std::runtime_error("Avro metadata object is not Avro OCF");
  }
  uint64_t totalEntries = 0;
  while (true) {
    auto count = readFileLong();
    if (count == 0) break;
    if (count == std::numeric_limits<int64_t>::min()) {
      throw std::runtime_error("Invalid Avro header map count");
    }
    std::optional<uint64_t> blockSize;
    if (count < 0) {
      count = -count;
      const auto encodedBlockSize = readFileLong();
      if (encodedBlockSize < 0 || static_cast<uint64_t>(encodedBlockSize) > kMaxHeaderBytes) {
        throw std::runtime_error("Invalid Avro header map block");
      }
      blockSize = encodedBlockSize;
    }
    if (count < 0 || static_cast<uint64_t>(count) > 1000 - totalEntries ||
        file_position_ > kMaxHeaderBytes) {
      throw std::runtime_error("Avro header map exceeds bound");
    }
    totalEntries += static_cast<uint64_t>(count);
    const auto blockStart = file_position_;
    for (int64_t index = 0; index < count; ++index) {
      auto key = readFileBytes();
      auto value = readFileBytes();
      if (file_position_ > kMaxHeaderBytes) {
        throw std::runtime_error("Avro header map exceeds bound");
      }
      if (!metadata_.emplace(std::move(key), std::move(value)).second) {
        throw std::runtime_error("Duplicate Avro header metadata key");
      }
    }
    if (blockSize.has_value() && file_position_ - blockStart != *blockSize) {
      throw std::runtime_error("Avro header map block size changed");
    }
  }
  sync_marker_ = readExact(16);
  auto schema = metadata_.find("avro.schema");
  if (schema == metadata_.end()) {
    throw std::runtime_error("Avro container lacks writer schema");
  }
  writer_schema_ = folly::parseJson(schema->second);
  if (!writer_schema_.isObject() || text(writer_schema_["type"]) != "record") {
    throw std::runtime_error("Avro metadata writer schema is not a record");
  }
  auto codec = metadata_.find("avro.codec");
  codec_ = codec == metadata_.end() ? "null" : codec->second;
  if (codec_ != "null" && codec_ != "deflate" && codec_ != "zstandard") {
    throw std::runtime_error("Unsupported Avro metadata Avro codec: " + codec_);
  }
}

std::string AvroContainerReader::metadataValue(const std::string& key) const {
  auto entry = metadata_.find(key);
  if (entry == metadata_.end()) {
    throw std::runtime_error("Iceberg Avro container lacks metadata: " + key);
  }
  return entry->second;
}

bool AvroContainerReader::readNextBlock() {
  if (file_position_ == file_size_) return false;
  const auto count = readFileLong();
  const auto encodedSize = readFileLong();
  if (count <= 0 || static_cast<uint64_t>(count) > kMaxBlockRecords ||
      encodedSize < 0 || static_cast<uint64_t>(encodedSize) > kMaxBlockBytes) {
    throw std::runtime_error("Invalid Iceberg Avro block dimensions");
  }
  auto compressed = readExact(encodedSize);
  if (readExact(sync_marker_.size()) != sync_marker_) {
    throw std::runtime_error("Iceberg Avro block sync marker changed");
  }
  if (codec_ == "deflate") block_ = inflateDeflate(compressed);
  else if (codec_ == "zstandard") block_ = inflateZstandard(compressed);
  else block_ = std::move(compressed);
  block_position_ = 0;
  remaining_rows_ = count;
  return true;
}

std::optional<folly::dynamic> AvroContainerReader::nextRecord() {
  if (cancellation_.isCancellationRequested()) throw std::runtime_error("Metadata decode cancelled");
  if (remaining_rows_ == 0 && !readNextBlock()) return std::nullopt;
  BinaryDecoder decoder(std::string_view(block_).substr(block_position_), cancellation_);
  auto row = decoder.decode(writer_schema_);
  last_record_size_ = decoder.position();
  block_position_ += last_record_size_;
  --remaining_rows_;
  if (remaining_rows_ == 0 && block_position_ != block_.size()) {
    throw std::runtime_error("Iceberg Avro block contains unconsumed bytes");
  }
  return row;
}

}  // namespace pangrova::velox
