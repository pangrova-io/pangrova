// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "artifact/ImmutableArtifactWriter.h"

#include <openssl/evp.h>

#include <array>
#include <algorithm>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string_view>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::artifact {
namespace {

constexpr size_t kIoChunkBytes = 8U * 1024U * 1024U;

class Sha256 final {
 public:
  Sha256() : context_(EVP_MD_CTX_new(), EVP_MD_CTX_free) {
    if (!context_ || EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1) {
      throw std::runtime_error("Failed to initialize artifact SHA-256");
    }
  }

  void update(const void* data, size_t size) {
    if (EVP_DigestUpdate(context_.get(), data, size) != 1) {
      throw std::runtime_error("Failed to update artifact SHA-256");
    }
  }

  std::string finish() {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(context_.get(), digest.data(), &size) != 1 ||
        size != 32) {
      throw std::runtime_error("Failed to finalize artifact SHA-256");
    }
    return {reinterpret_cast<const char*>(digest.data()), size};
  }

 private:
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context_;
};

std::string toHex(std::string_view bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (unsigned char value : bytes) {
    result.push_back(digits[value >> 4]);
    result.push_back(digits[value & 0x0f]);
  }
  return result;
}

ImmutableArtifactObject inspectLocal(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open local artifact output: " + path);
  }
  Sha256 hash;
  std::string buffer(kIoChunkBytes, '\0');
  uint64_t size = 0;
  while (input) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count > 0) {
      hash.update(buffer.data(), static_cast<size_t>(count));
      size += static_cast<uint64_t>(count);
    }
  }
  if (size == 0) {
    throw std::runtime_error("Local artifact output is empty: " + path);
  }
  return {"", hash.finish(), size};
}

ImmutableArtifactObject inspectRemote(
    const std::shared_ptr<facebook::velox::filesystems::FileSystem>& fs,
    const std::string& uri) {
  auto input = fs->openFileForRead(uri);
  const uint64_t size = input->size();
  if (size == 0) {
    throw std::runtime_error("Immutable artifact object is empty: " + uri);
  }
  Sha256 hash;
  std::string buffer(std::min<uint64_t>(kIoChunkBytes, size), '\0');
  for (uint64_t offset = 0; offset < size;) {
    const uint64_t count = std::min<uint64_t>(buffer.size(), size - offset);
    input->pread(offset, count, buffer.data());
    hash.update(buffer.data(), static_cast<size_t>(count));
    offset += count;
  }
  return {uri, hash.finish(), size};
}

void verifyExisting(
    const std::shared_ptr<facebook::velox::filesystems::FileSystem>& fs,
    const ImmutableArtifactObject& expected) {
  const auto actual = inspectRemote(fs, expected.uri);
  if (actual.size_bytes != expected.size_bytes ||
      actual.sha256 != expected.sha256) {
    throw std::runtime_error(
        "Content-addressed artifact collision or partial object: " +
        expected.uri);
  }
}

std::string appendPath(std::string base, const std::string& name) {
  if (base.empty()) {
    throw std::invalid_argument("Artifact staging URI must not be empty");
  }
  if (base.back() != '/') {
    base.push_back('/');
  }
  return base + name;
}

}  // namespace

ImmutableArtifactObject ImmutableArtifactWriter::putFile(
    const std::string& local_path,
    const std::string& staging_uri,
    const std::string& object_role) {
  if (object_role.empty()) {
    throw std::invalid_argument("Artifact object role must not be empty");
  }
  auto object = inspectLocal(local_path);
  object.uri = appendPath(
      staging_uri, object_role + "-" + toHex(object.sha256) + ".bin");

  auto fs = facebook::velox::filesystems::getFileSystem(object.uri, nullptr);
  if (!fs) {
    throw std::runtime_error(
        "No registered filesystem for artifact URI: " + object.uri);
  }
  if (fs->exists(object.uri)) {
    verifyExisting(fs, object);
    return object;
  }

  try {
    facebook::velox::filesystems::FileOptions options;
    options.shouldCreateParentDirectories = true;
    options.shouldThrowOnFileAlreadyExists = true;
    auto output = fs->openFileForWrite(object.uri, options);
    std::ifstream input(local_path, std::ios::binary);
    if (!input) {
      throw std::runtime_error("Cannot reopen local artifact output: " + local_path);
    }
    std::string buffer(kIoChunkBytes, '\0');
    while (input) {
      input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
      const auto count = input.gcount();
      if (count > 0) {
        output->append(std::string_view(buffer.data(), static_cast<size_t>(count)));
      }
    }
    output->flush();
    output->close();
    if (output->size() != object.size_bytes) {
      throw std::runtime_error("Artifact object size changed during write");
    }
  } catch (...) {
    // A concurrent retry may have won the create-if-absent race. Reuse only
    // when the complete object is byte-identical; otherwise preserve failure.
    if (fs->exists(object.uri)) {
      verifyExisting(fs, object);
      return object;
    }
    throw;
  }
  return object;
}

}  // namespace pangrova::artifact
