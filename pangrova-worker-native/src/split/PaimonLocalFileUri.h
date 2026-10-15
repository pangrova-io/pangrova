#pragma once

#include <stdexcept>
#include <string>

namespace pangrova::split {

/** Decode one canonical local file URI without accepting an authority or path separators in escapes. */
inline std::string decodePaimonLocalFileUri(const std::string& uri) {
  if (!uri.starts_with("file:/") ||
      (uri.starts_with("file://") && !uri.starts_with("file:///"))) {
    throw std::invalid_argument("Paimon branch directory requires a local file URI");
  }
  auto encoded = uri.substr(5);
  if (encoded.empty() || encoded[0] != '/' ||
      encoded.find('?') != std::string::npos ||
      encoded.find('#') != std::string::npos) {
    throw std::invalid_argument("Invalid local Paimon branch directory path");
  }
  auto hex = [](char digit) -> int {
    if (digit >= '0' && digit <= '9') return digit - '0';
    if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
    if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
    return -1;
  };
  std::string path;
  path.reserve(encoded.size());
  for (size_t i = 0; i < encoded.size(); ++i) {
    char value = encoded[i];
    if (value == '%') {
      if (i + 2 >= encoded.size() || hex(encoded[i + 1]) < 0 ||
          hex(encoded[i + 2]) < 0) {
        throw std::invalid_argument("Invalid Paimon branch directory URI encoding");
      }
      value = static_cast<char>(hex(encoded[i + 1]) * 16 + hex(encoded[i + 2]));
      if (value == '\0' || value == '/' || value == '\\') {
        throw std::invalid_argument("Unsafe Paimon branch directory URI encoding");
      }
      i += 2;
    }
    path.push_back(value);
  }
  return path;
}

}  // namespace pangrova::split
