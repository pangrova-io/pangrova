#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>

namespace pangrova::split {

/** Decode an absolute local file URI without accepting an authority or escaped separators. */
inline std::string decodeLocalFileUri(const std::string& uri) {
  if (!uri.starts_with("file:/") || uri.starts_with("file:////") ||
      (uri.starts_with("file://") && !uri.starts_with("file:///"))) {
    throw std::invalid_argument("Expected an absolute local file URI");
  }
  const auto encoded = uri.starts_with("file:///") ? uri.substr(7) : uri.substr(5);
  if (encoded.empty() || encoded.front() != '/' ||
      encoded.find('?') != std::string::npos ||
      encoded.find('#') != std::string::npos) {
    throw std::invalid_argument("Invalid local file URI path");
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
    auto value = encoded[i];
    if (value == '%') {
      if (i + 2 >= encoded.size() || hex(encoded[i + 1]) < 0 ||
          hex(encoded[i + 2]) < 0) {
        throw std::invalid_argument("Invalid local file URI encoding");
      }
      value = static_cast<char>(hex(encoded[i + 1]) * 16 + hex(encoded[i + 2]));
      if (value == '\0' || value == '/' || value == '\\') {
        throw std::invalid_argument("Unsafe local file URI encoding");
      }
      i += 2;
    }
    path.push_back(value);
  }
  if (path.find('\0') != std::string::npos ||
      path.find('\\') != std::string::npos) {
    throw std::invalid_argument("Unsafe local file URI path");
  }
  for (size_t begin = 0; begin < path.size();) {
    const auto end = path.find('/', begin);
    const auto segment = path.substr(begin, end - begin);
    if (segment == "." || segment == "..") {
      throw std::invalid_argument("Local file URI contains a dot path segment");
    }
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  return path;
}

}  // namespace pangrova::split
