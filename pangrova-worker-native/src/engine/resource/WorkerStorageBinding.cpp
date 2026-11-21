#include "engine/resource/WorkerStorageBinding.h"

#include <filesystem>
#include <stdexcept>
#include <utility>

namespace pangrova::resource {
namespace {

std::pair<std::string, std::filesystem::path> storageLocation(const std::string& uri) {
  if (uri.empty() || uri.size() > 8192 || uri.find_first_of("\\?#") != std::string::npos) {
    throw std::invalid_argument("Invalid absolute storage location");
  }
  std::string kind = "file";
  std::string raw = uri;
  if (uri.starts_with("file:")) {
    raw = uri.substr(5);
    if (raw.starts_with("//")) {
      if (!raw.starts_with("///")) throw std::invalid_argument("Remote file authority is forbidden");
      raw.erase(0, 2);
    }
  } else if (uri.starts_with("s3://")) {
    const auto slash = uri.find('/', 5);
    if (uri.size() == 5 || slash == 5) throw std::invalid_argument("S3 bucket is missing");
    kind = slash == std::string::npos ? uri : uri.substr(0, slash);
    raw = slash == std::string::npos ? "/" : uri.substr(slash);
  }
  std::string decoded;
  for (size_t index = 0; index < raw.size(); ++index) {
    if (raw[index] != '%') { decoded += raw[index]; continue; }
    if (index + 2 >= raw.size()) throw std::invalid_argument("Malformed percent encoding");
    const auto digit = [](char value) -> int {
      if (value >= '0' && value <= '9') return value - '0';
      if (value >= 'a' && value <= 'f') return value - 'a' + 10;
      if (value >= 'A' && value <= 'F') return value - 'A' + 10;
      return -1;
    };
    const int high = digit(raw[index + 1]), low = digit(raw[index + 2]);
    if (high < 0 || low < 0) throw std::invalid_argument("Malformed percent encoding");
    const char value = static_cast<char>((high << 4) | low);
    if (value == '/' || value == '\\' || value == 0 || value == '%') {
      throw std::invalid_argument("Encoded storage separator is forbidden");
    }
    decoded += value;
    index += 2;
  }
  std::filesystem::path path(decoded);
  if (!path.is_absolute()) throw std::invalid_argument("Storage path must be absolute");
  for (const auto& segment : path) {
    if (segment == "." || segment == "..") throw std::invalid_argument("Storage traversal is forbidden");
  }
  return {kind, path.lexically_normal()};
}

}  // namespace

void validateStorageLocation(const protocol::v2::ObjectStoreExecutionBinding& binding,
                                    const std::string& uri) {
  const auto [root_kind, root] = storageLocation(binding.warehouse_uri());
  const auto [kind, path] = storageLocation(uri);
  auto root_part = root.begin(), path_part = path.begin();
  for (; root_part != root.end() && path_part != path.end(); ++root_part, ++path_part) {
    if (*root_part != *path_part) break;
  }
  if (kind != root_kind || root_part != root.end() || path_part == path.end()) {
    throw std::invalid_argument("Storage location escapes its exact catalog warehouse");
  }
}

std::string workerLocalStorage(
    const google::protobuf::RepeatedPtrField<protocol::v2::ObjectStoreExecutionBinding>& bindings,
    const protocol::v2::CatalogExecutionBinding& catalog,
    const std::string& tenant, const std::string& uri) {
  const protocol::v2::ObjectStoreExecutionBinding* selected = nullptr;
  for (const auto& binding : bindings) {
    if (binding.catalog().SerializeAsString() != catalog.SerializeAsString() ||
        binding.tenant_id() != tenant) continue;
    if (selected) throw std::invalid_argument("Duplicate Worker-local storage authority");
    if (!uri.empty()) validateStorageLocation(binding, uri);
    selected = &binding;
  }
  if (!selected) throw std::invalid_argument("Dataset access requires exact Worker-local catalog storage");
  return selected->SerializeAsString();
}

}  // namespace pangrova::resource
