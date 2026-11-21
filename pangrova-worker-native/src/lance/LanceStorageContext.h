#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include "lance_c_ffi.h"

namespace pangrova::lance {

// Decodes exactly the canonical Worker-local storage contract. Rust copies the
// settings into live dataset/writer handles; no process environment is changed.
class LanceStorageContext final {
 public:
  explicit LanceStorageContext(const std::string& canonicalBinding)
      : storage_(lance_ffi_storage_open(
            reinterpret_cast<const uint8_t*>(canonicalBinding.data()), canonicalBinding.size()),
            lance_ffi_storage_close) {
    if (!storage_) {
      const char* error = lance_ffi_last_error();
      throw std::invalid_argument(std::string("Invalid Worker-local storage authority: ") +
                                  (error ? error : "no FFI diagnostic"));
    }
  }
  const LanceCStorage* get() const { return storage_.get(); }

 private:
  std::unique_ptr<LanceCStorage, decltype(&lance_ffi_storage_close)> storage_;
};

}  // namespace pangrova::lance
