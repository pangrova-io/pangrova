#pragma once

#include <bit>
#include <cstdint>
#include <istream>
#include <ostream>

namespace pangrova::vector::ann {

inline void writeUint32(std::ostream& output, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    output.put(static_cast<char>((value >> shift) & 0xffU));
  }
}

inline void writeUint64(std::ostream& output, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    output.put(static_cast<char>((value >> shift) & 0xffU));
  }
}

inline void writeFloat(std::ostream& output, float value) {
  writeUint32(output, std::bit_cast<uint32_t>(value));
}

inline bool readUint32(std::istream& input, uint32_t& value) {
  value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    const int byte = input.get();
    if (byte == std::char_traits<char>::eof()) {
      return false;
    }
    value |= static_cast<uint32_t>(static_cast<unsigned char>(byte)) << shift;
  }
  return true;
}

inline bool readUint64(std::istream& input, uint64_t& value) {
  value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    const int byte = input.get();
    if (byte == std::char_traits<char>::eof()) {
      return false;
    }
    value |= static_cast<uint64_t>(static_cast<unsigned char>(byte)) << shift;
  }
  return true;
}

inline bool readFloat(std::istream& input, float& value) {
  uint32_t bits = 0;
  if (!readUint32(input, bits)) {
    return false;
  }
  value = std::bit_cast<float>(bits);
  return true;
}

}  // namespace pangrova::vector::ann
