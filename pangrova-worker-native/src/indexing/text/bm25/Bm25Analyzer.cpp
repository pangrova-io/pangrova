// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "indexing/text/bm25/Bm25Analyzer.h"

#include <cstdint>
#include <stdexcept>

namespace pangrova::text::bm25 {
namespace {

bool isCjk(uint32_t code_point) {
  return (code_point >= 0x3400 && code_point <= 0x4DBF) ||
      (code_point >= 0x4E00 && code_point <= 0x9FFF) ||
      (code_point >= 0xF900 && code_point <= 0xFAFF) ||
      (code_point >= 0x20000 && code_point <= 0x2FA1F) ||
      (code_point >= 0x3040 && code_point <= 0x30FF) ||
      (code_point >= 0xAC00 && code_point <= 0xD7AF);
}

void appendCodePoint(std::string& output, uint32_t code_point) {
  if (code_point <= 0x7F) {
    output.push_back(static_cast<char>(code_point));
  } else if (code_point <= 0x7FF) {
    output.push_back(static_cast<char>(0xC0 | (code_point >> 6)));
    output.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else if (code_point <= 0xFFFF) {
    output.push_back(static_cast<char>(0xE0 | (code_point >> 12)));
    output.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    output.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else {
    output.push_back(static_cast<char>(0xF0 | (code_point >> 18)));
    output.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
    output.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    output.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  }
}

uint32_t nextCodePoint(std::string_view text, size_t& offset) {
  const auto first = static_cast<unsigned char>(text[offset++]);
  if (first < 0x80) {
    return first;
  }
  int continuation_count = 0;
  uint32_t result = 0;
  uint32_t minimum = 0;
  if ((first & 0xE0) == 0xC0) {
    continuation_count = 1;
    result = first & 0x1F;
    minimum = 0x80;
  } else if ((first & 0xF0) == 0xE0) {
    continuation_count = 2;
    result = first & 0x0F;
    minimum = 0x800;
  } else if ((first & 0xF8) == 0xF0) {
    continuation_count = 3;
    result = first & 0x07;
    minimum = 0x10000;
  } else {
    throw std::invalid_argument("BM25 input contains invalid UTF-8");
  }
  for (int index = 0; index < continuation_count; ++index) {
    if (offset >= text.size()) {
      throw std::invalid_argument("BM25 input contains truncated UTF-8");
    }
    const auto byte = static_cast<unsigned char>(text[offset++]);
    if ((byte & 0xC0) != 0x80) {
      throw std::invalid_argument("BM25 input contains invalid UTF-8");
    }
    result = (result << 6) | (byte & 0x3F);
  }
  if (result < minimum || result > 0x10FFFF ||
      (result >= 0xD800 && result <= 0xDFFF)) {
    throw std::invalid_argument("BM25 input contains non-canonical UTF-8");
  }
  return result;
}

bool isWord(uint32_t code_point) {
  if (code_point < 0x80) {
    return (code_point >= '0' && code_point <= '9') ||
        (code_point >= 'A' && code_point <= 'Z') ||
        (code_point >= 'a' && code_point <= 'z');
  }
  return !(code_point >= 0x2000 && code_point <= 0x206F) &&
      !(code_point >= 0x3000 && code_point <= 0x303F) &&
      !(code_point >= 0xFE10 && code_point <= 0xFE6F) &&
      !(code_point >= 0xFF00 && code_point <= 0xFF65);
}

void flush(std::string& token, std::vector<std::string>& output) {
  if (!token.empty()) {
    output.push_back(std::move(token));
    token.clear();
  }
}

}  // namespace

std::vector<std::string> Bm25Analyzer::tokenize(
    std::string_view text,
    std::string_view analyzer) {
  if (analyzer != kStandardAnalyzer) {
    throw std::invalid_argument("Unsupported BM25 analyzer: " +
                                std::string(analyzer));
  }
  std::vector<std::string> tokens;
  std::string token;
  size_t offset = 0;
  while (offset < text.size()) {
    uint32_t code_point = nextCodePoint(text, offset);
    if (isCjk(code_point)) {
      flush(token, tokens);
      std::string cjk;
      appendCodePoint(cjk, code_point);
      tokens.push_back(std::move(cjk));
    } else if (isWord(code_point)) {
      if (code_point >= 'A' && code_point <= 'Z') {
        code_point += 'a' - 'A';
      }
      appendCodePoint(token, code_point);
    } else {
      flush(token, tokens);
    }
  }
  flush(token, tokens);
  return tokens;
}

}  // namespace pangrova::text::bm25
