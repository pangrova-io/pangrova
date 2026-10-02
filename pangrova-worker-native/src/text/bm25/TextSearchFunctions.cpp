// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "text/bm25/TextSearchFunctions.h"

#include <memory>
#include <iterator>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "text/bm25/Bm25Analyzer.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::text::bm25 {
namespace {

using namespace facebook::velox;

using TokenSet = std::unordered_set<std::string>;

TokenSet uniqueTokens(std::string_view value) {
  auto tokens = Bm25Analyzer::tokenize(value);
  return TokenSet(
      std::make_move_iterator(tokens.begin()),
      std::make_move_iterator(tokens.end()));
}

bool intersects(const std::vector<std::string>& document, const TokenSet& query) {
  for (const auto& token : document) {
    if (query.contains(token)) {
      return true;
    }
  }
  return false;
}

class MatchFunction final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* output_type */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decoded_args(rows, args, context);
    const auto& text = *decoded_args.at(0);
    const auto& query = *decoded_args.at(1);
    auto local = BaseVector::create(BOOLEAN(), rows.end(), context.pool());
    auto output = local->asFlatVector<bool>();
    std::unordered_map<std::string, TokenSet> query_cache;

    rows.applyToSelected([&](vector_size_t row) {
      if (text.isNullAt(row) || query.isNullAt(row)) {
        output->setNull(row, true);
        return;
      }
      const auto query_view = query.valueAt<StringView>(row);
      std::string query_text(query_view.data(), query_view.size());
      auto [cached, inserted] = query_cache.try_emplace(query_text);
      if (inserted) {
        cached->second = uniqueTokens(query_text);
      }
      if (cached->second.empty()) {
        output->set(row, false);
        return;
      }
      const auto text_view = text.valueAt<StringView>(row);
      output->set(row, intersects(
          Bm25Analyzer::tokenize(
              std::string_view(text_view.data(), text_view.size())),
          cached->second));
    });
    context.moveOrCopyResult(local, rows, result);
  }
};

std::vector<exec::FunctionSignaturePtr> matchSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

}  // namespace

bool matchesAnalyzedText(std::string_view text, std::string_view query) {
  const TokenSet query_tokens = uniqueTokens(query);
  return !query_tokens.empty() &&
      intersects(Bm25Analyzer::tokenize(text), query_tokens);
}

void registerTextSearchFunctions() {
  exec::registerVectorFunction(
      "match", matchSignatures(), std::make_unique<MatchFunction>());
}

}  // namespace pangrova::text::bm25
