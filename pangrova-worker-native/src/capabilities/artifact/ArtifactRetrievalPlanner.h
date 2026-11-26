// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "pangrova/protocol/v2/execution_binding.pb.h"
#include "engine/split/SplitConfig.h"

namespace substrait {
class Plan;
}

namespace pangrova::artifact {

class ArtifactVectorSearch;

/** Task-local result of resolving immutable retrieval artifacts in Native. */
struct ArtifactRetrievalPreparation final {
  std::unordered_map<std::string, std::vector<split::SplitConfig>>
      split_overrides;
  std::vector<MaterializedArtifact> leases;
  std::vector<std::shared_ptr<ArtifactVectorSearch>> vector_search_owners;
  uint64_t downloaded_bytes{0};
};

/**
 * Resolves canonical retrieval relations into source-object row allowlists.
 * Candidate generation and exact vector rescore execute entirely in Native;
 * the JVM Coordinator never receives vectors or candidate rows.
 */
class ArtifactRetrievalPlanner final {
 public:
  static std::shared_ptr<ArtifactRetrievalPreparation> prepare(
      const substrait::Plan& plan,
      const std::unordered_map<
          std::string, protocol::v2::OperatorBinding>& operator_bindings,
      const std::vector<split::SplitConfig>& splits,
      uint64_t max_object_store_read_bytes,
      std::shared_ptr<resource::TaskObjectReadBudget> task_budget = nullptr);
};

}  // namespace pangrova::artifact
