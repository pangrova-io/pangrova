#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "engine/resource/OwnerMemoryResource.h"

#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "vector/diskann/VamanaGraph.h"

namespace pangrova::artifact {

struct VectorSearchWork {
  uint64_t output_limit;
  uint64_t max_expanded;
  uint64_t max_rescored;
};

struct VectorSearchUsage {
  uint64_t downloaded_bytes = 0;
  uint64_t expanded = 0;
  uint64_t rescored = 0;
};

/** One real checksum-verified DiskANN source bundle and its physical locators. */
class ArtifactVectorSearch final {
 public:
  static std::vector<float> boundQuery(const protocol::v2::CapabilityInvocation& invocation);
  ArtifactVectorSearch(
      std::span<const protocol::v2::ArtifactObject> objects,
      int64_t dimension,
      uint64_t remaining_read_bytes,
      pangrova::resource::OwnerMemoryResource& resource,
      const std::function<void()>& checkpoint,
      std::shared_ptr<pangrova::resource::TaskObjectReadBudget> task_budget = nullptr);
  ~ArtifactVectorSearch();

  ArtifactVectorSearch(const ArtifactVectorSearch&) = delete;
  ArtifactVectorSearch& operator=(const ArtifactVectorSearch&) = delete;

  // Mark exactly one active physical source row accepted by the actual SQL
  // Filter. A duplicate or unknown locator is a binding/identity failure.
  void addEligible(uint64_t physical_position);
  uint64_t eligibleCount() const;
  uint64_t rowCount() const;
  const std::string& sourceObjectId() const;
  uint64_t downloadedBytes() const;
  std::array<MaterializedArtifact, 3> materializedObjects() const;

  // The existing unfiltered route uses this same materialization/locator/raw
  // rescore owner. Its admitted PQ candidate count remains unchanged.
  pangrova::resource::OwnerVector<uint64_t> searchUnfiltered(
      std::span<const float> query,
      uint64_t candidate_limit,
      uint64_t exact_rescore_limit,
      uint64_t search_list_size,
      const std::function<void()>& checkpoint);

  // Search never tests a SQL predicate. Only the upstream owner sets
  // eligibility; nonmatching bridge nodes remain traversable.
  pangrova::resource::OwnerVector<uint64_t> search(
      std::span<const float> query,
      const VectorSearchWork& work,
      VectorSearchUsage& usage,
      const std::function<void()>& checkpoint);

 private:
  struct State;
  std::unique_ptr<State> state_;
};

}  // namespace pangrova::artifact
