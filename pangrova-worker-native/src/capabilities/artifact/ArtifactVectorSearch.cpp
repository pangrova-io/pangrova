#include "capabilities/artifact/ArtifactVectorSearch.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <unordered_map>

#include "capabilities/artifact/RowLocatorSidecar.h"
#include "vector/VectorDistance.h"
#include "vector/diskann/DiskAnnIndexCache.h"

namespace pangrova::artifact {
namespace {

struct ExactCandidate {
  uint64_t position;
  double distance;
};

bool better(const ExactCandidate& left, const ExactCandidate& right) {
  return left.distance == right.distance ? left.position < right.position
                                        : left.distance < right.distance;
}

double rescore(
    std::ifstream& input,
    uint64_t node,
    std::span<const float> query,
    pangrova::resource::OwnerVector<float>& scratch) {
  const auto row_bytes = query.size() * sizeof(float);
  if (row_bytes == 0 || node >
          static_cast<uint64_t>(std::numeric_limits<std::streamoff>::max()) / row_bytes) {
    throw std::runtime_error("ANN full-precision rescore offset overflows");
  }
  input.clear();
  input.seekg(static_cast<std::streamoff>(node * row_bytes));
  // One bulk read per candidate instead of one per dimension. Rescoring runs for
  // every candidate, so at dimension 1536 the per-float loop dominated the exact
  // rerank cost. The file stores little-endian floats, so decode explicitly.
  const size_t vector_bytes = scratch.size() * sizeof(float);
  std::vector<unsigned char> raw(vector_bytes);
  input.read(reinterpret_cast<char*>(raw.data()),
             static_cast<std::streamsize>(vector_bytes));
  if (input.gcount() != static_cast<std::streamsize>(vector_bytes)) {
    throw std::runtime_error("ANN raw vector is truncated or non-finite");
  }
  for (size_t index = 0; index < scratch.size(); ++index) {
    const auto* word = raw.data() + index * 4;
    const uint32_t bits = static_cast<uint32_t>(word[0]) |
        (static_cast<uint32_t>(word[1]) << 8) |
        (static_cast<uint32_t>(word[2]) << 16) |
        (static_cast<uint32_t>(word[3]) << 24);
    const float value = std::bit_cast<float>(bits);
    if (!std::isfinite(value)) {
      throw std::runtime_error("ANN raw vector is truncated or non-finite");
    }
    scratch[index] = value;
  }
  const double result = vector::l2Distance(query.data(), scratch.data(), query.size());
  if (!std::isfinite(result)) {
    throw std::runtime_error("ANN exact distance is non-finite");
  }
  return result;
}

}  // namespace

std::vector<float> ArtifactVectorSearch::boundQuery(
    const protocol::v2::CapabilityInvocation& invocation) {
  const protocol::v2::NamedValue* option = nullptr;
  for (const auto& candidate : invocation.options()) {
    if (candidate.name() == "query_vector") {
      if (option) throw std::runtime_error("Duplicate ANN query_vector option");
      option = &candidate;
    }
  }
  if (!option) throw std::runtime_error("Missing ANN query_vector option");
  const auto& value = option->value();
  if (!value.has_tensor_value()) {
    throw std::runtime_error("ANN query_vector must be a typed tensor");
  }
  const auto& tensor = value.tensor_value();
  if (tensor.element_type() != protocol::v2::TensorValue::ELEMENT_TYPE_FLOAT32 ||
      tensor.shape_size() != 1 || tensor.shape(0) == 0 ||
      tensor.shape(0) > 65'536 ||
      tensor.packed_little_endian_values().size() !=
          tensor.shape(0) * sizeof(float)) {
    throw std::runtime_error("ANN query_vector tensor shape is invalid");
  }
  std::vector<float> result(static_cast<size_t>(tensor.shape(0)));
  const auto& packed = tensor.packed_little_endian_values();
  for (size_t index = 0; index < result.size(); ++index) {
    const auto offset = index * sizeof(uint32_t);
    uint32_t bits = 0;
    for (size_t byte = 0; byte < sizeof(uint32_t); ++byte) {
      bits |= static_cast<uint32_t>(
          static_cast<unsigned char>(packed[offset + byte])) << (byte * 8);
    }
    result[index] = std::bit_cast<float>(bits);
    if (!std::isfinite(result[index])) {
      throw std::runtime_error("ANN query_vector contains a non-finite value");
    }
  }
  return result;
}

struct ArtifactVectorSearch::State {
  explicit State(pangrova::resource::OwnerMemoryResource& owner)
      : resource(owner), positions(&owner), position_to_node(&owner), eligible(&owner) {}
  using Roles = std::array<const protocol::v2::ArtifactObject*, 3>;
  Roles validateBundle(std::span<const protocol::v2::ArtifactObject> objects) {
    Roles roles{};
    for (const auto& object : objects) {
      if (object.scope() != protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT ||
          object.source_object_id().empty() || object.row_count() == 0 ||
          (!object_id.empty() && object_id != object.source_object_id())) {
        throw std::runtime_error("ANN source bundle identity is inconsistent");
      }
      object_id = object.source_object_id();
      size_t ordinal = object.object_role() == "DISKANN_GRAPH" ? 0
          : object.object_role() == "RAW_VECTORS" ? 1
          : object.object_role() == "ROW_LOCATORS" ? 2 : 3;
      if (ordinal == 3 || object.ordinal() != ordinal || roles[ordinal]) {
        throw std::runtime_error("ANN source bundle role is duplicated or invalid");
      }
      roles[ordinal] = &object;
    }
    if (roles[0]->row_count() != roles[1]->row_count() ||
        roles[0]->row_count() != roles[2]->row_count()) {
      throw std::runtime_error("ANN source bundle row counts differ");
    }
    return roles;
  }
  void materializeBundle(const Roles& roles, uint64_t remaining_read_bytes,
      const std::function<void()>& checkpoint,
      const std::shared_ptr<pangrova::resource::TaskObjectReadBudget>& task_budget) {
    auto materialize = [&](size_t ordinal) {
      checkpoint();
      auto lease = ImmutableArtifactCache::instance().materialize(
          *roles[ordinal], remaining_read_bytes - downloaded, task_budget);
      if (lease.downloaded_bytes > remaining_read_bytes - downloaded) {
        throw std::runtime_error("ANN immutable read grant exceeded");
      }
      downloaded += lease.downloaded_bytes;
      checkpoint();
      return lease;
    };
    graph = materialize(0);
    raw = materialize(1);
    locators = materialize(2);
  }
  void loadGraph(const Roles& roles, int64_t query_dimension) {
    dimension = query_dimension;
    index = vector::diskann::DiskAnnIndexCache::instance().getOrLoad(
        graph.local_path, dimension);
    if (!index || index->dimension() != dimension ||
        index->numVectors() <= 0 ||
        static_cast<uint64_t>(index->numVectors()) != roles[0]->row_count()) {
      throw std::runtime_error("ANN graph differs from its signed source bundle");
    }
    const uint64_t count = roles[0]->row_count();
    const uint64_t row_bytes = static_cast<uint64_t>(dimension) * sizeof(float);
    if (count > std::numeric_limits<uint64_t>::max() / row_bytes ||
        raw.size_bytes != count * row_bytes) {
      throw std::runtime_error("ANN raw-vector object has an invalid size");
    }
  }
  void loadLocators(uint64_t count, const std::function<void()>& checkpoint) {
    positions.resize(count);
    eligible.resize(count, 0);
    const uint64_t actual_count = RowLocatorSidecar::visitPhysicalRowPositions(
        locators.local_path, count, [&](uint64_t node, uint64_t position) {
          checkpoint();
          if (position > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
              !position_to_node.emplace(position, node).second) {
            throw std::runtime_error("ANN physical locator is invalid or duplicated");
          }
          positions.at(node) = position;
        });
    if (actual_count != count) {
      throw std::runtime_error("ANN row-locator count differs from the graph");
    }
  }
  pangrova::resource::OwnerMemoryResource& resource;
  std::string object_id;
  int64_t dimension = 0;
  MaterializedArtifact graph;
  MaterializedArtifact raw;
  MaterializedArtifact locators;
  std::shared_ptr<vector::diskann::VamanaIndex> index;
  pangrova::resource::OwnerVector<uint64_t> positions;
  pangrova::resource::OwnerUnorderedMap<uint64_t, uint64_t> position_to_node;
  pangrova::resource::OwnerVector<uint8_t> eligible;
  uint64_t eligible_count = 0;
  uint64_t downloaded = 0;
};

ArtifactVectorSearch::ArtifactVectorSearch(
    std::span<const protocol::v2::ArtifactObject> objects,
    int64_t dimension,
    uint64_t remaining_read_bytes,
    pangrova::resource::OwnerMemoryResource& resource,
    const std::function<void()>& checkpoint,
    std::shared_ptr<pangrova::resource::TaskObjectReadBudget> task_budget)
    : state_(std::make_unique<State>(resource)) {
  if (objects.size() != 3 || dimension <= 0 || dimension > 65'536 || !checkpoint) {
    throw std::invalid_argument("ANN search requires a complete bounded source bundle");
  }
  const auto roles = state_->validateBundle(objects);
  state_->materializeBundle(roles, remaining_read_bytes, checkpoint, task_budget);
  state_->loadGraph(roles, dimension);
  state_->loadLocators(roles[0]->row_count(), checkpoint);
}

ArtifactVectorSearch::~ArtifactVectorSearch() = default;

void ArtifactVectorSearch::addEligible(uint64_t physical_position) {
  const auto node = state_->position_to_node.find(physical_position);
  if (node == state_->position_to_node.end() || state_->eligible.at(node->second)) {
    throw std::runtime_error("ANN eligibility locator is unknown or duplicated");
  }
  state_->eligible[node->second] = 1;
  ++state_->eligible_count;
}

uint64_t ArtifactVectorSearch::eligibleCount() const { return state_->eligible_count; }
uint64_t ArtifactVectorSearch::rowCount() const { return state_->positions.size(); }
const std::string& ArtifactVectorSearch::sourceObjectId() const { return state_->object_id; }
uint64_t ArtifactVectorSearch::downloadedBytes() const { return state_->downloaded; }
std::array<MaterializedArtifact, 3> ArtifactVectorSearch::materializedObjects() const {
  return {state_->graph, state_->raw, state_->locators};
}

pangrova::resource::OwnerVector<uint64_t> ArtifactVectorSearch::searchUnfiltered(
    std::span<const float> query,
    uint64_t candidate_limit,
    uint64_t exact_rescore_limit,
    uint64_t search_list_size,
    const std::function<void()>& checkpoint) {
  if (query.size() != static_cast<size_t>(state_->dimension) || !checkpoint ||
      candidate_limit == 0 || exact_rescore_limit == 0 ||
      exact_rescore_limit > candidate_limit || search_list_size == 0 ||
      candidate_limit > 100'000 || search_list_size > 100'000) {
    throw std::invalid_argument("ANN unfiltered work differs from its bound query");
  }
  checkpoint();
  auto candidates = state_->index->search(query.data(), candidate_limit,
      std::min<uint64_t>(64, search_list_size), search_list_size);
  std::ifstream raw(state_->raw.local_path, std::ios::binary);
  if (!raw) {
    throw std::runtime_error("Cannot open the pinned ANN raw-vector object");
  }
  pangrova::resource::OwnerVector<float> scratch(query.size(), &state_->resource);
  pangrova::resource::OwnerVector<ExactCandidate> scored(&state_->resource);
  scored.reserve(candidates.size());
  for (const auto& candidate : candidates) {
    checkpoint();
    if (candidate.nodeId < 0 ||
        static_cast<uint64_t>(candidate.nodeId) >= state_->positions.size()) {
      throw std::runtime_error("ANN candidate has no physical locator");
    }
    scored.push_back({state_->positions.at(static_cast<size_t>(candidate.nodeId)),
        rescore(raw, candidate.nodeId, query, scratch)});
  }
  std::sort(scored.begin(), scored.end(), better);
  pangrova::resource::OwnerVector<uint64_t> output(&state_->resource);
  for (size_t ordinal = 0; ordinal < std::min<uint64_t>(scored.size(), exact_rescore_limit);
       ++ordinal) {
    output.push_back(scored[ordinal].position);
  }
  return output;
}

pangrova::resource::OwnerVector<uint64_t> ArtifactVectorSearch::search(
    std::span<const float> query,
    const VectorSearchWork& work,
    VectorSearchUsage& usage,
    const std::function<void()>& checkpoint) {
  if (query.size() != static_cast<size_t>(state_->dimension) || !checkpoint ||
      work.output_limit == 0 || work.max_expanded == 0 ||
      work.max_rescored < work.output_limit) {
    throw std::invalid_argument("ANN search work differs from its bound query");
  }
  checkpoint();
  pangrova::resource::OwnerVector<uint64_t> output(&state_->resource);
  if (state_->eligible_count == 0) {
    return output;
  }
  if (usage.expanded >= work.max_expanded) {
    throw std::runtime_error("ANN graph exploration grant exhausted");
  }
  std::ifstream raw(state_->raw.local_path, std::ios::binary);
  if (!raw) {
    throw std::runtime_error("Cannot open the pinned ANN raw-vector object");
  }
  pangrova::resource::OwnerVector<float> scratch(query.size(), &state_->resource);
  pangrova::resource::OwnerVector<ExactCandidate> heap(&state_->resource);
  heap.reserve(static_cast<size_t>(std::min(work.output_limit, state_->eligible_count)));
  auto stats = state_->index->traverse(query.data(), work.max_expanded - usage.expanded,
      state_->resource, checkpoint, [&](int64_t node, float) {
        if (!state_->eligible.at(static_cast<size_t>(node))) {
          return;
        }
        if (usage.rescored >= work.max_rescored) {
          throw std::runtime_error("ANN exact-rescore grant exhausted");
        }
        ++usage.rescored;
        ExactCandidate candidate{state_->positions.at(static_cast<size_t>(node)),
            rescore(raw, static_cast<uint64_t>(node), query, scratch)};
        if (heap.size() < work.output_limit) {
          heap.push_back(candidate);
          std::push_heap(heap.begin(), heap.end(), better);
        } else if (better(candidate, heap.front())) {
          std::pop_heap(heap.begin(), heap.end(), better);
          heap.back() = candidate;
          std::push_heap(heap.begin(), heap.end(), better);
        }
      });
  usage.expanded += stats.expanded;
  const auto wanted = static_cast<size_t>(
      std::min<uint64_t>(work.output_limit, state_->eligible_count));
  if (heap.size() < wanted) {
    // Deleted nodes and selective allowlists can make part of the eligible
    // population unreachable from the graph. Return what exists and surface the
    // gap to the operator instead of failing an otherwise valid query.
    usage.recall_shortfall += wanted - heap.size();
  }
  std::sort(heap.begin(), heap.end(), better);
  for (const auto& candidate : heap) {
    output.push_back(candidate.position);
  }
  return output;
}

}  // namespace pangrova::artifact
