#include "engine/velox/plan/VeloxPlanConverter.h"

#include "engine/velox/plan/CanonicalJoinSemantics.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
void requireCanonicalJoinKeys(const google::protobuf::Message& relation) {
  for (const auto name : {"left_keys", "right_keys"}) {
    const auto field = relation.GetDescriptor()->FindFieldByName(name);
    if (relation.GetReflection()->FieldSize(relation, field) != 0) {
      throw std::runtime_error("Deprecated join key lists are not a canonical plan");
    }
  }
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertHashJoinRel(
    const substrait::HashJoinRel& relation, const std::vector<split::SplitConfig>& splits) {
  requireCanonicalJoinKeys(relation);
  auto left = convertRel(relation.left(), splits);
  auto right = rebaseJoinInput(convertRel(relation.right(), splits), left->outputType()->size());
  const auto kind = canonicalJoinKind(relation.type());
  const auto output = canonicalJoinOutput(kind, left->outputType(), right->outputType());
  auto keys = prepareJoinKeys(relation.keys(), left, right);
  const auto side = relation.build_input();
  if (side != substrait::HashJoinRel::BUILD_INPUT_UNSPECIFIED &&
      side != substrait::HashJoinRel::BUILD_INPUT_LEFT &&
      side != substrait::HashJoinRel::BUILD_INPUT_RIGHT) {
    throw std::runtime_error("Hash join has an unknown build input");
  }
  auto joined = !keys.hashable || kind.ends_with("_MARK")
      ? buildNestedJoin(kind, left, right, keys.condition)
      : buildKeyedJoin(kind, std::move(keys), output,
          side == substrait::HashJoinRel::BUILD_INPUT_LEFT, false);
  if (relation.has_post_join_filter()) joined = applyPostJoinFilter(relation.post_join_filter(), joined);
  return joined;
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertMergeJoinRel(
    const substrait::MergeJoinRel& relation, const std::vector<split::SplitConfig>& splits) {
  requireCanonicalJoinKeys(relation);
  auto left = convertRel(relation.left(), splits);
  auto right = rebaseJoinInput(convertRel(relation.right(), splits), left->outputType()->size());
  const auto kind = canonicalJoinKind(relation.type());
  const auto output = canonicalJoinOutput(kind, left->outputType(), right->outputType());
  auto keys = prepareJoinKeys(relation.keys(), left, right);
  auto joined = !keys.hashable || kind.ends_with("_MARK")
      ? buildNestedJoin(kind, left, right, keys.condition)
      : buildKeyedJoin(kind, std::move(keys), output,
          kind == "JOIN_TYPE_RIGHT_SINGLE", true);
  if (relation.has_post_join_filter()) joined = applyPostJoinFilter(relation.post_join_filter(), joined);
  return joined;
}
}  // namespace pangrova::velox
