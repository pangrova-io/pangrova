#include "velox/aggregate/SumZeroAggregate.h"

#include <stdexcept>

#include "velox/exec/Aggregate.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

template <typename Value>
void zeroEmptyValues(VectorPtr* result) {
  auto* flat = (*result)->asFlatVector<Value>();
  if (!flat) throw std::runtime_error("SUM final extraction must return its actual flat numeric vector");
  for (vector_size_t row = 0; row < flat->size(); ++row) {
    if (flat->isNullAt(row)) flat->set(row, Value{0});
  }
}

void zeroEmptyValues(VectorPtr* result) {
  switch ((*result)->typeKind()) {
    case TypeKind::BIGINT: zeroEmptyValues<int64_t>(result); return;
    case TypeKind::HUGEINT: zeroEmptyValues<int128_t>(result); return;
    case TypeKind::REAL: zeroEmptyValues<float>(result); return;
    case TypeKind::DOUBLE: zeroEmptyValues<double>(result); return;
    default: throw std::runtime_error("SUM has an unsupported final physical numeric type");
  }
}

class SumZeroAggregate final : public exec::Aggregate {
 public:
  explicit SumZeroAggregate(std::unique_ptr<exec::Aggregate> aggregate)
      : Aggregate(aggregate->resultType()), aggregate_(std::move(aggregate)) {
    aggregate_->enableValidateIntermediateInputs();
  }

  int32_t accumulatorFixedWidthSize() const override { return aggregate_->accumulatorFixedWidthSize(); }
  int32_t accumulatorAlignmentSize() const override { return aggregate_->accumulatorAlignmentSize(); }
  bool accumulatorUsesExternalMemory() const override { return aggregate_->accumulatorUsesExternalMemory(); }
  bool isFixedSize() const override { return aggregate_->isFixedSize(); }
  bool supportsToIntermediate() const override { return aggregate_->supportsToIntermediate(); }
  bool supportsCompact() const override { return aggregate_->supportsCompact(); }
  uint64_t compact(folly::Range<char**> groups) override { return aggregate_->compact(groups); }

  void setConstantInputs(const std::vector<VectorPtr>& inputs) override { aggregate_->setConstantInputs(inputs); }
  void initializeNewGroups(char** groups, folly::Range<const vector_size_t*> indices) override {
    aggregate_->initializeNewGroups(groups, indices);
  }
  void destroy(folly::Range<char**> groups) override { aggregate_->destroy(groups); }

  void addRawInput(char** groups, const SelectivityVector& rows, const std::vector<VectorPtr>& arguments,
      bool mayPushdown) override {
    aggregate_->addRawInput(groups, rows, arguments, mayPushdown);
  }
  void addIntermediateResults(char** groups, const SelectivityVector& rows,
      const std::vector<VectorPtr>& arguments, bool mayPushdown) override {
    aggregate_->addIntermediateResults(groups, rows, arguments, mayPushdown);
  }
  void addSingleGroupRawInput(char* group, const SelectivityVector& rows,
      const std::vector<VectorPtr>& arguments, bool mayPushdown) override {
    aggregate_->addSingleGroupRawInput(group, rows, arguments, mayPushdown);
  }
  void addSingleGroupIntermediateResults(char* group, const SelectivityVector& rows,
      const std::vector<VectorPtr>& arguments, bool mayPushdown) override {
    aggregate_->addSingleGroupIntermediateResults(group, rows, arguments, mayPushdown);
  }
  void toIntermediate(const SelectivityVector& rows, std::vector<VectorPtr>& arguments,
      VectorPtr& result) const override {
    aggregate_->toIntermediate(rows, arguments, result);
  }
  void extractValues(char** groups, int32_t count, VectorPtr* result) override {
    aggregate_->extractValues(groups, count, result);
    zeroEmptyValues(result);
  }
  void extractAccumulators(char** groups, int32_t count, VectorPtr* result) override {
    // Preserve the actual SUM state. Empty input is not a fabricated input zero.
    aggregate_->extractAccumulators(groups, count, result);
  }

 protected:
  void initializeNewGroupsInternal(char** groups, folly::Range<const vector_size_t*> indices) override {
    aggregate_->initializeNewGroups(groups, indices);
  }
  void setAllocatorInternal(HashStringAllocator* allocator) override { aggregate_->setAllocator(allocator); }
  void setOffsetsInternal(int32_t offset, int32_t nullByte, uint8_t nullMask,
      int32_t initializedByte, uint8_t initializedMask, int32_t rowSizeOffset) override {
    aggregate_->setOffsets(offset, nullByte, nullMask, initializedByte, initializedMask, rowSizeOffset);
  }
  void clearInternal() override { aggregate_->clear(); }

 private:
  std::unique_ptr<exec::Aggregate> aggregate_;
};
}  // namespace

void registerSumZeroAggregate() {
  using namespace facebook::velox;
  const auto signatures = exec::getAggregateFunctionSignatures("sum");
  if (!signatures) throw std::runtime_error("SUM must be registered before SUM0");
  exec::registerAggregateFunction("sum0", *signatures,
      [](core::AggregationNode::Step step, const auto& arguments, const auto& result, const auto& config) {
        return std::make_unique<SumZeroAggregate>(exec::Aggregate::create("sum", step, arguments, result, config));
      }, exec::getAggregateFunctionMetadata("sum"), true, false);
}

}  // namespace pangrova::velox
