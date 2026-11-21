#include "engine/velox/window/DistinctAggregateWindow.h"

#include <stdexcept>
#include <utility>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/DistinctAggregations.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

// Same engine aggregate and DISTINCT tuple equality as an AggregationNode,
// evaluated over the actual Window operator's ROWS/RANGE frame boundaries.
// Every buffer and variable-width accumulator belongs to the task's pool.
class DistinctAggregateWindow final : public exec::WindowFunction {
 public:
  DistinctAggregateWindow(const std::string& name,
      const std::vector<exec::WindowFunctionArg>& arguments, const TypePtr& resultType,
      memory::MemoryPool* pool, HashStringAllocator* allocator, const core::QueryConfig& config)
      : WindowFunction(resultType, pool, allocator) {
    if (arguments.empty()) throw std::invalid_argument("DISTINCT aggregate window requires an argument");
    initializeArguments(arguments);
    const auto registry = config.functionRegistrySnapshot();
    if (!registry) throw std::invalid_argument("DISTINCT window requires its admitted function registry owner");
    const auto binding = registry->bindAggregate(name, inputType_->children());
    if (!binding || !resultType->equivalent(*binding->resultType)) {
      throw std::invalid_argument("DISTINCT window does not match its owned aggregate signature: " + name);
    }
    info_.intermediateType = binding->intermediateType;
    info_.function = exec::Aggregate::create(name, core::AggregationNode::Step::kSingle,
        inputType_->children(), resultType, config);
    info_.function->setConstantInputs(info_.constantInputs);
    distinct_ = exec::DistinctAggregations::create({&info_}, inputType_, pool_);
    initializeStorage();
    resultRow_ = std::dynamic_pointer_cast<RowVector>(BaseVector::create(ROW({"result"}, {resultType_}), 1, pool_));
    resetState();
    extractResult();
    emptyResult_ = BaseVector::create(resultType_, 1, pool_);
    emptyResult_->copy(resultRow_->childAt(0).get(), 0, 0, 1);
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    resetState();
    previousStart_.reset();
    previousEnd_ = -1;
  }

  void apply(const BufferPtr&, const BufferPtr&, const BufferPtr& frameStarts,
      const BufferPtr& frameEnds, const SelectivityVector& validRows,
      vector_size_t resultOffset, const VectorPtr& result) override {
    if (!partition_) throw std::runtime_error("DISTINCT window has no input partition");
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
    const auto* starts = frameStarts->as<vector_size_t>();
    const auto* ends = frameEnds->as<vector_size_t>();
    validRows.applyToSelected([&](vector_size_t ordinal) {
      const auto start = starts[ordinal];
      const auto end = ends[ordinal];
      if (start < 0 || end < start || end >= partition_->numRows()) {
        throw std::runtime_error("DISTINCT window frame is outside its actual partition");
      }
      if (!previousStart_ || *previousStart_ != start || end < previousEnd_) {
        resetState();
        previousEnd_ = start - 1;
      }
      if (end != previousEnd_) {
        addInput(previousEnd_ + 1, end);
        extractResult();
      }
      result->copy(resultRow_->childAt(0).get(), resultOffset + ordinal, 0, 1);
      previousStart_ = start;
      previousEnd_ = end;
    });
  }

 private:
  void initializeArguments(const std::vector<exec::WindowFunctionArg>& arguments) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (size_t ordinal = 0; ordinal < arguments.size(); ++ordinal) {
      const auto& argument = arguments[ordinal];
      names.push_back("argument_" + std::to_string(ordinal));
      types.push_back(argument.type);
      inputChannels_.push_back(argument.index);
      info_.constantInputs.push_back(argument.constantValue);
      info_.inputs.push_back(argument.constantValue ? kConstantChannel : ordinal);
      if (!argument.constantValue && !argument.index) {
        throw std::invalid_argument("DISTINCT window argument has no input channel or constant");
      }
    }
    inputType_ = ROW(std::move(names), std::move(types));
  }

  void initializeStorage() {
    std::vector<exec::Accumulator> accumulators;
    accumulators.reserve(2);
    accumulators.emplace_back(info_.function.get(), info_.intermediateType);
    accumulators.push_back(distinct_->accumulator());
    rows_ = std::make_unique<exec::RowContainer>(std::vector<TypePtr>{}, true,
        accumulators,
        std::vector<TypePtr>{}, false, false, false, false, false, false, pool_);
    const auto aggregate = rows_->columnAt(0);
    info_.function->setAllocator(&rows_->stringAllocator());
    info_.function->setOffsets(aggregate.offset(), aggregate.nullByte(), aggregate.nullMask(),
        aggregate.initializedByte(), aggregate.initializedMask(), rows_->rowSizeOffset());
    const auto distinct = rows_->columnAt(1);
    distinct_->setAllocator(&rows_->stringAllocator());
    distinct_->setOffsets(distinct.offset(), distinct.nullByte(), distinct.nullMask(),
        distinct.initializedByte(), distinct.initializedMask(), rows_->rowSizeOffset());
  }

  void resetState() {
    if (group_) rows_->eraseRows(folly::Range<char**>(&group_, 1));
    group_ = rows_->newRow();
    const vector_size_t index = 0;
    distinct_->initializeNewGroups(&group_, folly::Range<const vector_size_t*>(&index, 1));
  }

  void addInput(vector_size_t start, vector_size_t end) {
    // Read only new rows of a prefix frame. For a moving lower boundary the
    // caller resets the set; a value leaving a frame must not remain distinct.
    const auto size = end - start + 1;
    std::vector<VectorPtr> children;
    for (size_t ordinal = 0; ordinal < inputChannels_.size(); ++ordinal) {
      if (info_.constantInputs[ordinal]) {
        children.push_back(BaseVector::wrapInConstant(size, 0, info_.constantInputs[ordinal]));
      } else {
        auto values = BaseVector::create(inputType_->childAt(ordinal), size, pool_);
        partition_->extractColumn(*inputChannels_[ordinal], start, size, 0, values);
        children.push_back(std::move(values));
      }
    }
    auto input = std::make_shared<RowVector>(pool_, inputType_, nullptr, size, std::move(children));
    distinct_->addSingleGroupInput(group_, input, SelectivityVector(size));
  }

  void extractResult() {
    distinct_->extractValues(folly::Range<char**>(&group_, 1), resultRow_);
  }

  const exec::WindowPartition* partition_{nullptr};
  RowTypePtr inputType_;
  std::vector<std::optional<column_index_t>> inputChannels_;
  exec::AggregateInfo info_;
  std::unique_ptr<exec::DistinctAggregations> distinct_;
  // Destroy RowContainer BEFORE its function and distinct callbacks.
  std::unique_ptr<exec::RowContainer> rows_;
  char* group_{nullptr};
  RowVectorPtr resultRow_;
  VectorPtr emptyResult_;
  std::optional<vector_size_t> previousStart_;
  vector_size_t previousEnd_{-1};
};
}  // namespace

std::string distinctAggregateWindowName(const std::string& aggregateName) {
  return "pangrova_distinct_window_" + aggregateName;
}

void registerDistinctAggregateWindows() {
  // Snapshot before registering: insertion must not invalidate registry traversal.
  std::vector<std::pair<std::string, exec::WindowFunctionEntry>> sources;
  for (const auto& [name, entry] : exec::windowFunctions()) {
    if (entry.metadata.isAggregate && exec::getAggregateFunctionEntry(name)) sources.emplace_back(name, entry);
  }
  for (const auto& source : sources) {
    const std::string name = source.first;
    const auto& entry = source.second;
    exec::registerWindowFunction(distinctAggregateWindowName(name), entry.signatures,
        {exec::WindowFunction::ProcessMode::kPartition, true},
        [name](const auto& args, const auto& type, bool ignoreNulls, auto* pool, auto* allocator, const auto& config) {
          if (ignoreNulls) throw std::invalid_argument("Aggregate window does not support IGNORE NULLS");
          return std::make_unique<DistinctAggregateWindow>(name, args, type, pool, allocator, config);
        });
  }
}

}  // namespace pangrova::velox
