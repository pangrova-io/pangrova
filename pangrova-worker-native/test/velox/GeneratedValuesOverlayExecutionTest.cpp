// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <cstdlib>
#include <stdexcept>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "artifact/GeneratedValueSegment.h"
#include "artifact/ImmutableArtifactCache.h"
#include "artifact/ImmutableArtifactWriter.h"
#include "artifact/RowLocatorSidecar.h"
#include "resource/TaskObjectReadBudget.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "capability/NativeCapabilityCatalog.h"
#include "jni/RuntimeInitializer.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "split/SplitConfig.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/plan/GeneratedValuesOverlayOperator.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

namespace proto = pangrova::protocol::v2;

class TestDirectory final {
 public:
  TestDirectory() {
    const auto pattern = (std::filesystem::temp_directory_path() /
                          "pangrova-generated-overlay-XXXXXX").string();
    std::vector<char> name(pattern.begin(), pattern.end());
    name.push_back('\0');
    const auto* created = ::mkdtemp(name.data());
    if (created == nullptr) {
      throw std::runtime_error("Cannot reserve exclusive generated overlay test directory");
    }
    path_ = created;
  }
  ~TestDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

class ArtifactCacheGuard final {
 public:
  explicit ArtifactCacheGuard(const std::filesystem::path& root) {
    if (!artifact::ImmutableArtifactCache::instance().enabled()) {
      artifact::ImmutableArtifactCache::instance().configure(root.string(),
                                                             8 * 1024 * 1024);
      owns_cache_ = true;
    }
  }
  ~ArtifactCacheGuard() {
    if (owns_cache_) {
      artifact::ImmutableArtifactCache::instance().shutdown();
    }
  }

 private:
  bool owns_cache_{false};
};

void addStringColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
}

void addVectorColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* vector = schema->mutable_struct_()->add_types()->mutable_list();
  vector->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  vector->mutable_type()->mutable_fp32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
}

void addFieldArgument(proto::CapabilityInvocation* invocation,
                      const std::string& name, int32_t field) {
  auto* argument = invocation->add_arguments();
  argument->set_name(name);
  argument->mutable_expression()->mutable_selection()->mutable_root_reference();
  argument->mutable_expression()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(field);
}

std::vector<uint8_t> overlayPlan() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  auto* extension = root->mutable_input()->mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  auto* input_schema = read->mutable_base_schema();
  addStringColumn(input_schema, std::string(kSourceObjectIdColumnName));
  addI64Column(input_schema, std::string(kPhysicalRowPositionColumnName));
  addStringColumn(input_schema, "description");

  proto::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_table_function();
  detail->set_plan_node_id("generated-values-overlay-1");
  *detail->mutable_invocation()->mutable_capability() =
      capability::generatedValuesOverlayDescriptor().ref();
  addFieldArgument(detail->mutable_invocation(), "source_object_id", 0);
  addFieldArgument(detail->mutable_invocation(), "row_position", 1);
  auto* output_schema = detail->mutable_output_schema();
  addStringColumn(output_schema, std::string(kSourceObjectIdColumnName));
  addI64Column(output_schema, std::string(kPhysicalRowPositionColumnName));
  addStringColumn(output_schema, "description");
  addVectorColumn(output_schema, "embedding");
  *root->mutable_names() = output_schema->names();
  extension->mutable_detail()->PackFrom(envelope);

  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return {bytes.begin(), bytes.end()};
}

void addStringProperty(
    google::protobuf::RepeatedPtrField<proto::NamedValue>* properties,
    const std::string& name, const std::string& value) {
  auto* property = properties->Add();
  property->set_name(name);
  property->mutable_value()->set_string_value(value);
}

proto::ArtifactObject artifactObject(
    const artifact::ImmutableArtifactObject& object, const std::string& role,
    uint32_t ordinal) {
  proto::ArtifactObject result;
  result.set_object_id("generated-column-1:9:object-1:" + role);
  result.set_opaque_uri(object.uri);
  result.set_size_bytes(object.size_bytes);
  result.mutable_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  result.mutable_checksum()->set_digest(object.sha256);
  result.set_object_role(role);
  result.set_source_object_id("object-1");
  result.set_row_count(2);
  result.set_ordinal(ordinal);
  result.set_scope(proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
  return result;
}

proto::OperatorBinding overlayBinding(
    const std::filesystem::path& root,
    const std::vector<uint8_t>& validity = std::vector<uint8_t>{1, 1}) {
  const auto values_path = (root / "values.segment").string();
  const auto locators_path = (root / "values.locators").string();
  const std::vector<float> generated_values{
      validity[0] ? 1.0F : 0.0F, validity[0] ? 2.0F : 0.0F,
      validity[1] ? 3.0F : 0.0F, validity[1] ? 4.0F : 0.0F};
  artifact::GeneratedValueSegment::write(values_path, generated_values,
                                         validity, 2, 2);
  artifact::RowLocatorSidecar::writePhysicalRowPositions(
      locators_path, std::vector<uint64_t>{3, 8});
  const auto values = artifact::ImmutableArtifactWriter::putFile(
      values_path, (root / "published").string(), "GENERATED_VALUES");
  const auto locators = artifact::ImmutableArtifactWriter::putFile(
      locators_path, (root / "published").string(), "ROW_LOCATORS");

  proto::OperatorBinding binding;
  binding.set_plan_node_id("generated-values-overlay-1");
  *binding.mutable_capability() =
      capability::generatedValuesOverlayDescriptor().ref();
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  auto* resources = binding.mutable_resources();
  resources->set_fill_keys(2);
  resources->set_object_store_read_bytes(values.size_bytes +
                                         locators.size_bytes);
  auto* artifact_binding = binding.add_artifacts();
  artifact_binding->set_artifact_id("generated-column-1");
  artifact_binding->set_artifact_type("GENERATED_COLUMN_VALUES");
  artifact_binding->set_generation("9");
  artifact_binding->set_source_id("catalog.schema.items");
  artifact_binding->set_source_snapshot("snapshot-9");
  artifact_binding->set_source_data_timestamp_ms(1);
  artifact_binding->set_artifact_created_at_epoch_ms(2);
  artifact_binding->set_manifest_uri((root / "manifest.pb").string());
  artifact_binding->mutable_manifest_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  artifact_binding->mutable_manifest_checksum()->set_digest(
      std::string(32, '\x11'));
  *artifact_binding->mutable_format() =
      capability::generatedValuesArtifactFormat();
  *artifact_binding->mutable_row_locator() =
      capability::physicalRowLocatorContract();
  addStringProperty(artifact_binding->mutable_properties(), "target_column",
                    "embedding");
  addStringProperty(artifact_binding->mutable_properties(), "output_type",
                    "VECTOR(2)");
  auto* dimension = artifact_binding->add_properties();
  dimension->set_name("vector_dimension");
  dimension->mutable_value()->set_int64_value(2);
  addStringProperty(artifact_binding->mutable_properties(), "source_encoding",
                    "ordered-unit-separator-v1");
  addStringProperty(artifact_binding->mutable_properties(), "execution_mode",
                    "EXECUTION_MODE_ASYNC");
  addStringProperty(artifact_binding->mutable_properties(), "source_column.0",
                    "description");
  *artifact_binding->add_split_objects() =
      artifactObject(values, "GENERATED_VALUES", 0);
  *artifact_binding->add_split_objects() =
      artifactObject(locators, "ROW_LOCATORS", 1);
  return binding;
}

class GeneratedValuesOverlayExecutionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("GeneratedValuesOverlayExecutionTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    GeneratedValuesOverlayExecutionTest::pool_;

std::shared_ptr<ArtifactBuildTaskContext> readContext(const proto::OperatorBinding& binding) {
  auto context = std::make_shared<ArtifactBuildTaskContext>();
  context->max_object_store_read_bytes = binding.resources().object_store_read_bytes();
  context->read_budget = std::make_shared<resource::TaskObjectReadBudget>(
      context->max_object_store_read_bytes);
  return context;
}

TEST_F(GeneratedValuesOverlayExecutionTest,
       ConvertsMaterializesAndExecutesExactOverlay) {
  for (const auto& validity :
       {std::vector<uint8_t>{1, 1}, std::vector<uint8_t>{0, 1},
        std::vector<uint8_t>{0, 0}}) {
    TestDirectory directory;
    ArtifactCacheGuard cache(directory.path() / "cache");
    auto binding = overlayBinding(directory.path(), validity);
    split::SplitConfig split;
    split.source_snapshot = "snapshot-9";
    split.data_object_id = "object-1";
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto converted = converter.convert(overlayPlan(), {split}, false, "ArrowBatch",
                                       {{binding.plan_node_id(), binding}}, readContext(binding));
    auto overlay =
        std::dynamic_pointer_cast<const GeneratedValuesOverlayNode>(converted);
    ASSERT_NE(overlay, nullptr);
    ASSERT_EQ(overlay->config()->sources.size(), 1);
    EXPECT_EQ(overlay->config()->sources.at("object-1").dimension, 2);
    EXPECT_EQ(overlay->config()->sources.at("object-1").validity, validity);

    facebook::velox::test::VectorMaker maker(pool_.get());
    auto input = maker.rowVector(
        {std::string(kSourceObjectIdColumnName),
         std::string(kPhysicalRowPositionColumnName), "description"},
        {maker.flatVector<std::string>({"object-1", "object-1"}),
         maker.flatVector<int64_t>({8, 3}),
         maker.flatVector<std::string>({"second", "first"})});
    auto values = std::make_shared<facebook::velox::core::ValuesNode>(
        "overlay-values", std::vector<facebook::velox::RowVectorPtr>{input},
        true);
    auto executable = std::make_shared<GeneratedValuesOverlayNode>(
        "generated-values-overlay-exec", overlay->config(),
        overlay->outputType(), values);
    EXPECT_EQ(executable->config().get(), overlay->config().get());

    std::mutex output_mutex;
    std::vector<facebook::velox::RowVectorPtr> outputs;
    auto query_context = facebook::velox::core::QueryCtx::create(
        pangrova::jni::globalExecutor().get(),
        facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
            converter_registry));
    auto task = facebook::velox::exec::Task::create(
        "generated-values-overlay-execution-test",
        facebook::velox::core::PlanFragment{executable}, 0,
        std::move(query_context),
        facebook::velox::exec::Task::ExecutionMode::kParallel,
        [&](facebook::velox::RowVectorPtr output, bool,
            facebook::velox::ContinueFuture*) {
          if (output) {
            std::lock_guard<std::mutex> lock(output_mutex);
            outputs.push_back(std::move(output));
          }
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        });
    // A parallelizable ValuesNode is replicated once per driver. Use one driver
    // so the fixture models one physical source scan rather than duplicate
    // input.
    task->start(1);
    task->taskCompletionFuture().wait();
    ASSERT_FALSE(task->error()) << task->errorMessage();
    ASSERT_EQ(outputs.size(), 1);
    ASSERT_EQ(outputs.front()->size(), 2);
    {
      auto* arrays =
          outputs.front()->childAt(3)->as<facebook::velox::ArrayVector>();
      ASSERT_NE(arrays, nullptr);
      facebook::velox::DecodedVector elements(*arrays->elements());
      EXPECT_EQ(arrays->isNullAt(0), validity[1] == 0);
      EXPECT_EQ(arrays->isNullAt(1), validity[0] == 0);
      if (validity[1]) {
        EXPECT_FLOAT_EQ(elements.valueAt<float>(arrays->offsetAt(0)), 3.0F);
        EXPECT_FLOAT_EQ(elements.valueAt<float>(arrays->offsetAt(0) + 1), 4.0F);
      }
      if (validity[0]) {
        EXPECT_FLOAT_EQ(elements.valueAt<float>(arrays->offsetAt(1)), 1.0F);
        EXPECT_FLOAT_EQ(elements.valueAt<float>(arrays->offsetAt(1) + 1), 2.0F);
      }
    }
    // Output vectors use the task's query pool. Release all consumers before
    // Task destroys that pool, matching the production output-drain ordering.
    outputs.clear();
  }
}

TEST_F(GeneratedValuesOverlayExecutionTest,
       RejectsRowCountCapabilityAndOptionDrift) {
  TestDirectory directory;
  ArtifactCacheGuard cache(directory.path() / "cache");
  auto binding = overlayBinding(directory.path());
  binding.mutable_artifacts(0)->mutable_split_objects(1)->set_row_count(3);
  const auto row_count_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter row_count_converter(row_count_converter_registry);
  EXPECT_THROW(row_count_converter.convert(overlayPlan(), {}, false, "ArrowBatch",
                                           {{binding.plan_node_id(), binding}}, readContext(binding)),
               std::runtime_error);

  substrait::Plan plan;
  auto bytes = overlayPlan();
  ASSERT_TRUE(plan.ParseFromArray(bytes.data(), bytes.size()));
  proto::RelationDetailEnvelope envelope;
  auto* extension = plan.mutable_relations(0)
                        ->mutable_root()
                        ->mutable_input()
                        ->mutable_extension_single();
  ASSERT_TRUE(extension->detail().UnpackTo(&envelope));
  auto* option =
      envelope.mutable_table_function()->mutable_invocation()->add_options();
  option->set_name("missing_value");
  option->mutable_value()->set_string_value("empty");
  extension->mutable_detail()->PackFrom(envelope);
  auto option_bytes_string = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  std::vector<uint8_t> option_bytes(option_bytes_string.begin(),
                                    option_bytes_string.end());
  auto valid_binding = overlayBinding(directory.path());
  const auto option_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter option_converter(option_converter_registry);
  EXPECT_THROW(
      option_converter.convert(option_bytes, {}, false, "ArrowBatch",
                               {{valid_binding.plan_node_id(), valid_binding}}, readContext(valid_binding)),
      std::runtime_error);

  auto wrong_capability = valid_binding;
  wrong_capability.mutable_capability()->set_signature_sha256(
      std::string(32, '\0'));
  const auto capability_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter capability_converter(capability_converter_registry);
  EXPECT_THROW(capability_converter.convert(
                   overlayPlan(), {}, false, "ArrowBatch",
                   {{wrong_capability.plan_node_id(), wrong_capability}}, readContext(wrong_capability)),
               std::runtime_error);
}

TEST_F(GeneratedValuesOverlayExecutionTest,
       FailsClosedForMissingAndDuplicatePhysicalRows) {
  TestDirectory directory;
  ArtifactCacheGuard cache(directory.path() / "cache");
  auto binding = overlayBinding(directory.path());
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto converted = converter.convert(overlayPlan(), {}, false, "ArrowBatch",
                                     {{binding.plan_node_id(), binding}}, readContext(binding));
  auto overlay =
      std::dynamic_pointer_cast<const GeneratedValuesOverlayNode>(converted);
  ASSERT_NE(overlay, nullptr);

  const auto execute = [&](const std::string& task_id,
                           const std::vector<int64_t>& positions) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto input = maker.rowVector(
        {std::string(kSourceObjectIdColumnName),
         std::string(kPhysicalRowPositionColumnName), "description"},
        {maker.flatVector<std::string>(
             std::vector<std::string>(positions.size(), "object-1")),
         maker.flatVector<int64_t>(positions),
         maker.flatVector<std::string>(
             std::vector<std::string>(positions.size(), "text"))});
    auto values = std::make_shared<facebook::velox::core::ValuesNode>(
        task_id + "-values", std::vector<facebook::velox::RowVectorPtr>{input},
        true);
    auto executable = std::make_shared<GeneratedValuesOverlayNode>(
        task_id + "-overlay", overlay->config(), overlay->outputType(), values);
    auto query_context = facebook::velox::core::QueryCtx::create(
        pangrova::jni::globalExecutor().get(),
        facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
            converter_registry));
    auto task = facebook::velox::exec::Task::create(
        task_id, facebook::velox::core::PlanFragment{executable}, 0,
        std::move(query_context),
        facebook::velox::exec::Task::ExecutionMode::kParallel,
        [](facebook::velox::RowVectorPtr, bool,
           facebook::velox::ContinueFuture*) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        });
    task->start(1);
    task->taskCompletionFuture().wait();
    return task->errorMessage();
  };

  EXPECT_NE(execute("generated-values-overlay-missing", {3, 99})
                .find("missing a physical row position"),
            std::string::npos);
  EXPECT_NE(execute("generated-values-overlay-duplicate", {3, 3})
                .find("duplicate source row"),
            std::string::npos);
}

}  // namespace
TEST_F(GeneratedValuesOverlayExecutionTest, MissingOrOversizedReadOwnerRejectsBeforeMaterialization) {
  TestDirectory directory;
  ArtifactCacheGuard cache(directory.path() / "cache");
  auto binding = overlayBinding(directory.path());
  auto registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter absent(registry);
  EXPECT_THROW(absent.convert(overlayPlan(), {}, false, "ArrowBatch",
                              {{binding.plan_node_id(), binding}}), std::runtime_error);
  auto context = readContext(binding);
  context->max_object_store_read_bytes = binding.resources().object_store_read_bytes() - 1;
  VeloxPlanConverter oversized(registry);
  EXPECT_THROW(oversized.convert(overlayPlan(), {}, false, "ArrowBatch",
                                 {{binding.plan_node_id(), binding}}, context), std::runtime_error);
  EXPECT_EQ(context->read_budget->requestedBytes(), 0u);
  EXPECT_EQ(context->read_budget->activeReads(), 0u);
  EXPECT_EQ(artifact::ImmutableArtifactCache::instance().usedBytes(), 0u);
  context->read_budget->stopAndDrain();
}

}  // namespace pangrova::velox::test
