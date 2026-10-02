// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <chrono>
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
    path_ = std::filesystem::temp_directory_path() /
            ("pangrova-generated-overlay-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(path_);
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
  schema->mutable_struct_()->add_types()->mutable_string();
}

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64();
}

void addVectorColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()
      ->add_types()
      ->mutable_list()
      ->mutable_type()
      ->mutable_fp32();
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

  const auto bytes = plan.SerializeAsString();
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
                    "VECTOR(2, COSINE)");
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
    VeloxPlanConverter converter;
    auto converted = converter.convert(overlayPlan(), {split}, false, "Presto",
                                       {{binding.plan_node_id(), binding}});
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
        pangrova::jni::globalExecutor().get());
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
  VeloxPlanConverter row_count_converter;
  EXPECT_THROW(row_count_converter.convert(overlayPlan(), {}, false, "Presto",
                                           {{binding.plan_node_id(), binding}}),
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
  auto option_bytes_string = plan.SerializeAsString();
  std::vector<uint8_t> option_bytes(option_bytes_string.begin(),
                                    option_bytes_string.end());
  auto valid_binding = overlayBinding(directory.path());
  VeloxPlanConverter option_converter;
  EXPECT_THROW(
      option_converter.convert(option_bytes, {}, false, "Presto",
                               {{valid_binding.plan_node_id(), valid_binding}}),
      std::runtime_error);

  auto wrong_capability = valid_binding;
  wrong_capability.mutable_capability()->set_signature_sha256(
      std::string(32, '\0'));
  VeloxPlanConverter capability_converter;
  EXPECT_THROW(capability_converter.convert(
                   overlayPlan(), {}, false, "Presto",
                   {{wrong_capability.plan_node_id(), wrong_capability}}),
               std::runtime_error);
}

TEST_F(GeneratedValuesOverlayExecutionTest,
       FailsClosedForMissingAndDuplicatePhysicalRows) {
  TestDirectory directory;
  ArtifactCacheGuard cache(directory.path() / "cache");
  auto binding = overlayBinding(directory.path());
  VeloxPlanConverter converter;
  auto converted = converter.convert(overlayPlan(), {}, false, "Presto",
                                     {{binding.plan_node_id(), binding}});
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
        pangrova::jni::globalExecutor().get());
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
}  // namespace pangrova::velox::test
