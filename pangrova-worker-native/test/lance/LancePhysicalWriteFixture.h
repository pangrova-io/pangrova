#pragma once

#include "TestMemoryHelper.h"
#include "LanceTestDataset.h"
#include "lance/LancePhysicalBatchWriter.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>
#include <openssl/evp.h>
#include <unistd.h>

#include <array>
#include <bit>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::lance::test {
namespace proto = protocol::v2;
using namespace facebook::velox;

// Real local Native FileWriter/conditional-create component fixture. Metadata
// is a canonical test allocation, not evidence of SDK catalog/public SQL wiring.
class LancePhysicalWriteFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("physical-file-writer");
    directory_ = std::filesystem::temp_directory_path() /
        ("pangrova-physical-write-" + std::to_string(::getpid()));
    std::filesystem::create_directories(directory_ / "stage");
    std::filesystem::create_directories(directory_ / "bucket-0");
    output_type_ = ROW({"c0", "c1"}, {BIGINT(), ARRAY(DOUBLE())});
  }
  void TearDown() override {
    pool_.reset();
    std::filesystem::remove_all(directory_);
  }

  static std::string bytes(const google::protobuf::Message& message) {
    std::string value(message.ByteSizeLong(), '\0');
    google::protobuf::io::ArrayOutputStream output(value.data(), static_cast<int>(value.size()));
    google::protobuf::io::CodedOutputStream coded(&output);
    coded.SetSerializationDeterministic(true);
    if (!message.SerializeToCodedStream(&coded) || coded.HadError()) {
      throw std::runtime_error("Cannot serialize physical component allocation");
    }
    return value;
  }
  static proto::Checksum checksum(const std::string& value) {
    std::array<unsigned char, 32> digest{};
    unsigned int size = 0;
    if (EVP_Digest(value.data(), value.size(), digest.data(), &size, EVP_sha256(), nullptr) != 1 || size != 32) {
      throw std::runtime_error("Cannot hash physical component allocation");
    }
    proto::Checksum result;
    result.set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    result.set_digest(digest.data(), digest.size());
    return result;
  }
  static std::string hex(const std::string& value) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    for (unsigned char byte : value) {
      result.push_back(digits[byte >> 4]);
      result.push_back(digits[byte & 15]);
    }
    return result;
  }

  static std::string actualFileBytes(const std::string& uri) {
    std::ifstream stream(uri, std::ios::binary);
    if (!stream) throw std::runtime_error("Cannot open actual physical file");
    std::string bytes((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    if (stream.bad()) throw std::runtime_error("Cannot read actual physical file");
    return bytes;
  }

  static std::string schemaBytes(int32_t dimension = 3) {
    auto id = arrow::field("id", arrow::int64(), false,
        arrow::key_value_metadata({"PARQUET:field_id"}, {"0"}));
    auto embedding = arrow::field("embedding", arrow::fixed_size_list(
        arrow::field("element", arrow::float64(), true), dimension), true,
        arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}));
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, arrow::schema({id, embedding})).ValueOrDie();
    auto status = writer->Close();
    if (!status.ok()) throw std::runtime_error(status.ToString());
    return sink->Finish().ValueOrDie()->ToString();
  }

  PhysicalWriterConfig config(int32_t dimension = 3) const {
    proto::TaskAttemptId attempt;
    attempt.set_query_or_job_id("physical-component");
    attempt.set_stage_id(1);
    attempt.set_partition_id(0);
    attempt.set_plan_epoch(1);
    attempt.set_fencing_token(1);
    attempt.set_control_epoch(1);
    attempt.set_task_id("task-0");
    auto* owner = attempt.mutable_execution_owner();
    owner->set_owner_id("component-owner");
    owner->set_owner_incarnation("component-incarnation");
    owner->set_fencing_token(1);
    const auto attempt_bytes = bytes(attempt);
    const auto suffix = "attempt-" + hex(checksum(attempt_bytes).digest());
    PhysicalWriterConfig config;
    config.taskAttempt.assign(attempt_bytes.begin(), attempt_bytes.end());
    config.tenantId = "tenant-a";
    config.spillDirectory = (directory_ / "stage").string();
    config.maxSpillBytes = 16 * 1024 * 1024;
    config.maxObjectWriteBytes = 16 * 1024 * 1024;
    config.storageBinding = pangrova::test::testStorageBinding((directory_ / "data.lance").string());
    auto& binding = config.binding;
    *binding.mutable_catalog() = pangrova::test::testDatasetCatalog();
    binding.set_write_id("physical-component");
    binding.set_generation("generation-1");
    binding.set_base_snapshot_id(-1);
    binding.set_schema_id(0);
    binding.set_file_format(proto::FILE_FORMAT_LANCE);
    binding.set_physical_schema(schemaBytes(dimension));
    *binding.mutable_physical_schema_checksum() = checksum(binding.physical_schema());
    addColumns(binding, dimension);
    auto* policy = binding.mutable_policy();
    policy->set_write_mode("INSERT");
    policy->set_bucket_count(1);
    policy->set_writer_buffer_hint_bytes(65536);
    (*policy->mutable_options())["file.format"] = "lance";
    *policy->mutable_declaration_checksum() = checksum(bytes(*policy));
    binding.set_allocation_prefix((directory_ / "bucket-0").string());
    binding.set_attempt_prefix(binding.allocation_prefix() + "/" + suffix);
    auto* file = binding.add_files();
    file->set_file_name("data." + suffix + ".lance");
    file->set_opaque_location(binding.allocation_prefix() + "/" + file->file_name());
    file->set_restored_max_sequence(-1);
    file->set_file_source("APPEND");
    *file->mutable_restored_file_set_checksum() = checksum("");
    *file->mutable_task_attempt() = attempt;
    *file->mutable_allocation_checksum() = checksum(bytes(*file));
    return config;
  }

  static void addColumns(proto::PhysicalFileWriteBinding& binding, int32_t dimension) {
    auto* id = binding.add_columns();
    id->set_field_id(0);
    id->set_name("id");
    id->mutable_logical_type()->set_canonical_sql("BIGINT");
    id->mutable_logical_type()->mutable_substrait_type()->mutable_i64()->set_nullability(
        substrait::Type::NULLABILITY_REQUIRED);
    auto* embedding = binding.add_columns();
    embedding->set_field_id(1);
    embedding->set_name("embedding");
    embedding->set_nullable(true);
    auto* logical = embedding->mutable_logical_type();
    logical->set_canonical_sql("VECTOR(" + std::to_string(dimension) + ",DOUBLE)");
    auto* list = logical->mutable_substrait_type()->mutable_list();
    list->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    list->mutable_type()->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    auto* dimension_constraint = logical->add_constraints();
    dimension_constraint->set_name("dimension");
    dimension_constraint->set_integer_value(dimension);
    auto* element = logical->add_constraints();
    element->set_name("element_type");
    element->set_string_value("FLOAT64");
  }

  RowVectorPtr batch() const {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto ids = maker.flatVector<int64_t>({7, 9, 11});
    auto vectors = maker.arrayVectorNullable<double>({
        std::vector<std::optional<double>>{1.0000000001, std::nullopt, -4.0},
        std::nullopt, std::vector<std::optional<double>>{3.0, 4.0, 5.0}});
    return std::make_shared<RowVector>(pool_.get(), output_type_, nullptr, 3,
        std::vector<VectorPtr>{ids, vectors});
  }

  RowVectorPtr largeBatch(int32_t dimension) const {
    constexpr vector_size_t kRows = 8;
    facebook::velox::test::VectorMaker maker(pool_.get());
    std::mt19937_64 random(42);
    auto ids = maker.flatVector<int64_t>(kRows, [](vector_size_t row) { return row; });
    auto vectors = maker.arrayVector<double>(kRows,
        [dimension](vector_size_t) { return dimension; },
        [&random](vector_size_t) {
          // Deterministic distinct mantissas prevent a tiny constant-value file.
          return std::bit_cast<double>(0x3ff0000000000000ULL |
              (random() & ((1ULL << 52) - 1)));
        });
    return std::make_shared<RowVector>(pool_.get(), output_type_, nullptr, kRows,
        std::vector<VectorPtr>{ids, vectors});
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::filesystem::path directory_;
  RowTypePtr output_type_;
};

}  // namespace pangrova::lance::test
