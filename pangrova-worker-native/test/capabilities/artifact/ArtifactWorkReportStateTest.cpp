#include <gtest/gtest.h>

#include <openssl/evp.h>

#include <array>

#include "capabilities/artifact/ArtifactWorkReportState.h"

namespace pangrova::artifact::test {
namespace {

std::string sha256(std::string_view input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(input.data(), input.size(), digest.data(), &size,
                       EVP_sha256(), nullptr), 1);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

protocol::v2::ArtifactWorkReport validReport(std::string page = "page-1") {
  protocol::v2::ArtifactWorkReport report;
  report.set_job_id("job-1");
  report.set_page_id(std::move(page));
  report.set_fencing_token(7);
  report.set_source_id("catalog.schema.items");
  report.set_source_snapshot("snapshot-1");
  report.set_staging_generation("generation-1");
  auto* output = report.add_outputs();
  output->set_object_id("object-1");
  output->set_opaque_uri("s3://bucket/staging/object-1");
  output->set_object_role("GRAPH");
  output->set_source_object_id("source-1");
  output->set_scope(protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
  output->set_size_bytes(123);
  output->set_row_count(10);
  output->mutable_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  output->mutable_checksum()->set_digest(std::string(32, 'a'));
  report.mutable_report_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  auto unsigned_report = report;
  unsigned_report.clear_report_checksum();
  report.mutable_report_checksum()->set_digest(
      sha256(unsigned_report.SerializeAsString()));
  return report;
}

TEST(ArtifactWorkReportStateTest, PublishesExactlyOneImmutableReport) {
  ArtifactWorkReportState state;
  const auto report = validReport();
  state.publish(report);
  state.publish(report);
  ASSERT_TRUE(state.snapshot().has_value());
  EXPECT_EQ(state.snapshot()->SerializeAsString(), report.SerializeAsString());
  EXPECT_THROW(state.publish(validReport("page-2")), std::logic_error);
}

TEST(ArtifactWorkReportStateTest, AcceptsMaterializedZeroRowOutput) {
  ArtifactWorkReportState state;
  auto report = validReport();
  report.mutable_outputs(0)->set_row_count(0);
  report.clear_report_checksum();
  report.mutable_report_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  auto unsigned_report = report;
  unsigned_report.clear_report_checksum();
  report.mutable_report_checksum()->set_digest(
      sha256(unsigned_report.SerializeAsString()));

  state.publish(report);

  ASSERT_TRUE(state.snapshot().has_value());
  EXPECT_EQ(state.snapshot()->outputs(0).row_count(), 0);
  EXPECT_GT(state.snapshot()->outputs(0).size_bytes(), 0);
}

TEST(ArtifactWorkReportStateTest, RejectsIncompleteOutputMetadata) {
  ArtifactWorkReportState state;
  auto report = validReport();
  report.mutable_outputs(0)->clear_checksum();
  EXPECT_THROW(state.publish(report), std::invalid_argument);
  EXPECT_FALSE(state.snapshot().has_value());
}

TEST(ArtifactWorkReportStateTest, RejectsForgedReportChecksum) {
  ArtifactWorkReportState state;
  auto report = validReport();
  report.set_page_id("tampered-after-signing");
  EXPECT_THROW(state.publish(report), std::invalid_argument);
  EXPECT_FALSE(state.snapshot().has_value());
}

}  // namespace
}  // namespace pangrova::artifact::test
