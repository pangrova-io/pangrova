// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "artifact/ArtifactWorkReportState.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <array>
#include <stdexcept>

namespace pangrova::artifact {
namespace {

std::string sha256(std::string_view input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  if (EVP_Digest(input.data(), input.size(), digest.data(), &size,
                 EVP_sha256(), nullptr) != 1 || size != 32) {
    throw std::runtime_error("Failed to compute ArtifactWorkReport SHA-256");
  }
  return {reinterpret_cast<const char*>(digest.data()), size};
}

}  // namespace

void ArtifactWorkReportState::publish(
    const protocol::v2::ArtifactWorkReport& report) {
  if (report.job_id().empty() || report.page_id().empty() ||
      report.fencing_token() == 0 || report.source_id().empty() ||
      report.source_snapshot().empty() ||
      report.staging_generation().empty() || report.outputs().empty() ||
      !report.has_report_checksum() ||
      report.report_checksum().algorithm() !=
          protocol::v2::Checksum::ALGORITHM_SHA256 ||
      report.report_checksum().digest().size() != 32) {
    throw std::invalid_argument(
        "ArtifactWorkReport is incomplete or missing its SHA-256 checksum");
  }
  auto unsigned_report = report;
  const auto claimed_checksum = unsigned_report.report_checksum().digest();
  unsigned_report.clear_report_checksum();
  const auto actual_checksum = sha256(unsigned_report.SerializeAsString());
  if (CRYPTO_memcmp(
          claimed_checksum.data(), actual_checksum.data(),
          actual_checksum.size()) != 0) {
    throw std::invalid_argument("ArtifactWorkReport SHA-256 checksum mismatch");
  }
  for (const auto& output : report.outputs()) {
    if (output.object_id().empty() || output.opaque_uri().empty() ||
        output.object_role().empty() || output.source_object_id().empty() ||
        output.scope() ==
            protocol::v2::ARTIFACT_OBJECT_SCOPE_UNSPECIFIED ||
        output.size_bytes() == 0 ||
        !output.has_checksum() ||
        output.checksum().algorithm() !=
            protocol::v2::Checksum::ALGORITHM_SHA256 ||
        output.checksum().digest().size() != 32) {
      throw std::invalid_argument("ArtifactWorkReport contains an invalid output");
    }
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (report_.has_value()) {
    if (report_->SerializeAsString() == report.SerializeAsString()) {
      return;
    }
    throw std::logic_error(
        "ArtifactWorkReportState rejects a conflicting second publication");
  }
  report_ = report;
}

std::optional<protocol::v2::ArtifactWorkReport>
ArtifactWorkReportState::snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return report_;
}

}  // namespace pangrova::artifact
