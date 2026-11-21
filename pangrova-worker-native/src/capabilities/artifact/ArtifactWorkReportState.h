// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <mutex>
#include <optional>

#include "pangrova/protocol/v2/result.pb.h"

namespace pangrova::artifact {

/**
 * Task-scoped, write-once publication channel from a Native background sink
 * to TaskReport serialization. It carries metadata only; artifact bytes are
 * written directly by Native to immutable object storage.
 */
class ArtifactWorkReportState final {
 public:
  void publish(const protocol::v2::ArtifactWorkReport& report);
  std::optional<protocol::v2::ArtifactWorkReport> snapshot() const;

 private:
  mutable std::mutex mutex_;
  std::optional<protocol::v2::ArtifactWorkReport> report_;
};

}  // namespace pangrova::artifact
