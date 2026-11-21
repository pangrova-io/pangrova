// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <string>

namespace pangrova::artifact {

struct ImmutableArtifactObject {
  std::string uri;
  std::string sha256;
  uint64_t size_bytes{0};
};

/** Writes a local build output as a content-addressed immutable object. */
class ImmutableArtifactWriter final {
 public:
  static ImmutableArtifactObject putFile(
      const std::string& local_path,
      const std::string& staging_uri,
      const std::string& object_role);
};

}  // namespace pangrova::artifact
