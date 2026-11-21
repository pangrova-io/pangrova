#include <iostream>

#include "velox/expression/FunctionRegistryIdentity.h"

// Link to the exact archive supplied to record. The build writer binds its
// actual SHA; the SDK digest is read from that archive's out-of-line object.
#ifndef PANGROVA_INSPECTED_ARCHIVE_SHA256
#error "The inspected archive SHA256 must be supplied by the actual build writer"
#endif

int main() {
  std::cout << facebook::velox::exec::functionContractGenerationSha256() << '\n'
            << facebook::velox::exec::functionContractSdkPatchSha256() << '\n'
            << PANGROVA_INSPECTED_ARCHIVE_SHA256 << '\n';
}
