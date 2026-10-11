#include <gtest/gtest.h>

#include "capability/BuiltinFunctionRegistration.h"
#include "capability/FunctionContractEncoding.h"
#include "capability/RegisteredFunctionCatalog.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionCallToSpecialForm.h"

#include <set>
#include <stdexcept>

namespace pangrova::capability::test {
namespace {
using namespace pangrova::protocol::v2;
using Registry = facebook::velox::exec::FunctionRegistrySnapshot;

std::shared_ptr<const Registry> actualRegistry() {
  registerBuiltinFunctions();
  return Registry::capture();
}

TEST(RegisteredFunctionCatalogTest, EveryActualNameAndTraversalEntryHasOneTypedOwnerContract) {
  const auto owner = actualRegistry();
  FunctionCatalogManifest manifest;
  ASSERT_NO_THROW(appendRegisteredFunctionCatalog(*owner, manifest));
  EXPECT_EQ(manifest.registered_functions_size(),
      owner->scalars().size() + owner->aggregates().size() + owner->windows().size());
  std::set<std::pair<FunctionKind, std::string>> identities;
  for (auto function : manifest.registered_functions()) {
    SCOPED_TRACE(function.implementation_name());
    EXPECT_TRUE(identities.emplace(function.kind(), function.implementation_name()).second);
    const auto digest = function.contract_sha256();
    function.clear_contract_sha256();
    EXPECT_EQ(digest, functionContractDigest(function));
    if (function.kind() != FUNCTION_KIND_SCALAR) continue;
    const auto& actual = owner->scalars().at(function.implementation_name());
    ASSERT_EQ(function.implementations_size(), actual.size());
    EXPECT_EQ(function.signatures_size(), 0);
    for (int ordinal = 0; ordinal < function.implementations_size(); ++ordinal) {
      auto implementation = function.implementations(ordinal);
      EXPECT_EQ(implementation.traversal_ordinal(), ordinal);
      const auto implementationDigest = implementation.contract_sha256();
      implementation.clear_contract_sha256();
      EXPECT_EQ(implementationDigest, functionContractDigest(implementation));
      if (actual[ordinal].special) {
        EXPECT_EQ(implementation.special_type_program(), actual[ordinal].special->typeProgram());
        EXPECT_EQ(implementation.vector_companion(), actual[ordinal].special->vectorCompanion());
      } else if (actual[ordinal].vector) {
        ASSERT_TRUE(implementation.metadata().has_deterministic());
        ASSERT_TRUE(implementation.metadata().has_default_null_behavior());
        EXPECT_EQ(implementation.metadata().deterministic(), actual[ordinal].vector->metadata.deterministic);
        EXPECT_EQ(implementation.metadata().default_null_behavior(), actual[ordinal].vector->metadata.defaultNullBehavior);
      } else {
        ASSERT_TRUE(implementation.metadata().has_deterministic());
        ASSERT_TRUE(implementation.metadata().has_default_null_behavior());
        EXPECT_EQ(implementation.metadata().default_null_behavior(), actual[ordinal].simple->getMetadata().defaultNullBehavior());
        EXPECT_EQ(implementation.priority(), actual[ordinal].simple->getMetadata().priority());
        EXPECT_EQ(implementation.argument_physical_size(), actual[ordinal].simple->getMetadata().argPhysicalTypes().size());
      }
    }
  }
}

TEST(RegisteredFunctionCatalogTest, ActualSignaturelessRowCompanionIsNotInventedAsCallableOverload) {
  const auto owner = actualRegistry();
  FunctionCatalogManifest manifest;
  appendRegisteredFunctionCatalog(*owner, manifest);
  const auto actual = owner->vectorCompanion("row_constructor");
  ASSERT_TRUE(actual);
  ASSERT_TRUE(actual->signatures.empty());
  bool found = false;
  for (auto companion : manifest.vector_companions()) {
    SCOPED_TRACE(companion.implementation_name());
    const auto entry = owner->vectorCompanion(companion.implementation_name());
    ASSERT_TRUE(entry);
    EXPECT_TRUE(entry->signatures.empty());
    ASSERT_TRUE(companion.metadata().has_deterministic());
    ASSERT_TRUE(companion.metadata().has_default_null_behavior());
    EXPECT_EQ(companion.metadata().deterministic(), entry->metadata.deterministic);
    EXPECT_EQ(companion.metadata().default_null_behavior(), entry->metadata.defaultNullBehavior);
    const auto digest = companion.contract_sha256();
    companion.clear_contract_sha256();
    EXPECT_EQ(digest, functionContractDigest(companion));
    found |= companion.implementation_name() == "row_constructor";
  }
  EXPECT_TRUE(found);
}

TEST(RegisteredFunctionCatalogTest, ReprojectionOfSameOwnedRegistryIsByteExactAndCannotAppendTwice) {
  const auto owner = actualRegistry();
  FunctionCatalogManifest first, second;
  appendRegisteredFunctionCatalog(*owner, first);
  appendRegisteredFunctionCatalog(*owner, second);
  EXPECT_EQ(encodeFunctionContract(first), encodeFunctionContract(second));
  EXPECT_EQ(first.registration_sha256().size(), 32);
  const auto frozen = encodeFunctionContract(first);
  EXPECT_THROW(appendRegisteredFunctionCatalog(*owner, first), std::invalid_argument);
  EXPECT_EQ(frozen, encodeFunctionContract(first));
}
}  // namespace
}  // namespace pangrova::capability::test
