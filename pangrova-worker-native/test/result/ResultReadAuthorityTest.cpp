#include <gtest/gtest.h>
#include <chrono>
#include <openssl/evp.h>
#include "result/ResultReadAuthority.h"
#include "shuffle/ShuffleTicketKeys.h"
#include "shuffle/TicketSigner.h"

namespace pangrova::result::test {
namespace {
using namespace protocol::v2;
constexpr std::string_view kKey = "producer-transport-key-32-bytes!!!";

ExecutionCallbackContext admitted() {
  ExecutionCallbackContext context;
  context.mutable_security()->set_tenant_id("tenant");
  context.mutable_security()->set_principal("reader");
  context.mutable_security()->add_active_roles("role");
  context.mutable_security()->set_policy_context_hash("policy");
  auto* attempt = context.mutable_attempt();
  attempt->set_query_or_job_id("query");
  attempt->set_task_id("task");
  attempt->set_stage_id(2);
  attempt->set_partition_id(3);
  attempt->set_attempt_id(4);
  attempt->set_plan_epoch(5);
  attempt->set_fencing_token(6);
  attempt->set_control_epoch(7);
  attempt->mutable_execution_owner()->set_owner_id("coordinator");
  attempt->mutable_execution_owner()->set_owner_incarnation("incarnation");
  attempt->mutable_execution_owner()->set_fencing_token(8);
  context.set_deadline_epoch_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count() + 60000);
  return context;
}

ResultReadGrant grant(const ExecutionCallbackContext& context) {
  ResultReadGrant value;
  *value.mutable_security() = context.security();
  *value.mutable_attempt() = context.attempt();
  value.set_scope(RESULT_READ_SCOPE_RESULT_STREAM);
  value.set_expires_at_epoch_ms(context.deadline_epoch_ms());
  return value;
}

std::string ticket(const ResultReadGrant& authority, std::string_view key = kKey,
    std::string_view domain = "pangrova.result.read.v1:") {
  const auto bytes = authority.SerializeAsString();
  std::string encoded(4 * ((bytes.size() + 2) / 3) + 1, '\0');
  const int size = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()),
      reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size());
  encoded.resize(size);
  return shuffle::TicketSigner(std::string(key)).signWithExpiry(
      std::string(domain) + encoded, authority.expires_at_epoch_ms()).ticket;
}

class ResultReadAuthorityTest : public ::testing::Test {
 protected:
  void SetUp() override { shuffle::ShuffleTicketKeys::instance().set(std::string(kKey), ""); }
  void TearDown() override { shuffle::ShuffleTicketKeys::instance().set("", ""); }
};

TEST_F(ResultReadAuthorityTest, RealSignatureBindsAllAuthorityFieldsAndExactDeadline) {
  const auto context = admitted();
  const auto original = grant(context);
  auto authenticated = authenticateResultRead(ticket(original));
  ASSERT_TRUE(authenticated.ok()) << authenticated.status().ToString();
  EXPECT_TRUE(validateResultRead(*authenticated, context).ok());
  for (int field = 0; field < 9; ++field) {
    auto changed = original;
    switch (field) {
      case 0: changed.mutable_security()->set_tenant_id("other"); break;
      case 1: changed.mutable_security()->set_principal("other"); break;
      case 2: changed.mutable_security()->set_policy_context_hash("other"); break;
      case 3: changed.mutable_security()->clear_active_roles(); break;
      case 4: changed.mutable_attempt()->set_task_id("other"); break;
      case 5: changed.mutable_attempt()->set_attempt_id(9); break;
      case 6: changed.mutable_attempt()->set_control_epoch(9); break;
      case 7: changed.mutable_attempt()->mutable_execution_owner()->set_fencing_token(9); break;
      case 8: changed.set_expires_at_epoch_ms(original.expires_at_epoch_ms() + 1); break;
    }
    auto parsed = authenticateResultRead(ticket(changed));
    ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
    EXPECT_FALSE(validateResultRead(*parsed, context).ok()) << field;
  }
}

TEST_F(ResultReadAuthorityTest, RejectsRawWrongDomainTamperedExpiredScopeAndUnknownFields) {
  auto authority = grant(admitted());
  EXPECT_FALSE(authenticateResultRead("task").ok());
  EXPECT_FALSE(authenticateResultRead(ticket(authority, "different-key-32-bytes!!!!!!!!!")).ok());
  EXPECT_FALSE(authenticateResultRead(ticket(authority, kKey, "pangrova.shuffle.read.v1:")).ok());
  auto tampered = ticket(authority);
  tampered.back() = tampered.back() == '0' ? '1' : '0';
  EXPECT_FALSE(authenticateResultRead(tampered).ok());
  auto expired = authority;
  expired.set_expires_at_epoch_ms(1);
  EXPECT_FALSE(authenticateResultRead(ticket(expired)).ok());
  authority.set_scope(RESULT_READ_SCOPE_UNSPECIFIED);
  EXPECT_FALSE(authenticateResultRead(ticket(authority)).ok());
  authority.set_scope(RESULT_READ_SCOPE_RESULT_STREAM);
  authority.GetReflection()->MutableUnknownFields(&authority)->AddVarint(100, 1);
  EXPECT_FALSE(authenticateResultRead(ticket(authority)).ok());
}
}  // namespace
}  // namespace pangrova::result::test
