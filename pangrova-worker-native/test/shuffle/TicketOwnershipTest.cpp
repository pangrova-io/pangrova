// Unit test for F-CHAIN-SHUFFLE-06 (#110): the pure decision the Flight DoGet
// path uses to validate that a (HMAC-verified) ticket actually owns the shuffle
// handle it routed to. A ticket is signed with the producer's query/stage
// identity; this guard rejects a ticket whose identity does not match the
// identity recorded when the handle was registered, so a reused/mis-routed
// handle can never serve one query's committed shuffle output to another query.
//
// Kept free of Arrow/Flight/gRPC types so it is unit-testable without the
// two-Arrow ODR landmine (the real reject path returns a non-OK Flight Status,
// which cannot be exercised in the standalone test binary).

#include "shuffle/FlightShuffleServer.h"

#include <gtest/gtest.h>

namespace pangrova::shuffle {
namespace {

using detail::ticketOwnershipValid;

// Matching query_id and stage_id -> owns the handle.
TEST(TicketOwnershipTest, MatchingIdentityIsValid) {
  EXPECT_TRUE(ticketOwnershipValid(/*ticket_query_id=*/"q1",
                                   /*ticket_stage_id=*/3,
                                   /*registered_query_id=*/"q1",
                                   /*registered_stage_id=*/3));
}

// Different query_id -> rejected (the core cross-query handle-reuse guard).
TEST(TicketOwnershipTest, MismatchedQueryIdIsRejected) {
  EXPECT_FALSE(ticketOwnershipValid(/*ticket_query_id=*/"q2",
                                    /*ticket_stage_id=*/3,
                                    /*registered_query_id=*/"q1",
                                    /*registered_stage_id=*/3));
}

// Same query_id but different stage -> rejected when stage was recorded.
TEST(TicketOwnershipTest, MismatchedStageIdIsRejected) {
  EXPECT_FALSE(ticketOwnershipValid(/*ticket_query_id=*/"q1",
                                    /*ticket_stage_id=*/4,
                                    /*registered_query_id=*/"q1",
                                    /*registered_stage_id=*/3));
}

// Empty registered query_id -> identity not recorded (legacy path) -> accept
// regardless of the ticket's claimed identity.
TEST(TicketOwnershipTest, UnrecordedIdentityIsNotEnforced) {
  EXPECT_TRUE(ticketOwnershipValid(/*ticket_query_id=*/"anything",
                                   /*ticket_stage_id=*/7,
                                   /*registered_query_id=*/"",
                                   /*registered_stage_id=*/-1));
}

// stage_id < 0 means stage was not recorded -> query_id is still enforced, but
// the stage is ignored (matches the native producer path which has no stage id).
TEST(TicketOwnershipTest, UnrecordedStageEnforcesQueryOnly) {
  EXPECT_TRUE(ticketOwnershipValid(/*ticket_query_id=*/"q1",
                                   /*ticket_stage_id=*/99,
                                   /*registered_query_id=*/"q1",
                                   /*registered_stage_id=*/-1));
  EXPECT_FALSE(ticketOwnershipValid(/*ticket_query_id=*/"qX",
                                    /*ticket_stage_id=*/99,
                                    /*registered_query_id=*/"q1",
                                    /*registered_stage_id=*/-1));
}

}  // namespace
}  // namespace pangrova::shuffle
