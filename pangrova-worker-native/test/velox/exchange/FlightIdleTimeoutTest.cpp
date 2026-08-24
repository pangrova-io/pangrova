// Unit test for F-CHAIN-SHUFFLE-05 (#102): the pure decision the Flight idle
// watchdog uses to decide whether a single in-flight read has stalled long
// enough to cancel the stream.
//
// The old PangrovaExchangeSource put a fixed wall-clock timeout on the WHOLE DoGet
// stream, so a long-running but progressing sparse-output query was killed once
// total stream time crossed the deadline. The fix is a per-Next IDLE timeout:
// a stall is declared ONLY while a read is actually in flight AND no progress
// has happened for the budget — never capping total stream duration, and never
// firing while the consumer simply isn't pulling.
//
// Kept free of Arrow/Flight/gRPC types so it is unit-testable without the
// two-Arrow ODR landmine (the real cancel path drives a gRPC StopSource, which
// cannot be exercised in the standalone test binary).

#include "velox/exchange/PangrovaExchangeSource.h"

#include <gtest/gtest.h>

namespace pangrova::velox {
namespace {

using detail::flightShouldCancelOnIdle;

// No read in flight -> never cancel, no matter how much wall-clock has elapsed.
// This is the "consumer isn't pulling" case the old total-stream deadline got
// wrong (it would kill a healthy idle stream).
TEST(FlightIdleTimeoutTest, NotInFlightNeverCancels) {
  EXPECT_FALSE(flightShouldCancelOnIdle(/*next_in_flight=*/false,
                                        /*now_ms=*/1'000'000,
                                        /*next_started_ms=*/0,
                                        /*idle_budget_ms=*/30'000));
}

// In flight but still within the idle budget -> keep waiting (progress is
// allowed to be slow; only a true stall is fatal).
TEST(FlightIdleTimeoutTest, InFlightWithinBudgetDoesNotCancel) {
  EXPECT_FALSE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                        /*now_ms=*/29'999,
                                        /*next_started_ms=*/0,
                                        /*idle_budget_ms=*/30'000));
}

// In flight and idle past the budget -> cancel (stalled / unreachable peer).
TEST(FlightIdleTimeoutTest, InFlightPastBudgetCancels) {
  EXPECT_TRUE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                       /*now_ms=*/30'000,
                                       /*next_started_ms=*/0,
                                       /*idle_budget_ms=*/30'000));
  EXPECT_TRUE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                       /*now_ms=*/120'000,
                                       /*next_started_ms=*/1'000,
                                       /*idle_budget_ms=*/30'000));
}

// A long-running stream that keeps re-arming (each Next starts a fresh window)
// is never killed for total duration: even at a huge absolute time, a freshly
// armed read is within budget.
TEST(FlightIdleTimeoutTest, FreshArmAtLargeAbsoluteTimeSurvives) {
  const int64_t huge = 10'000'000'000;  // ~115 days of uptime
  EXPECT_FALSE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                        /*now_ms=*/huge + 10,
                                        /*next_started_ms=*/huge,
                                        /*idle_budget_ms=*/30'000));
}

// Not yet armed (started_ms < 0) -> never cancel.
TEST(FlightIdleTimeoutTest, UnarmedNeverCancels) {
  EXPECT_FALSE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                        /*now_ms=*/1'000'000,
                                        /*next_started_ms=*/-1,
                                        /*idle_budget_ms=*/30'000));
}

// Disabled budget (<= 0) -> never cancel.
TEST(FlightIdleTimeoutTest, DisabledBudgetNeverCancels) {
  EXPECT_FALSE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                        /*now_ms=*/1'000'000,
                                        /*next_started_ms=*/0,
                                        /*idle_budget_ms=*/0));
  EXPECT_FALSE(flightShouldCancelOnIdle(/*next_in_flight=*/true,
                                        /*now_ms=*/1'000'000,
                                        /*next_started_ms=*/0,
                                        /*idle_budget_ms=*/-5));
}

}  // namespace
}  // namespace pangrova::velox
