// Unit test for F-CHAIN-SHUFFLE-03 (#100): the pure decision function the
// PangrovaExchangeSource consumer uses at its two terminal points (a mid-stream
// Flight Next() error and a clean EOF) to classify a shuffle read as
// keep-reading / clean-complete / failed-truncated.
//
// Kept free of Arrow/Flight/gRPC types so it is unit-testable without the
// two-Arrow ODR landmine: the real truncation path returns a non-OK Flight
// Status which cannot be exercised in the standalone test binary, but the
// decision logic that consumes its outcome can be.

#include "engine/velox/exchange/PangrovaExchangeSource.h"

#include <gtest/gtest.h>

namespace pangrova::velox {
namespace {

using detail::decideStreamCompletion;
using detail::StreamCompletion;

// A mid-stream error ALWAYS means truncation, regardless of counts: a Flight
// error mid-stream signals the producer was torn down before committing.
TEST(DecideStreamCompletionTest, MidStreamErrorAlwaysTruncated) {
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/true, /*clean_eof=*/false,
                             /*received=*/0, /*expected=*/-1),
      StreamCompletion::kFailedTruncated);
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/true, /*clean_eof=*/false,
                             /*received=*/100, /*expected=*/100),
      StreamCompletion::kFailedTruncated);
}

// Clean EOF with no over-the-wire contract (expected < 0): trust the EOF.
TEST(DecideStreamCompletionTest, CleanEofNoContractIsComplete) {
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/false, /*clean_eof=*/true,
                             /*received=*/0, /*expected=*/-1),
      StreamCompletion::kCompleteOk);
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/false, /*clean_eof=*/true,
                             /*received=*/7, /*expected=*/-1),
      StreamCompletion::kCompleteOk);
}

// Clean EOF WITH a contract: complete only if received >= expected.
TEST(DecideStreamCompletionTest, CleanEofContractEnforcesReceivedCount) {
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/false, /*clean_eof=*/true,
                             /*received=*/10, /*expected=*/10),
      StreamCompletion::kCompleteOk);
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/false, /*clean_eof=*/true,
                             /*received=*/11, /*expected=*/10),
      StreamCompletion::kCompleteOk);
  // Short read: EOF arrived before the committed count was reached => truncated.
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/false, /*clean_eof=*/true,
                             /*received=*/9, /*expected=*/10),
      StreamCompletion::kFailedTruncated);
}

// Neither error nor EOF: keep pulling.
TEST(DecideStreamCompletionTest, NoTerminalSignalKeepsReading) {
  EXPECT_EQ(
      decideStreamCompletion(/*mid_stream_error=*/false, /*clean_eof=*/false,
                             /*received=*/3, /*expected=*/10),
      StreamCompletion::kKeepReading);
}

}  // namespace
}  // namespace pangrova::velox
