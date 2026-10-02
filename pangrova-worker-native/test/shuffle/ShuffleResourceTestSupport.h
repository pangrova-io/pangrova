#pragma once

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>

#include <gtest/gtest.h>

#include "shuffle/FlightShuffleServer.h"

namespace pangrova::shuffle::test {

inline ShuffleBatch taggedBatch(int32_t partition, int64_t tag, size_t bytes = 32) {
  const std::string payload(bytes, static_cast<char>('a' + tag % 26));
  return {partition, folly::IOBuf::copyBuffer(payload), tag};
}

class SpillDirectory {
 public:
  SpillDirectory() {
    auto path = (std::filesystem::temp_directory_path() / "shuffle-test-XXXXXX").string();
    if (!::mkdtemp(path.data())) throw std::runtime_error("cannot create shuffle test directory");
    path_ = std::move(path);
  }
  ~SpillDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
    EXPECT_FALSE(error) << error.message();
  }
  const std::string& path() const { return path_; }
 private:
  std::string path_;
};

// A real Flight stream can be driven synchronously without socket timing or
// transport-specific status conversion. Context lives longer than the stream.
class CallContext : public arrow::flight::ServerCallContext {
 public:
  const std::string& peer_identity() const override { return peer_; }
  const std::string& peer() const override { return peer_; }
  void AddHeader(const std::string&, const std::string&) const override {}
  void AddTrailer(const std::string&, const std::string&) const override {}
  arrow::flight::ServerMiddleware* GetMiddleware(const std::string&) const override {
    return nullptr;
  }
  bool is_cancelled() const override { return cancelled.load(); }
  const arrow::flight::CallHeaders& incoming_headers() const override { return headers_; }
  std::atomic<bool> cancelled{false};
 private:
  const std::string peer_ = "shuffle-test";
  arrow::flight::CallHeaders headers_;
};

inline arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> openStream(
    FlightShuffleServer& server, const CallContext& context,
    int32_t partition = 0, int64_t handle = 1) {
  std::unique_ptr<arrow::flight::FlightDataStream> stream;
  const arrow::flight::Ticket ticket{std::to_string(handle) + "\nq\n0\n" +
      std::to_string(partition) + "\n1\n0\n1"};
  ARROW_RETURN_NOT_OK(server.DoGet(context, ticket, &stream));
  return stream;
}

inline void registerQueue(FlightShuffleServer& server, SerializedShuffleQueue& queue,
                          int64_t handle = 1) {
  server.registerHandle(handle, &queue, "q", 0, 3, 1, 0, 1);
}

}  // namespace pangrova::shuffle::test