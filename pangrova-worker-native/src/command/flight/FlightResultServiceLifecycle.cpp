#include "command/flight/FlightServerRegistry.h"

#include <exception>
#include <stdexcept>
#include "engine/result/FlightResultServer.h"

namespace pangrova::jni {
int FlightServerRegistry::ensureResultServerRunning(pangrova::result::FlightResultServerConfig config) {
  {
    std::unique_lock<std::mutex> lock(result_mutex_);
    result_cv_.wait(lock, [this] { return result_state_ != ResultServiceState::STARTING; });
    if (result_state_ == ResultServiceState::CLOSING) throw std::logic_error("Result service is closing");
    if (result_state_ == ResultServiceState::READY) {
      if (!result_server_->isRunning()) throw std::runtime_error("Result service cleanup requires retry");
      return result_server_->boundPort();
    }
    result_state_ = ResultServiceState::STARTING;
  }
  std::shared_ptr<pangrova::result::FlightResultServer> candidate;
  try {
    applyTransportSecurity(config);
    candidate = std::make_shared<pangrova::result::FlightResultServer>(std::move(config));
    const int port = candidate->start();
    std::lock_guard<std::mutex> lock(result_mutex_);
    result_server_ = candidate;
    result_state_ = ResultServiceState::READY;
    result_cv_.notify_all();
    return port;
  } catch (...) {
    const auto failure = std::current_exception();
    try {
      if (candidate) candidate->stop();
    } catch (...) {
      std::lock_guard<std::mutex> lock(result_mutex_);
      result_server_ = candidate;
      result_state_ = ResultServiceState::READY;
      result_cv_.notify_all();
      throw;
    }
    std::lock_guard<std::mutex> lock(result_mutex_);
    result_state_ = ResultServiceState::STOPPED;
    result_cv_.notify_all();
    std::rethrow_exception(failure);
  }
}
}  // namespace pangrova::jni
