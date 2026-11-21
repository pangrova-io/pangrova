// ShuffleTicketKeys implementation.

#include "engine/shuffle/ShuffleTicketKeys.h"

namespace pangrova::shuffle {

ShuffleTicketKeys& ShuffleTicketKeys::instance() {
  static ShuffleTicketKeys keys;
  return keys;
}

void ShuffleTicketKeys::set(std::string currentKey, std::string previousKey) {
  std::lock_guard<std::mutex> lock(mu_);
  currentKey_ = std::move(currentKey);
  previousKey_ = std::move(previousKey);
}

std::string ShuffleTicketKeys::currentKey() const {
  std::lock_guard<std::mutex> lock(mu_);
  return currentKey_;
}

std::string ShuffleTicketKeys::previousKey() const {
  std::lock_guard<std::mutex> lock(mu_);
  return previousKey_;
}

bool ShuffleTicketKeys::enabled() const {
  std::lock_guard<std::mutex> lock(mu_);
  return !currentKey_.empty();
}

}  // namespace pangrova::shuffle
