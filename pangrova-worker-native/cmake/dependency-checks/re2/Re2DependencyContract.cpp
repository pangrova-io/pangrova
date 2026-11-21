#include <absl/container/flat_hash_set.h>
#include <re2/re2.h>

#include <cstdint>
#include <string>
#include <thread>

// Compile and link the actual selected package; copying an archive alone does
// not prove that template layout and the DFA use the same sanitizer ABI.
int checkPackage() {
  absl::flat_hash_set<std::uint64_t> values;
  for (std::uint64_t value = 0; value < 200; ++value) {
    values.insert(value);
  }
  if (values.size() != 200 || !values.contains(199)) {
    return 3;
  }
  const re2::RE2 pattern("([a-z]+)-[0-9]+");
  const std::string input = "worker-17";
  std::string captured;
  if (!pattern.ok() || !re2::RE2::FullMatch(input, pattern, &captured) || captured != "worker") {
    return 1;
  }
  std::string replaced = input;
  if (re2::RE2::GlobalReplace(&replaced, pattern, "node") != 1 || replaced != "node") {
    return 2;
  }
  return 0;
}

int main() {
  // Exercise and retire the actual thread-local hash state before process leak
  // analysis. Keep this same lifecycle for matching and incompatible packages.
  int result = 1;
  std::thread owner([&result]() { result = checkPackage(); });
  owner.join();
  return result;
}
