#include <atomic>
#include <cstddef>
#include <cstring>
#include <string>

#include <FlexLexer.h>
#include <zlib.h>

int main() {
  const std::atomic<int> state{0};
  const std::string version{zlibVersion()};
  return state.load() != 0 || version.empty();
}
