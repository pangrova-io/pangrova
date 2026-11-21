#include <cstdio>
#include <cstdlib>
#include <dlfcn.h>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>

__attribute__((noinline)) std::runtime_error makeError() {
  return std::runtime_error(std::string("diagnostic failure: ") + "actual owned message");
}

__attribute__((noinline)) void constructAndCopy() {
  const auto error = makeError();
  const std::runtime_error copy(error);
  if (std::string(copy.what()) != "diagnostic failure: actual owned message") std::abort();
}

__attribute__((noinline)) void throwNamedError() {
  const auto error = makeError();
  throw error;
}

__attribute__((noinline)) void throwTemporaryError() {
  throw makeError();
}

__attribute__((noinline)) void captureAndRethrow() {
  std::exception_ptr failure;
  try { throwNamedError(); }
  catch (...) { failure = std::current_exception(); }
  try { std::rethrow_exception(failure); }
  catch (const std::runtime_error& error) {
    if (std::string(error.what()) != "diagnostic failure: actual owned message") std::abort();
  }
  failure = nullptr;
}

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  const int mode = std::atoi(argv[1]);
  const int count = std::atoi(argv[2]);
  if (mode < 0 || mode > 8 || count < 0 || count > 4096) return 2;
  for (int attempt = 0; attempt < count; ++attempt) {
    if (mode == 1) constructAndCopy();
    else if (mode == 2 || mode == 3) {
      try {
        if (mode == 2) throwNamedError();
        else throwTemporaryError();
      } catch (const std::runtime_error& error) {
        if (std::string(error.what()) != "diagnostic failure: actual owned message") std::abort();
      }
    } else if (mode == 4) captureAndRethrow();
    else if (mode == 5 || mode == 6 || mode == 7) {
      constructAndCopy();
      if (mode == 5 || mode == 6) {
        const char* error = dlerror();
        if (attempt == 0) std::printf("actual_dyld_pending_error=%s\n", error ? error : "none");
        if (mode == 6) (void)dlerror();
      } else {
        void* library = dlopen("/usr/lib/libc++.1.dylib", RTLD_LAZY);
        if (!library) std::abort();
        if (dlclose(library) != 0) std::abort();
      }
    } else if (mode == 8) {
      std::thread worker(constructAndCopy);
      worker.join();
    }
  }
  std::printf("diagnostic_completed mode=%d count=%d\n", mode, count);
  return 0;
}
