#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <pthread.h>
#include <sched.h>
#include <vector>
#include <cerrno>
#include <sanitizer/lsan_interface.h>

static std::atomic<int> destructors{0};
static void destroy(void* value) { std::free(value); ++destructors; }
__attribute__((noinline)) static void allocateInKey(pthread_key_t key) {
  void* value = std::malloc(193);
  if (!value || pthread_setspecific(key, value)) std::abort();
  std::memset(value, 0x31, 193);
}
__attribute__((noinline)) static void abandonKey(pthread_key_t key) {
  allocateInKey(key);
  if (pthread_setspecific(key, nullptr)) std::abort();
}
__attribute__((noinline)) static void actualUaf() {
  volatile char* value = static_cast<char*>(std::malloc(37));
  std::free(const_cast<char*>(value));
  value[4] = 1;
}
__attribute__((noinline)) static void actualOob() {
  volatile char* value = static_cast<char*>(std::malloc(37));
  value[37] = 1;
  std::free(const_cast<char*>(value));
}
struct WorkerOwner { pthread_key_t first, last; std::atomic<bool> ready{false}, finish{false}; };
static void* liveWorker(void* raw) {
  auto& state = *static_cast<WorkerOwner*>(raw);
  allocateInKey(state.first); allocateInKey(state.last);
  state.ready.store(true, std::memory_order_release);
  while (!state.finish.load(std::memory_order_acquire)) sched_yield();
  return nullptr;
}
static void* lostWorker(void* raw) {
  abandonKey(*static_cast<pthread_key_t*>(raw));
  return nullptr;
}
int main(int argc, char** argv) {
  if (argc != 2) return 2;
  Dl_info loaded{};
  if (!dladdr(reinterpret_cast<void*>(&__lsan_do_recoverable_leak_check), &loaded)) std::abort();
  std::printf("actual_sanitizer_runtime=%s\n", loaded.dli_fname);
  std::fflush(stdout);
  const int mode = std::atoi(argv[1]);
  if (mode == 3) actualUaf();
  if (mode == 4) actualOob();
  if (mode < 0 || mode > 4) return 2;
  std::vector<pthread_key_t> keys;
  while (keys.size() < PTHREAD_KEYS_MAX) {
    pthread_key_t key;
    int result = pthread_key_create(&key, destroy);
    if (result == EAGAIN) break;
    if (result) std::abort();
    keys.push_back(key);
  }
  if (keys.size() < 2 || keys.back() != 767) std::abort();
  const auto first = keys.front(), last = keys.back();
  if (mode == 0) {
    allocateInKey(first); allocateInKey(last);
    if (__lsan_do_recoverable_leak_check() != 0) std::abort();
    for (auto key : {first, last}) {
      void* value = pthread_getspecific(key);
      if (!value || pthread_setspecific(key, nullptr)) std::abort();
      destroy(value);
    }
  }
  if (mode == 1) {
    WorkerOwner state{first, last};
    pthread_t worker;
    if (pthread_create(&worker, nullptr, liveWorker, &state)) std::abort();
    while (!state.ready.load(std::memory_order_acquire)) sched_yield();
    int leaks = __lsan_do_recoverable_leak_check();
    state.finish.store(true, std::memory_order_release);
    if (pthread_join(worker, nullptr)) std::abort();
    if (leaks != 0 || destructors.load() != 2) std::abort();
  }
  if (mode == 2) {
    pthread_t worker;
    pthread_key_t abandoned = last;
    if (pthread_create(&worker, nullptr, lostWorker, &abandoned) || pthread_join(worker, nullptr)) std::abort();
    if (__lsan_do_recoverable_leak_check() != 1 || destructors.load() != 0) std::abort();
  }
  for (auto key : keys) if (pthread_key_delete(key)) std::abort();
  if (mode <= 1 && (destructors.load() != 2 || __lsan_do_recoverable_leak_check() != 0)) std::abort();
  std::printf("actual_tls_control mode=%d keys=%zu last=%lu destroyed=%d\n", mode,
      keys.size(), static_cast<unsigned long>(last), destructors.load());
  return 0;
}
