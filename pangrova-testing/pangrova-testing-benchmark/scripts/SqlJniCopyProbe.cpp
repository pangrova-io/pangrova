// Benchmark-only JVMTI counter for the JNI methods that return copied
// task-report byte arrays. These three methods are audited to allocate/copy
// their complete C++ payload on each success.
#include <jni.h>
#include <jvmti.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>

namespace {
using ReportMethod = jbyteArray(JNICALL *)(JNIEnv *, jclass, jlong);
constexpr std::array<const char *, 3> methods = {
    "nativeReleaseExecutor0", "nativeGetTaskStatus0", "nativeGetTaskStats0"};
struct Counter {
  std::atomic<ReportMethod> original{nullptr};
  std::atomic<unsigned long long> calls{0};
  std::atomic<unsigned long long> bytes{0};
};
std::array<Counter, 3> counters;
std::string output;

template <size_t index>
jbyteArray JNICALL countReport(JNIEnv *env, jclass owner, jlong handle) {
  Counter &count = counters[index];
  jbyteArray result = count.original.load()(env, owner, handle);
  if (result && !env->ExceptionCheck()) {
    count.bytes.fetch_add(env->GetArrayLength(result),
                          std::memory_order_relaxed);
    count.calls.fetch_add(1, std::memory_order_relaxed);
  }
  return result;
}

void JNICALL bind(jvmtiEnv *jvmti, JNIEnv *, jthread, jmethodID method,
                  void *address, void **replacement) {
  char *name = nullptr, *signature = nullptr;
  if (jvmti->GetMethodName(method, &name, &signature, nullptr) !=
      JVMTI_ERROR_NONE)
    return;
  if (std::strcmp(signature, "(J)[B") == 0) {
    for (size_t i = 0; i < methods.size(); ++i) {
      if (std::strcmp(name, methods[i]) != 0)
        continue;
      jclass cls = nullptr;
      char *className = nullptr;
      if (jvmti->GetMethodDeclaringClass(method, &cls) != JVMTI_ERROR_NONE ||
          jvmti->GetClassSignature(cls, &className, nullptr) !=
              JVMTI_ERROR_NONE)
        break;
      bool runtime =
          std::strcmp(className,
                      "Lorg/pangrova/worker/nativebridge/jni/NativeRuntime;") ==
              0 ||
          std::strcmp(className, "Lorg/pangrova/worker/jni/NativeRuntime;") ==
              0;
      jvmti->Deallocate(reinterpret_cast<unsigned char *>(className));
      if (runtime) {
        constexpr ReportMethod wrappers[] = {countReport<0>, countReport<1>,
                                             countReport<2>};
        counters[i].original.store(reinterpret_cast<ReportMethod>(address));
        *replacement = reinterpret_cast<void *>(wrappers[i]);
      }
    }
  }
  jvmti->Deallocate(reinterpret_cast<unsigned char *>(name));
  jvmti->Deallocate(reinterpret_cast<unsigned char *>(signature));
}

void JNICALL finish(jvmtiEnv *, JNIEnv *) {
  // Parents and clients also inherit the agent option. Emit files only for
  // Native Workers.
  bool bound = false;
  for (const auto &counter : counters)
    bound |= counter.original.load() != nullptr;
  if (!bound)
    return;
  auto path = output + "/worker-" + std::to_string(getpid()) + ".csv";
  FILE *file = std::fopen(path.c_str(), "wx");
  if (!file) {
    std::perror("JNI copy probe output");
    return;
  }
  std::fprintf(file, "pid,method,successful_calls,copied_report_bytes\n");
  for (size_t i = 0; i < methods.size(); ++i) {
    std::fprintf(file, "%d,%s,%llu,%llu\n", getpid(), methods[i],
                 counters[i].calls.load(), counters[i].bytes.load());
  }
  if (std::fclose(file))
    std::perror("JNI copy probe flush");
}
} // namespace

extern "C" JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *vm, char *options,
                                               void *) {
  if (!options || options[0] != '/')
    return JNI_ERR;
  output = options;
  jvmtiEnv *env = nullptr;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), JVMTI_VERSION_1_2) != JNI_OK)
    return JNI_ERR;
  jvmtiCapabilities capabilities{};
  capabilities.can_generate_native_method_bind_events = 1;
  jvmtiEventCallbacks callbacks{};
  callbacks.NativeMethodBind = bind;
  callbacks.VMDeath = finish;
  if (env->AddCapabilities(&capabilities) != JVMTI_ERROR_NONE ||
      env->SetEventCallbacks(&callbacks, sizeof(callbacks)) !=
          JVMTI_ERROR_NONE ||
      env->SetEventNotificationMode(JVMTI_ENABLE,
                                    JVMTI_EVENT_NATIVE_METHOD_BIND,
                                    nullptr) != JVMTI_ERROR_NONE ||
      env->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_VM_DEATH,
                                    nullptr) != JVMTI_ERROR_NONE)
    return JNI_ERR;
  return JNI_OK;
}
