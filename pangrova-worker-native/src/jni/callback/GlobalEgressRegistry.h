// Process-wide owner of the single Java external-shuffle egress callback.
#pragma once

#include <jni.h>

#include <mutex>
#include <stdexcept>

namespace pangrova::jni {

class GlobalEgressRegistry {
 public:
  static GlobalEgressRegistry& instance() {
    static GlobalEgressRegistry registry;
    return registry;
  }

  void registerDefaultEgress(JNIEnv* env, jobject egress) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (egress_ref_) {
      throw std::logic_error("External shuffle egress is already registered");
    }
    egress_ref_ = env->NewGlobalRef(egress);
    if (!egress_ref_) {
      throw std::runtime_error(
          "Failed to retain external shuffle egress callback");
    }
  }

  // Returns a caller-owned local reference, preventing concurrent release.
  jobject acquire(JNIEnv* env) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return egress_ref_ ? env->NewLocalRef(egress_ref_) : nullptr;
  }

  void clear(JNIEnv* env) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (egress_ref_) {
      env->DeleteGlobalRef(egress_ref_);
      egress_ref_ = nullptr;
    }
  }

 private:
  GlobalEgressRegistry() = default;

  mutable std::mutex mutex_;
  jobject egress_ref_ = nullptr;
};

}  // namespace pangrova::jni
