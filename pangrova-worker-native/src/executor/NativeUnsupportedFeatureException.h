#pragma once

#include <stdexcept>
#include <string>

namespace pangrova::executor {

// Thrown when the native connector encounters an unsupported feature
// that requires fallback to Java-side execution.
//
// Carries structured metadata (feature_name, connector_type, split_id,
// query_id, task_id) so the Java side can report NATIVE_FALLBACK_REQUIRED
// with full context for diagnostics and observability.
class NativeUnsupportedFeatureException : public std::runtime_error {
 public:
  NativeUnsupportedFeatureException(
      std::string feature_name,
      std::string connector_type,
      std::string split_id)
      : std::runtime_error(
            buildMessage(feature_name, connector_type, split_id, "", "")),
        feature_name_(std::move(feature_name)),
        connector_type_(std::move(connector_type)),
        split_id_(std::move(split_id)) {}

  NativeUnsupportedFeatureException(
      std::string feature_name,
      std::string connector_type,
      std::string split_id,
      std::string query_id,
      std::string task_id)
      : std::runtime_error(
            buildMessage(feature_name, connector_type, split_id, query_id, task_id)),
        feature_name_(std::move(feature_name)),
        connector_type_(std::move(connector_type)),
        split_id_(std::move(split_id)),
        query_id_(std::move(query_id)),
        task_id_(std::move(task_id)) {}

  const std::string& featureName() const noexcept { return feature_name_; }
  const std::string& connectorType() const noexcept { return connector_type_; }
  const std::string& splitId() const noexcept { return split_id_; }
  const std::string& queryId() const noexcept { return query_id_; }
  const std::string& taskId() const noexcept { return task_id_; }

 private:
  static std::string buildMessage(
      const std::string& feature_name,
      const std::string& connector_type,
      const std::string& split_id,
      const std::string& query_id,
      const std::string& task_id) {
    std::string msg = "Unsupported feature: feature_name=" + feature_name +
        ", connector_type=" + connector_type +
        ", split_id=" + split_id;
    if (!query_id.empty()) {
      msg += ", query_id=" + query_id;
    }
    if (!task_id.empty()) {
      msg += ", task_id=" + task_id;
    }
    return msg;
  }

  std::string feature_name_;
  std::string connector_type_;
  std::string split_id_;
  std::string query_id_;
  std::string task_id_;
};

}  // namespace pangrova::executor
