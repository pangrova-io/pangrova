#include "engine/result/FlightResultServer.h"
#include "engine/result/ResultPayloadQueue.h"

#include <arrow/ipc/dictionary.h>
#include <arrow/util/key_value_metadata.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

#include "engine/observability/PangrovaLog.h"

namespace pangrova::result {
namespace {
using arrow::flight::FlightPayload;

int64_t epochMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

bool sameFieldLayout(const arrow::Field& storage, const arrow::Field& display) {
  if (storage.nullable() != display.nullable()) return false;
  const auto& left = *storage.type();
  const auto& right = *display.type();
  if (left.Equals(right)) return true;
  if (left.id() != right.id() || left.num_fields() == 0 ||
      left.num_fields() != right.num_fields()) return false;
  if (left.id() == arrow::Type::FIXED_SIZE_LIST &&
      static_cast<const arrow::FixedSizeListType&>(left).list_size() !=
          static_cast<const arrow::FixedSizeListType&>(right).list_size()) return false;
  for (int index = 0; index < left.num_fields(); ++index) {
    if (!sameFieldLayout(*left.field(index), *right.field(index))) return false;
  }
  return true;
}

class ResultFlightStream final : public arrow::flight::FlightDataStream {
 public:
  ResultFlightStream(FlightResultServer::TaskOutputInfo info,
      std::shared_ptr<arrow::Schema> display, int64_t timeoutSeconds,
      const arrow::flight::ServerCallContext* context)
      : info_(std::move(info)), display_(std::move(display)),
        timeout_seconds_(timeoutSeconds), context_(context) {
    info_.ownership->acquireReader();
  }

  ~ResultFlightStream() override {
    if (closed_) return;
    try {
      (void)finish(false);
    } catch (const std::exception& error) {
      PANGROVA_LOG(ERROR) << "event=result_flight_close_failed task_id=" << taskId()
                         << " error=" << error.what();
    }
  }

  std::shared_ptr<arrow::Schema> schema() override { return display_; }

  arrow::Result<FlightPayload> GetSchemaPayload() override {
    if (!live()) return fail(arrow::Status::Cancelled("Result authority was revoked or expired"));
    FlightPayload payload;
    const arrow::ipc::DictionaryFieldMapper mapper(*display_);
    auto status = arrow::ipc::GetSchemaPayload(*display_, arrow::ipc::IpcWriteOptions::Defaults(),
        mapper, &payload.ipc_message);
    if (!status.ok()) return fail(status);
    return payload;
  }

  arrow::Result<FlightPayload> Next() override {
    if (!failure_.ok()) return failure_;
    if (eof_) return FlightPayload{};
    try {
      auto wait = static_cast<int64_t>(info_.authority.deadline_epoch_ms()) - epochMillis();
      if (timeout_seconds_ > 0) wait = std::min(wait, timeout_seconds_ * 1000);
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait);
      auto result = info_.queue->next(deadline, [this] { return !live(); });
      if (!result.ok()) return fail(result.status());
      if (!live()) return fail(arrow::Status::Cancelled("Result authority was revoked or expired"));
      if (!*result) eof_ = true;
      return result->flightPayload();
    } catch (const std::exception& error) {
      return fail(arrow::Status::IOError("Result Flight output failed: ", error.what()));
    }
  }

  arrow::Status Close() override {
    // Arrow calls Close after every WriteData and WritesDone succeeds.
    // Destruction can instead follow a failed transport write.
    return finish(eof_ && live());
  }

 private:
  arrow::Status finish(bool committed) {
    if (closed_) return failure_;
    closed_ = true;
    const auto release = folly::makeGuard([this] { info_.ownership->releaseReader(); });
    if (failure_.ok()) {
      if (!committed) {
        (void)fail(arrow::Status::IOError("Result reader closed before committed EOF"));
      } else if (info_.report_stream_finished) {
        try {
          info_.report_stream_finished();
        } catch (const std::exception& error) {
          (void)fail(arrow::Status::IOError("Cannot commit result delivery: ", error.what()));
        }
      }
    }
    info_.ownership->revokeAuthority();
    info_.queue->abort(arrow::Status::Cancelled("Result Flight reader closed"));
    info_.ownership->revokeAndClearQueue();
    return failure_;
  }

  const std::string& taskId() const { return info_.authority.attempt().task_id(); }

  bool live() const {
    return !(context_ && context_->is_cancelled()) &&
        !info_.ownership->evidence().authority_revoked &&
        info_.authority.deadline_epoch_ms() > static_cast<uint64_t>(epochMillis());
  }

  arrow::Status fail(const arrow::Status& status) {
    failure_ = status;
    if (info_.ownership->revokeAuthority() && info_.report_stream_failure) {
      try {
        info_.report_stream_failure(status.ToString());
      } catch (const std::exception& error) {
        logFailure(arrow::Status::IOError("Result stream failure report failed: ", error.what()));
      }
    }
    return status;
  }

  void logFailure(const arrow::Status& status) const {
    PANGROVA_LOG(ERROR) << "event=result_flight_cleanup_failed task_id=" << taskId()
                       << " error=" << status.ToString();
  }

  const FlightResultServer::TaskOutputInfo info_;
  const std::shared_ptr<arrow::Schema> display_;
  const int64_t timeout_seconds_;
  const arrow::flight::ServerCallContext* context_;
  bool eof_ = false;
  bool closed_ = false;
  arrow::Status failure_;
};
}  // namespace

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> resultFlightStream(
    FlightResultServer::TaskOutputInfo info, int64_t timeoutSeconds,
    const arrow::flight::ServerCallContext* context) {
  if (!info.claimed || !info.pool || !info.queue || !info.output_type || !info.wire_schema ||
      !info.ownership || static_cast<size_t>(info.wire_schema->num_fields()) != info.output_type->size() ||
      info.column_labels.size() != info.output_type->size()) {
    return arrow::Status::Invalid("Result registration lacks its exact schema, ownership or byte queue");
  }
  auto storage = info.wire_schema;
  ARROW_ASSIGN_OR_RAISE(auto typed, detail::withResultColumnTypes(
      info.result_schema ? info.result_schema : storage, info.result_column_types));
  if (typed->num_fields() != storage->num_fields()) {
    return arrow::Status::Invalid("Result presentation changes the admitted output arity");
  }
  auto fields = typed->fields();
  for (size_t i = 0; i < fields.size(); ++i) {
    if (!sameFieldLayout(*storage->field(i), *fields[i])) {
      return arrow::Status::TypeError("Result presentation changes the admitted output type at ordinal ", i);
    }
    auto metadata = fields[i]->metadata()
        ? fields[i]->metadata()->Copy() : std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append("pangrova:result:ordinal", std::to_string(i));
    fields[i] = fields[i]->WithMetadata(metadata)->WithName(info.column_labels[i]);
  }
  auto display = arrow::schema(std::move(fields), typed->metadata());
  const auto ownership = info.ownership;
  try {
    return std::make_unique<ResultFlightStream>(
        std::move(info), std::move(display), timeoutSeconds, context);
  } catch (const std::runtime_error& error) {
    if (ownership->evidence().authority_revoked) {
      return arrow::Status::Cancelled("Result reader admission failed: ", error.what());
    }
    return arrow::Status::Invalid("Result reader admission failed: ", error.what());
  }
}
}  // namespace pangrova::result
