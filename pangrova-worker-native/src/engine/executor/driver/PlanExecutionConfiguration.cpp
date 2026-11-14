#include "engine/executor/driver/PlanExecutionConfiguration.h"

#include <algorithm>
#include <array>
#include <string_view>

#include "engine/executor/driver/PlanExecutionDriver.h"
#include "engine/executor/driver/TaskExecutor.h"
#include "engine/observability/PangrovaLog.h"
#include "engine/velox/plan/VeloxPlanConverter.h"

namespace pangrova::executor {
namespace {
using RunnerConfig = velox::VeloxTaskRunnerConfig;

template <typename Value>
struct NamedMember {
  std::string_view key;
  Value RunnerConfig::* member;
};

const std::array<NamedMember<bool>, 6> booleanOptions{{
    {"velox.hash_probe_finish_early_on_empty_build", &RunnerConfig::hash_probe_finish_early_on_empty_build},
    {"velox.hash_probe_string_dynamic_filter_pushdown", &RunnerConfig::hash_probe_string_dynamic_filter_pushdown},
    {"velox.adaptive_filter_reordering", &RunnerConfig::adaptive_filter_reordering},
    {"velox.parallel_output_join_build_rows", &RunnerConfig::parallel_output_join_build_rows},
    {"velox.table_scan_scaled_processing", &RunnerConfig::table_scan_scaled_processing},
    {"velox.preserve_dictionary_encoding", &RunnerConfig::preserve_dictionary_encoding}
}};

const std::array<NamedMember<int32_t>, 8> integerOptions{{
    {"velox.max_split_preload_per_driver", &RunnerConfig::max_split_preload_per_driver},
    {"velox.abandon_partial_agg_min_rows", &RunnerConfig::abandon_partial_agg_min_rows},
    {"velox.abandon_partial_agg_min_pct", &RunnerConfig::abandon_partial_agg_min_pct},
    {"velox.spill_num_partition_bits", &RunnerConfig::spill_num_partition_bits},
    {"velox.driver_cpu_time_slice_limit_ms", &RunnerConfig::driver_cpu_time_slice_limit_ms},
    {"velox.min_spillable_reservation_pct", &RunnerConfig::min_spillable_reservation_pct},
    {"velox.spillable_reservation_growth_pct", &RunnerConfig::spillable_reservation_growth_pct},
    {"velox.max_drivers", &RunnerConfig::max_drivers_override}
}};

const std::array<NamedMember<int64_t>, 4> wideIntegerOptions{{
    {"velox.max_partial_agg_memory", &RunnerConfig::max_partial_agg_memory},
    {"velox.max_extended_partial_agg_memory", &RunnerConfig::max_extended_partial_agg_memory},
    {"velox.spill_write_buffer_size", &RunnerConfig::spill_write_buffer_size},
    {"velox.shuffle_compression_min_bytes", &RunnerConfig::shuffle_compression_min_bytes}
}};

template <typename Value, size_t Size, typename Parse>
bool applyOption(RunnerConfig& config, const std::string& key, const std::string& value,
                 const std::array<NamedMember<Value>, Size>& options, Parse parse) {
  for (const auto& option : options) {
    if (option.key != key) continue;
    config.*(option.member) = parse(value);
    return true;
  }
  return false;
}

void applySessionProperties(RunnerConfig& runner, const TaskExecutorConfig& task) {
  for (const auto& [key, value] : task.session_properties) {
    try {
      if (applyOption(runner, key, value, booleanOptions,
              [](const std::string& text) { return text == "true"; }) ||
          applyOption(runner, key, value, integerOptions,
              [](const std::string& text) { return std::stoi(text); }) ||
          applyOption(runner, key, value, wideIntegerOptions,
              [](const std::string& text) { return std::stol(text); })) continue;
    } catch (const std::exception&) {
      PANGROVA_LOG_WARN << "invalid_session_property" << " key=" << key
                       << " value=" << value << " task_id=" << task.task_id;
    }
  }
}
}  // namespace

velox::VeloxTaskRunnerConfig makeRunnerConfig(const TaskExecutorConfig& config,
    const velox::VeloxPlanConverter& converter, const WorkerLocalSpillPolicy& spill_policy) {
  velox::VeloxTaskRunnerConfig runner_config;
  runner_config.function_registry = config.function_registry;
  runner_config.canonical_output_schema = std::make_shared<const substrait::NamedStruct>(converter.canonicalOutputSchema());
  if (converter.resultOutputSchema().names_size() > 0) {
    runner_config.result_output_schema = std::make_shared<const substrait::NamedStruct>(converter.resultOutputSchema());
  }
  runner_config.serde_schema_bindings = converter.serdeSchemaBindings();
  runner_config.executor_handle = config.executor_handle;
  runner_config.failure_reporter = config.failure_reporter;
  runner_config.task_id = config.task_id;
  runner_config.query_id = config.query_id;
  runner_config.tenant_id = config.tenant_id;
  runner_config.principal = config.principal;
  runner_config.policy_context_hash = config.policy_context_hash;
  runner_config.dependency_scope_hash = config.dependency_scope_hash;
  runner_config.dependency_version_hash = config.dependency_version_hash;
  runner_config.trace_id = config.trace_id;
  runner_config.deadline_epoch_ms = config.deadline_epoch_ms;
  runner_config.max_provider_concurrency =
      config.resource_limit.max_provider_concurrency;
  runner_config.callback_context = config.callback_context;
  runner_config.max_provider_items = config.resource_limit.max_provider_items;
  runner_config.max_provider_attempts = config.resource_limit.max_provider_attempts;
  runner_config.max_provider_request_bytes = config.resource_limit.max_provider_request_bytes;
  runner_config.max_provider_response_bytes = config.resource_limit.max_provider_response_bytes;
  runner_config.max_media_bytes = config.resource_limit.max_media_bytes;
  runner_config.max_memory_bytes = config.resource_limit.max_memory_bytes;
  // Spill paths are Worker-local runtime state. The Coordinator grants only
  // a byte budget and must never select a filesystem path on another host.
  // Both the node policy and the task grant must enable spill; if a future
  // node-wide cap is configured, the smaller positive limit wins.
  runner_config.spill_enabled = spill_policy.enabled;
  runner_config.spill_directory = spill_policy.directory;
  runner_config.max_spill_bytes = spill_policy.max_bytes;

  // Propagate shuffle configuration.
  runner_config.is_shuffle_task = (config.num_shuffle_partitions > 0);
  runner_config.num_shuffle_partitions = config.num_shuffle_partitions;
  runner_config.shuffle_compression_codec = config.shuffle_compression_codec;
  runner_config.preferred_output_batch_rows = config.shuffle_batch_size;
  runner_config.shuffle_output_spec = converter.shuffleOutputSpec();
  if (runner_config.is_shuffle_task) {
    if (config.shuffle_batch_serde != pangrova::protocol::v2::BATCH_SERDE_ARROW_BATCH ||
        config.shuffle_batch_size <= 0 ||
        (config.shuffle_compression_codec != "none" && config.shuffle_compression_codec != "lz4" &&
         config.shuffle_compression_codec != "zstd")) {
      throw std::runtime_error("Shuffle execution requires canonical batch serde, codec and row limit");
    }
    if (!runner_config.shuffle_output_spec ||
        runner_config.shuffle_output_spec->partition_count !=
            runner_config.num_shuffle_partitions ||
        runner_config.shuffle_output_spec->key_indices !=
            config.shuffle_partition_key_indices) {
      throw std::runtime_error(
          "Producer ExchangeRel and shuffle task partition binding disagree");
    }
  }
  runner_config.shuffle_partition_key_indices =
      config.shuffle_partition_key_indices;

  // Propagate result task flag. When true, VeloxTaskRunner skips the
  // consumer callback — Velox's PartitionedOutputNode routes output
  // through OutputBufferManager, and FlightResultServer pulls from it.
  runner_config.is_result_task = config.is_result_task;
  if (runner_config.is_result_task || runner_config.is_shuffle_task) {
    if (config.resource_limit.max_shuffle_buffer_bytes <= 0) {
      throw std::runtime_error("Serialized output requires a positive serialized-buffer grant");
    }
    runner_config.serialized_output_buffer_threshold_bytes =
        config.resource_limit.max_shuffle_buffer_bytes;
  }
  runner_config.result_column_types = config.result_column_types;
  runner_config.result_column_labels = converter.resultColumnLabels();

  // Propagate session properties for Velox connector configuration.
  runner_config.session_properties = config.session_properties;

  applySessionProperties(runner_config, config);
  // Enforce min 2 drivers: Velox serial mode (max_drivers=1) uses
  // Task::next() which deadlocks on multi-pipeline plans (JOIN/agg)
  // because the single driver thread blocks waiting for the build side.
  // Parallel mode (Task::start() + taskCompletionFuture()) delegates
  // scheduling to Velox's internal executor, handling this correctly.
  // L3-4: Allow per-query override via session property "velox.max_drivers".
  if (runner_config.max_drivers_override > 0) {
    runner_config.max_drivers =
        std::max(2, runner_config.max_drivers_override);
  } else {
    runner_config.max_drivers =
        std::max(2, config.resource_limit.max_threads);
  }
  return runner_config;
}

}  // namespace pangrova::executor
