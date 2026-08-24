package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;

import java.util.List;
import java.util.Map;

/**
 * Request body for submitting a task via REST API (TaskDescriptor JSON format).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SubmitTaskRequest(
    @JsonProperty("task_id") String taskId,
    @JsonProperty("query_id") String queryId,
    @JsonProperty("stage_id") int stageId,
    @JsonProperty("partition_id") int partitionId,
    @JsonProperty("task_type") String taskType,
    @JsonProperty("plan_fragment") String planFragment,
    @JsonProperty("splits") List<String> splits,
    @JsonProperty("exchange_sources") List<ExchangeSourceDescriptor> exchangeSources,
    @JsonProperty("resource_limit") ResourceLimitPayload resourceLimit,
    @JsonProperty("session_properties") Map<String, String> sessionProperties,
    @JsonProperty("output_config") OutputConfig outputConfig,
    @JsonProperty("write_config") WriteConfig writeConfig,
    @JsonProperty("dynamic_filters") List<DynamicFilterPayload> dynamicFilters,
    @JsonProperty("shuffle_config") ShuffleConfig shuffleConfig,
    @JsonProperty("split_mode") String splitMode,
    @JsonProperty("tenant_id") String tenantId,
    @JsonProperty("principal") String principal,
    @JsonProperty("vector_search") VectorSearchInfoPayload vectorSearch,
    // F-COORD-HA-04: Coordinator ownership epoch for zombie fencing.
    // Workers can reject task submissions from a stale coordinator whose
    // epoch is lower than the last-seen epoch, preventing split-brain writes.
    @JsonProperty("coordinator_epoch") Long coordinatorEpoch
) {

    public SubmitTaskRequest {
        requireNonBlank(taskId, "taskId");
        requireNonBlank(queryId, "queryId");
        if (planFragment != null && planFragment.isBlank()) {
            throw new IllegalArgumentException("planFragment must not be blank when provided");
        }
        splits = copyListOrEmpty(splits);
        exchangeSources = copyListOrEmpty(exchangeSources);
        sessionProperties = copyMapOrEmpty(sessionProperties);
        dynamicFilters = copyListOrEmpty(dynamicFilters);
    }

    /**
     * Creates a new builder with all fields unset.
     */
    public static Builder builder() {
        return new Builder();
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
    }

    private static <T> List<T> copyListOrEmpty(List<T> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    private static Map<String, String> copyMapOrEmpty(Map<String, String> values) {
        return values == null ? Map.of() : Map.copyOf(values);
    }

    /**
     * Fluent builder for {@link SubmitTaskRequest}, covering callers that only
     * populate a subset of the fields (the historical backward-compatible
     * constructor overloads are expressed through selective setter calls).
     */
    public static final class Builder {

        private String taskId;
        private String queryId;
        private int stageId;
        private int partitionId;
        private String taskType;
        private String planFragment;
        private List<String> splits;
        private List<ExchangeSourceDescriptor> exchangeSources;
        private ResourceLimitPayload resourceLimit;
        private Map<String, String> sessionProperties;
        private OutputConfig outputConfig;
        private WriteConfig writeConfig;
        private List<DynamicFilterPayload> dynamicFilters;
        private ShuffleConfig shuffleConfig;
        private String splitMode;
        private String tenantId;
        private String principal;
        private VectorSearchInfoPayload vectorSearch;
        private Long coordinatorEpoch;

        private Builder() {}

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder stageId(int stageId) {
            this.stageId = stageId;
            return this;
        }

        public Builder partitionId(int partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder planFragment(String planFragment) {
            this.planFragment = planFragment;
            return this;
        }

        public Builder splits(List<String> splits) {
            this.splits = splits;
            return this;
        }

        public Builder exchangeSources(List<ExchangeSourceDescriptor> exchangeSources) {
            this.exchangeSources = exchangeSources;
            return this;
        }

        public Builder resourceLimit(ResourceLimitPayload resourceLimit) {
            this.resourceLimit = resourceLimit;
            return this;
        }

        public Builder sessionProperties(Map<String, String> sessionProperties) {
            this.sessionProperties = sessionProperties;
            return this;
        }

        public Builder outputConfig(OutputConfig outputConfig) {
            this.outputConfig = outputConfig;
            return this;
        }

        public Builder writeConfig(WriteConfig writeConfig) {
            this.writeConfig = writeConfig;
            return this;
        }

        public Builder dynamicFilters(List<DynamicFilterPayload> dynamicFilters) {
            this.dynamicFilters = dynamicFilters;
            return this;
        }

        public Builder shuffleConfig(ShuffleConfig shuffleConfig) {
            this.shuffleConfig = shuffleConfig;
            return this;
        }

        public Builder splitMode(String splitMode) {
            this.splitMode = splitMode;
            return this;
        }

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder principal(String principal) {
            this.principal = principal;
            return this;
        }

        public Builder vectorSearch(VectorSearchInfoPayload vectorSearch) {
            this.vectorSearch = vectorSearch;
            return this;
        }

        public Builder coordinatorEpoch(Long coordinatorEpoch) {
            this.coordinatorEpoch = coordinatorEpoch;
            return this;
        }

        /**
         * Builds the request, applying the same validation as the canonical constructor.
         */
        public SubmitTaskRequest build() {
            return new SubmitTaskRequest(taskId, queryId, stageId, partitionId, taskType,
                    planFragment, splits, exchangeSources, resourceLimit, sessionProperties,
                    outputConfig, writeConfig, dynamicFilters, shuffleConfig, splitMode,
                    tenantId, principal, vectorSearch, coordinatorEpoch);
        }
    }
}
