package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.Base64;
import java.util.List;
import java.util.UUID;

import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v1.SplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds SubmitTaskRequest instances for task dispatch.
 * Supports push mode (splits embedded in request) and pull mode (empty splits).
 */
public final class TaskRequestFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestFactory.class);

    private TaskRequestFactory() {}

    /**
     * Builds a push-mode WRITE task request for DML operations (DELETE/UPDATE/MERGE).
     * Sets taskType to WRITE and attaches WriteConfig so the native worker knows
     * this is a write operation and which table to modify.
     */
    public static SubmitTaskRequest buildDmlWriteRequest(
            CoordinatorSessionContext session, byte[] planBytes,
            org.pangrova.protocol.rest.config.ResourceLimitPayload resourceLimit,
            List<DynamicFilterPayload> dynamicFilters,
            List<String> serializedSplits,
            org.pangrova.protocol.rest.config.WriteConfig writeConfig) {
        String taskId = UUID.randomUUID().toString();
        String planBase64 = Base64.getEncoder().encodeToString(planBytes);
        return new SubmitTaskRequest(
                taskId, session.queryId(), 0, 0, "WRITE",
                planBase64, serializedSplits, List.of(), resourceLimit,
                session.sessionProperties(), null, writeConfig, dynamicFilters, null, "PUSH",
                session.tenantId(), extractPrincipal(session), null, null);
    }

    /**
     * Builds a pull-mode task request for a multi-stage DAG leaf stage.
     * The task carries the plan but no splits — the Worker will pull splits on demand.
     */
    public static SubmitTaskRequest buildLeafTaskRequest(
            CoordinatorSessionContext session, byte[] planBytes, StageNode stage, int partition,
            org.pangrova.protocol.rest.config.ResourceLimitPayload resourceLimit,
            List<org.pangrova.protocol.rest.task.ExchangeSourceDescriptor> exchangeSources,
            List<DynamicFilterPayload> dynamicFilters) {
        return buildLeafTaskRequest(session, planBytes, stage, partition,
                resourceLimit, exchangeSources, dynamicFilters, null);
    }

    /**
     * Builds a push-mode task request for a multi-stage DAG leaf stage with shuffle config.
     * Splits are embedded directly so the native engine can read files immediately.
     */
    public static SubmitTaskRequest buildLeafTaskRequest(
            CoordinatorSessionContext session, byte[] planBytes, StageNode stage, int partition,
            org.pangrova.protocol.rest.config.ResourceLimitPayload resourceLimit,
            List<org.pangrova.protocol.rest.task.ExchangeSourceDescriptor> exchangeSources,
            List<DynamicFilterPayload> dynamicFilters,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig) {
        return buildLeafTaskRequest(session, planBytes, stage, partition,
                resourceLimit, exchangeSources, dynamicFilters, shuffleConfig, List.of());
    }

    /**
     * Builds a task request for a multi-stage DAG leaf stage with explicit splits.
     * When {@code splits} is non-empty, uses push mode; otherwise pull mode.
     */
    public static SubmitTaskRequest buildLeafTaskRequest(
            CoordinatorSessionContext session, byte[] planBytes, StageNode stage, int partition,
            org.pangrova.protocol.rest.config.ResourceLimitPayload resourceLimit,
            List<org.pangrova.protocol.rest.task.ExchangeSourceDescriptor> exchangeSources,
            List<DynamicFilterPayload> dynamicFilters,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            List<String> serializedSplits) {
        String taskId = UUID.randomUUID().toString();
        String planBase64 = Base64.getEncoder().encodeToString(planBytes);
        String mode = (serializedSplits != null && !serializedSplits.isEmpty()) ? "PUSH" : "PULL";
        return new SubmitTaskRequest(
                taskId, session.queryId(), stage.stageId(), partition,
                stage.taskType(), planBase64, serializedSplits != null ? serializedSplits : List.of(),
                exchangeSources, resourceLimit,
                session.sessionProperties(), null, null, dynamicFilters,
                shuffleConfig, mode,
                session.tenantId(), extractPrincipal(session), null, null);
    }

    /**
     * Validates that multi-table splits carry table_name at coordinator dispatch time.
     * Fails fast before sending to worker if any split in a multi-table plan lacks table_name.
     *
     * @param serializedSplits Base64-encoded SplitInfo proto bytes
     * @param queryId          query identifier for error context
     * @param stageId          stage identifier for error context
     */
    public static void validateMultiTableSplits(List<String> serializedSplits,
                                                 String queryId, int stageId) {
        if (serializedSplits == null || serializedSplits.size() <= 1) {
            return;
        }
        long distinctTableNames = serializedSplits.stream()
                .map(TaskRequestFactory::tryParseSplitTableName)
                .filter(name -> !name.isEmpty())
                .distinct()
                .count();
        if (distinctTableNames <= 1) {
            return;
        }
        boolean anyMissing = serializedSplits.stream()
                .map(TaskRequestFactory::tryParseSplitTableName)
                .anyMatch(String::isEmpty);
        if (anyMissing) {
            throw new IllegalStateException(
                    "Multi-table plan has splits without table_name at coordinator dispatch. "
                            + "queryId=" + queryId + " stageId=" + stageId
                            + " distinctTableNames=" + distinctTableNames);
        }
    }

    private static String extractPrincipal(CoordinatorSessionContext session) {
        return session.userIdentity() != null ? session.userIdentity().principal() : null;
    }

    private static String tryParseSplitTableName(String encoded) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            return SplitInfo.parseFrom(bytes).getTableName();
        } catch (InvalidProtocolBufferException e) {
            LOG.warn("event=split_parse_failed_for_validation error={}", e.getMessage());
            return "";
        }
    }

}
