package org.pangrova.protocol.rest.task.convert;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.Split;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;

/**
 * Shared converter from REST protocol DTOs to SPI execution types.
 *
 * <p>Single source of truth for {@link SubmitTaskRequest} → {@link TaskDescriptor}
 * conversion. Both coordinator and worker modules delegate to this class,
 * eliminating duplicate conversion logic.</p>
 */
public final class TaskRequestDtoConverter {

    private TaskRequestDtoConverter() {}

    /**
     * Converts a REST SubmitTaskRequest to an SPI TaskDescriptor.
     */
    public static TaskDescriptor toTaskDescriptor(SubmitTaskRequest request) {
        QueryId queryId = new QueryId(request.queryId());
        PlanFragment fragment = toPlanFragment(request.planFragment());
        List<Split> splits = toSplits(request.splits());
        Map<String, String> props = nullSafe(request.sessionProperties());
        String taskType = (request.taskType() != null) ? request.taskType() : "COMPUTE";
        TaskResourceLimit limit = toResourceLimit(request.resourceLimit());
        return new TaskDescriptor(queryId, fragment, splits, props,
                request.stageId(), request.partitionId(), taskType, limit);
    }

    /**
     * Converts protocol ResourceLimit to SPI TaskResourceLimit.
     */
    public static TaskResourceLimit toResourceLimit(ResourceLimitPayload limit) {
        if (limit == null) {
            return TaskResourceLimit.NONE;
        }
        return new TaskResourceLimit(
                limit.maxMemoryBytes(), limit.maxThreads(),
                limit.maxSpillBytes(),
                (limit.spillDirectory() != null) ? limit.spillDirectory() : "");
    }

    /**
     * Wraps a base64-encoded plan fragment string into a PlanFragment SPI type.
     */
    public static PlanFragment toPlanFragment(String planFragmentBase64) {
        byte[] bytes = (planFragmentBase64 != null)
                ? Base64.getDecoder().decode(planFragmentBase64)
                : new byte[0];
        return new SerializedPlanFragment(bytes);
    }

    /**
     * Converts base64-encoded split strings into Split SPI types.
     */
    public static List<Split> toSplits(List<String> splitsBase64) {
        if (splitsBase64 == null || splitsBase64.isEmpty()) {
            return List.of();
        }
        return splitsBase64.stream()
                .map(b64 -> (Split) new SerializedSplit(Base64.getDecoder().decode(b64)))
                .toList();
    }

    static Map<String, String> nullSafe(Map<String, String> map) {
        return (map != null) ? map : Map.of();
    }

    /**
     * Provides raw plan bytes for native bridge integration.
     */
    public interface PlanBytesProvider {
        byte[] planBytes();
    }

    /** Wraps serialized plan bytes as a PlanFragment. */
    public record SerializedPlanFragment(byte[] planBytes) implements PlanFragment, PlanBytesProvider {

        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("serialized-fragment");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return Collections.emptyList();
        }
    }

    /** Wraps serialized split bytes as a Split marker. */
    public record SerializedSplit(byte[] splitBytes) implements Split {
    }
}
