package org.pangrova.protocol.rest.task.convert;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.spi.query.identity.QueryId;
import org.pangrova.protocol.execution.Split;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;

/**
 * Shared converter from canonical task protocol to SPI scheduling types.
 *
 * <p>The REST object is only a byte envelope; conversion reads the parsed
 * canonical {@link TaskRequest} and never reconstructs it from JSON fields.</p>
 */
public final class TaskRequestDtoConverter {

    private TaskRequestDtoConverter() {}

    /**
     * Converts a REST SubmitTaskRequest to an SPI TaskDescriptor.
     */
    public static TaskDescriptor toTaskDescriptor(SubmitTaskRequest request) {
        TaskRequest canonical = request.taskRequest();
        QueryId queryId = new QueryId(canonical.getAttempt().getQueryOrJobId());
        PlanFragment fragment = new SerializedPlanFragment(
                canonical.getSubstraitPlan().toByteArray());
        List<Split> splits = canonical.getSplitsList().stream()
                .map(split -> (Split) new SerializedSplit(split.toByteArray()))
                .toList();
        Map<String, String> props = canonical.hasExecutionOptions()
                ? canonical.getExecutionOptions().getSessionPropertiesMap()
                : Map.of();
        String taskType = taskType(canonical.getTaskKind(), canonical);
        TaskResourceLimit limit = toResourceLimit(canonical.hasResourceGrant()
                ? canonical.getResourceGrant() : null);
        return new TaskDescriptor(queryId, fragment, splits, props,
                canonical.getAttempt().getStageId(),
                canonical.getAttempt().getPartitionId(), taskType, limit);
    }

    /**
     * Converts protocol ResourceLimit to SPI TaskResourceLimit.
     */
    public static TaskResourceLimit toResourceLimit(StageResourceGrant grant) {
        if (grant == null) {
            return TaskResourceLimit.NONE;
        }
        return new TaskResourceLimit(
                grant.getNativeMemoryBytes(), grant.getCpuSlots(),
                grant.getSpillBytes(), "");
    }

    static Map<String, String> nullSafe(Map<String, String> map) {
        return (map != null) ? map : Map.of();
    }

    private static String taskType(TaskKind kind, TaskRequest request) {
        if (request.hasExecutionOptions()
                && request.getExecutionOptions().getDirectResultOutput()) {
            return "OUTPUT";
        }
        return switch (kind) {
            case TASK_KIND_READ, TASK_KIND_SHUFFLE_READ, TASK_KIND_SHUFFLE_WRITE ->
                    "COMPUTE";
            case TASK_KIND_WRITE -> "WRITE";
            case TASK_KIND_BACKGROUND_BUILD -> "BACKGROUND_BUILD";
            case TASK_KIND_BACKGROUND_VALIDATE -> "BACKGROUND_VALIDATE";
            case TASK_KIND_BACKGROUND_EXPORT -> "BACKGROUND_EXPORT";
            default -> throw new IllegalArgumentException(
                    "Unsupported canonical task kind: " + kind);
        };
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
