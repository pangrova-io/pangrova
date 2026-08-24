package org.pangrova.worker.task.convert;

import java.util.List;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.Split;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;

/**
 * Delegates REST DTO → SPI conversion to the shared
 * {@link TaskRequestDtoConverter} in pangrova-protocol.
 *
 * <p>Kept as a thin facade so existing worker-side call sites
 * (e.g. {@code TaskResource}) remain unchanged.</p>
 */
public final class TaskRequestConverter {

    private TaskRequestConverter() {}

    /** Converts a REST SubmitTaskRequest to an SPI TaskDescriptor. */
    public static TaskDescriptor toTaskDescriptor(SubmitTaskRequest request) {
        return TaskRequestDtoConverter.toTaskDescriptor(request);
    }

    /** Converts protocol ResourceLimit to SPI TaskResourceLimit. */
    public static TaskResourceLimit toResourceLimit(ResourceLimitPayload limit) {
        return TaskRequestDtoConverter.toResourceLimit(limit);
    }

    /** Wraps a base64-encoded plan fragment string into a PlanFragment SPI type. */
    public static PlanFragment toPlanFragment(String planFragmentBase64) {
        return TaskRequestDtoConverter.toPlanFragment(planFragmentBase64);
    }

    /** Converts base64-encoded split strings into Split SPI types. */
    public static List<Split> toSplits(List<String> splitsBase64) {
        return TaskRequestDtoConverter.toSplits(splitsBase64);
    }
}
