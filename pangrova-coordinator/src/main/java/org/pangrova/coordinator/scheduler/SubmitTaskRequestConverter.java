package org.pangrova.coordinator.scheduler;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;

/**
 * Coordinator-side facade for REST DTO → SPI conversion.
 *
 * <p>Core conversion logic is delegated to the shared
 * {@link TaskRequestDtoConverter} in pangrova-protocol.
 * This class adds coordinator-specific helpers (e.g. result endpoint
 * construction from {@link WorkerInfo}).</p>
 */
public final class SubmitTaskRequestConverter {

    private SubmitTaskRequestConverter() {}

    /**
     * Converts a REST DTO to an SPI task descriptor with full field preservation.
     */
    public static TaskDescriptor toTaskDescriptor(SubmitTaskRequest request) {
        return TaskRequestDtoConverter.toTaskDescriptor(request);
    }

    /**
     * Builds a result endpoint from worker info and task id.
     */
    public static ResultEndpoint buildResultEndpoint(WorkerInfo worker, String taskId) {
        return new ResultEndpoint(worker.host(), worker.transportPort(), taskId, "grpc-transport");
    }

    /**
     * Converts protocol ResourceLimit to SPI TaskResourceLimit.
     */
    public static TaskResourceLimit toResourceLimit(ResourceLimitPayload limit) {
        return TaskRequestDtoConverter.toResourceLimit(limit);
    }

    /**
     * Re-exports the shared SerializedPlanFragment type for backward compatibility.
     * Coordinator tests that reference this inner type continue to compile.
     */
    public static TaskRequestDtoConverter.SerializedPlanFragment serializedPlanFragment(byte[] bytes) {
        return new TaskRequestDtoConverter.SerializedPlanFragment(bytes);
    }
}
