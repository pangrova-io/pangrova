package org.pangrova.coordinator.scheduler;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.protocol.execution.TaskDescriptor;

/**
 * Coordinator-side facade for REST DTO → SPI conversion.
 *
 * <p>Core conversion logic is delegated to the shared
 * {@link TaskRequestDtoConverter} in pangrova-protocol.
 * Result endpoints are issued only by the admitted Worker producer.</p>
 */
public final class SubmitTaskRequestConverter {

    private SubmitTaskRequestConverter() {}

    /**
     * Converts a REST DTO to an SPI task descriptor with full field preservation.
     */
    public static TaskDescriptor toTaskDescriptor(SubmitTaskRequest request) {
        return TaskRequestDtoConverter.toTaskDescriptor(request);
    }

}
