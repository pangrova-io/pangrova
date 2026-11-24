package org.pangrova.worker.task.memory;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-task JVM reservation ledger and immutable memory demand.
 *
 * <p>Canonical Native capacity is a growth limit, while descriptor-only memory
 * and serialized buffers are admission reservations. This account owns their
 * two counters; the task lifecycle still owns admission and resource-release
 * sequencing. Native handles, execution state and release receipts are not
 * part of this memory owner.</p>
 */
@ThreadSafe
public final class TaskMemoryAccount {
    private final String taskId;
    private final String queryId;
    private final SubmitTaskRequest request;
    private final TaskDescriptor descriptor;
    private final AtomicLong allocatedMemory = new AtomicLong(0);
    private final AtomicLong allocatedBufferMemory = new AtomicLong(0);

    /** Creates one task's reservation ledger; memory demand is resolved when allocation is requested. */
    public TaskMemoryAccount(String taskId, String queryId,
                             SubmitTaskRequest request, TaskDescriptor descriptor) {
        this.taskId = taskId;
        this.queryId = queryId;
        this.request = request;
        this.descriptor = descriptor;
    }

    String taskId() { return taskId; }

    String queryId() { return queryId; }

    boolean isCanonical() { return request != null; }

    boolean requiresShuffleBuffer() {
        return request != null
                && request.taskRequest().hasExecutionOptions()
                && request.taskRequest().getExecutionOptions().hasShuffle();
    }

    long canonicalBufferBytes() {
        return request.taskRequest().getResourceGrant().getShuffleBufferBytes();
    }

    int stageId() {
        return request == null ? descriptor.stageId() : request.taskRequest().getAttempt().getStageId();
    }

    String taskType() {
        return request == null ? descriptor.taskType() : TaskRequestDtoConverter.taskType(request.taskRequest());
    }

    /** Resolves the canonical grant first; descriptor-only tasks are test wiring. */
    public long requestedNativeMemoryBytes() {
        if (request != null) {
            return request.taskRequest().getResourceGrant().getNativeMemoryBytes();
        }
        TaskDescriptor desc = descriptor;
        if (desc == null) {
            return 0;
        }
        TaskResourceLimit limit = desc.resourceLimit();
        return (limit != null) ? limit.maxMemoryBytes() : 0;
    }

    public long allocatedMemory() { return allocatedMemory.get(); }

    public void setAllocatedMemory(long bytes) { allocatedMemory.set(bytes); }

    public long allocatedBufferMemory() { return allocatedBufferMemory.get(); }

    public void setAllocatedBufferMemory(long bytes) { allocatedBufferMemory.set(bytes); }
}
