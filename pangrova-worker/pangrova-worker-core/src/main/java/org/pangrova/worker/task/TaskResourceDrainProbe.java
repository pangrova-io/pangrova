package org.pangrova.worker.task;

import org.pangrova.protocol.v2.ResourceDrainEvidence;
import org.pangrova.protocol.v2.ResourceDrainEvidenceRequest;
import org.pangrova.protocol.v2.ResourceDrainScope;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.nativebridge.jni.NativeCommandBuilder;

/** Queries actual Native resource ownership without transporting result rows. */
final class TaskResourceDrainProbe {
    private final TaskExecutor executor;

    TaskResourceDrainProbe(TaskExecutor executor) {
        this.executor = executor;
    }

    boolean resultDrained(TaskInfo info) {
        var submitted = info.request();
        if (submitted == null || !submitted.taskRequest().getExecutionOptions().getDirectResultOutput()
                || info.getState() != TaskManager.TaskState.FINISHED || info.executorHandle() == 0) {
            return false;
        }
        var request = submitted.taskRequest();
        var command = NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE,
                request.getAttempt(), ResourceDrainEvidenceRequest.newBuilder()
                        .setSecurity(request.getSecurity())
                        .setResource(ResourceDrainScope.RESOURCE_DRAIN_SCOPE_RESULT_STREAM).build());
        var response = NativeCommandBuilder.requireSuccess(
                executor.executeCommand(info.executorHandle(), command), "resource drain evidence");
        try {
            if (!response.hasPayload() || !response.getPayload().is(ResourceDrainEvidence.class)) {
                throw new IllegalStateException("Native resource drain response lacks canonical evidence");
            }
            var evidence = response.getPayload().unpack(ResourceDrainEvidence.class);
            return validatedResultDrain(request, evidence);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Native resource drain response is malformed", failure);
        }
    }

    static boolean validatedResultDrain(org.pangrova.protocol.v2.TaskRequest request,
                                        ResourceDrainEvidence evidence) {
        boolean drained = evidence.getProducerStopped() && evidence.getAuthorityRevoked()
                && evidence.getQueueCleared() && evidence.getActiveReaders() == 0
                && evidence.getRetainedBatchOwners() == 0;
        if (!evidence.getUnknownFields().asMap().isEmpty()
                || !evidence.getAttempt().equals(request.getAttempt())
                || !evidence.getSecurity().equals(request.getSecurity())
                || evidence.getResource() != ResourceDrainScope.RESOURCE_DRAIN_SCOPE_RESULT_STREAM
                || evidence.getDrained() != drained) {
            throw new IllegalStateException("Native resource drain evidence violates its exact task authority");
        }
        return drained;
    }
}
