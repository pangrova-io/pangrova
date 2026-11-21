package org.pangrova.execution.task;

/** Accepted task dispatch and its exact retained request, shared by stage cleanup and retry. */
public record StageDispatchRecord(
        int stageId,
        DispatchResult result,
        int workerHttpPort,
        int workerTransportPort,
        org.pangrova.protocol.rest.task.SubmitTaskRequest request) {

    /** Creates a record when the serialized request is not retained. */
    public StageDispatchRecord(
            int stageId,
            DispatchResult result,
            int workerHttpPort,
            int workerTransportPort) {
        this(stageId, result, workerHttpPort, workerTransportPort, null);
    }
}

