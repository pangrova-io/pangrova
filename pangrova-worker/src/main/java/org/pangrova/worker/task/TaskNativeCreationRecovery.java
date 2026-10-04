package org.pangrova.worker.task;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryRequest;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryResult;
import org.pangrova.protocol.v2.ExecutorCreationState;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.spi.execution.TaskExecutor;

/** Reconciles an exited but unresolved creation before the existing release path can refund it. */
final class TaskNativeCreationRecovery {
    private TaskNativeCreationRecovery() {}

    static boolean seal(TaskInfo info, TaskExecutor executor) {
        try {
            return info.sealNativeCreationWithoutOwnerOrAwaitExit();
        } catch (java.util.concurrent.CompletionException unresolved) {
            if (executor == null || info.request() == null) throw unresolved;
            recover(info, executor);
            return info.sealNativeCreationWithoutOwnerOrAwaitExit();
        }
    }

    private static void recover(TaskInfo info, TaskExecutor executor) {
        var request = info.request().taskRequest();
        var identity = ExecutorCreationRecoveryRequest.newBuilder()
                .setAdmissionSeal(CanonicalTaskAdmissionSeals.create(request)).setSecurity(request.getSecurity()).build();
        byte[] receipt = info.stagedCreationRecoveryReceipt();
        if (receipt == null) {
            var command = RuntimeCommand.newBuilder().setProtocol(request.getProtocol()).setAttempt(request.getAttempt())
                    .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_RECOVER_EXECUTOR_CREATION)
                    .setPayload(Any.pack(identity)).build();
            receipt = executor.executeCommand(0, command.toByteArray());
            // Reference transfer precedes decoding/allocation. A retry consumes
            // this actual response, not another request to an erased Java owner.
            info.stageCreationRecoveryReceipt(receipt);
        }
        try {
            var response = RuntimeCommandResponse.parseFrom(receipt);
            if (!response.getSuccess() || response.hasError() || !response.getUnknownFields().asMap().isEmpty()
                    || !response.getPayload().getUnknownFields().asMap().isEmpty()
                    || !response.hasPayload() || !response.getPayload().is(ExecutorCreationRecoveryResult.class)) {
                throw new IllegalStateException("Task creation recovery lacks canonical ownership evidence");
            }
            var actual = response.getPayload().unpack(ExecutorCreationRecoveryResult.class);
            accept(info, identity, actual);
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalStateException("Malformed task creation ownership evidence", malformed);
        }
    }

    static void accept(TaskInfo info, ExecutorCreationRecoveryRequest expected, ExecutorCreationRecoveryResult actual) {
        if (!actual.hasIdentity() || !actual.getIdentity().equals(expected) || !actual.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalStateException("Recovered task creation identity changed");
        }
        if (actual.getState() == ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT && actual.getExecutorHandle() > 0) {
            info.resolveNativeCreationFailure(actual.getExecutorHandle());
        } else if (actual.getState() == ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER
                && actual.getExecutorHandle() == 0) {
            info.resolveNativeCreationFailure(0);
        } else throw new IllegalStateException("Recovered task creation remains unresolved");
    }
}
