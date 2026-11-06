package org.pangrova.worker.execution;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryRequest;
import org.pangrova.protocol.v2.ExecutorCreationRecoveryResult;
import org.pangrova.protocol.v2.ExecutorCreationState;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;

/** Resolves only the exact pre-registered creation owner through the sole Native registry. */
final class NativeCreationRecovery {
    private NativeCreationRecovery() {}

    static void recover(NativeCreationOwners.Owner owner) {
        var identity =
                ExecutorCreationRecoveryRequest.newBuilder()
                        .setAdmissionSeal(CanonicalTaskAdmissionSeals.create(owner.request))
                        .setSecurity(owner.request.getSecurity())
                        .build();
        var command =
                RuntimeCommand.newBuilder()
                        .setProtocol(owner.request.getProtocol())
                        .setAttempt(owner.request.getAttempt())
                        .setCommandType(
                                RuntimeCommandType.RUNTIME_COMMAND_TYPE_RECOVER_EXECUTOR_CREATION)
                        .setPayload(Any.pack(identity))
                        .build();
        try {
            var response =
                    RuntimeCommandResponse.parseFrom(
                            NativeRuntime.nativeCommand(0, command.toByteArray()));
            if (!response.getSuccess()
                    || response.hasError()
                    || !response.hasPayload()
                    || !response.getUnknownFields().asMap().isEmpty()
                    || !response.getPayload().getUnknownFields().asMap().isEmpty()
                    || !response.getPayload().is(ExecutorCreationRecoveryResult.class)) {
                throw new IllegalStateException(
                        "Native creation recovery did not return exact ownership evidence");
            }
            accept(
                    owner,
                    identity,
                    response.getPayload().unpack(ExecutorCreationRecoveryResult.class));
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalStateException(
                    "Native creation recovery returned malformed evidence", malformed);
        }
    }

    static byte[] recoverTracked(
            NativeCreationOwners owners,
            org.pangrova.shuffle.pangrova.client.RemoteShuffleByteEgress egress,
            RuntimeCommand command) {
        try {
            var identity = command.getPayload().unpack(ExecutorCreationRecoveryRequest.class);
            var owner = requireTrackedOwner(owners, command, identity);
            owner.awaitConstructionExit();
            try (var claim = owner.operations.claim(false)) {
                claim.awaitReaders();
                if (!owners.contains(owner)) {
                    throw new IllegalStateException("Java creation owner was already transferred");
                }
                if (owner.releaseReceipt != null) {
                    throw new IllegalStateException("Native owner has already been released");
                }
                if (!owner.nativeClosedWithoutOwner) {
                    recover(owner);
                }
                if (owner.handle > 0) {
                    owners.publish(owner);
                } else if (owner.nativeClosedWithoutOwner) {
                    NativeCreationOwners.cleanupCallbacks(owner, egress);
                }
                byte[] receipt = recoveredReceipt(owner, identity);
                if (owner.nativeClosedWithoutOwner) {
                    owners.remove(owner);
                }
                return receipt;
            }
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalStateException("Malformed creation recovery command", malformed);
        }
    }

    private static NativeCreationOwners.Owner requireTrackedOwner(
            NativeCreationOwners owners,
            RuntimeCommand command,
            ExecutorCreationRecoveryRequest identity) {
        if (!identity.getUnknownFields().asMap().isEmpty()
                || !command.getUnknownFields().asMap().isEmpty()
                || command.getWorkerLocalStorageBindingsCount() != 0
                || !command.getPayload().getUnknownFields().asMap().isEmpty()
                || !command.getPayload()
                        .getTypeUrl()
                        .equals(
                                "type.googleapis.com/pangrova.protocol.v2.ExecutorCreationRecoveryRequest")) {
            throw new IllegalArgumentException(
                    "Creation recovery command contains noncanonical fields");
        }
        var owner = owners.forAttempt(command.getAttempt());
        if (owner == null
                || !identity.getSecurity().equals(owner.request.getSecurity())
                || !identity.getAdmissionSeal()
                        .equals(CanonicalTaskAdmissionSeals.create(owner.request))
                || !command.getProtocol().equals(owner.request.getProtocol())) {
            throw new IllegalStateException("Creation recovery has no exact registered Java owner");
        }
        return owner;
    }

    private static byte[] recoveredReceipt(
            NativeCreationOwners.Owner owner, ExecutorCreationRecoveryRequest identity) {
        var result =
                ExecutorCreationRecoveryResult.newBuilder()
                        .setIdentity(identity)
                        .setState(
                                owner.handle > 0
                                        ? ExecutorCreationState
                                                .EXECUTOR_CREATION_STATE_OWNER_PRESENT
                                        : ExecutorCreationState
                                                .EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER)
                        .setExecutorHandle(owner.handle)
                        .build();
        return RuntimeCommandResponse.newBuilder()
                .setSuccess(true)
                .setPayload(Any.pack(result))
                .build()
                .toByteArray();
    }

    static void accept(
            NativeCreationOwners.Owner owner,
            ExecutorCreationRecoveryRequest expected,
            ExecutorCreationRecoveryResult actual) {
        if (!actual.hasIdentity()
                || !actual.getIdentity().equals(expected)
                || !actual.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalStateException(
                    "Native creation recovery identity does not match its creation transaction");
        }
        if (actual.getState() == ExecutorCreationState.EXECUTOR_CREATION_STATE_OWNER_PRESENT
                && actual.getExecutorHandle() > 0) {
            if (owner.nativeClosedWithoutOwner
                    || (owner.handle != 0 && owner.handle != actual.getExecutorHandle())) {
                throw new IllegalStateException(
                        "Native creation recovery changed its actual registered handle");
            }
            // Assign before any further Java publication allocation can fail.
            owner.handle = actual.getExecutorHandle();
        } else if (actual.getState()
                        == ExecutorCreationState.EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER
                && actual.getExecutorHandle() == 0
                && owner.handle == 0
                && owner.releaseReceipt == null) {
            owner.nativeClosedWithoutOwner = true;
        } else {
            throw new IllegalStateException(
                    "Native construction has not supplied a completed owner/no-owner fact");
        }
    }
}
