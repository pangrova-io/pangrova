package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.time.Clock;
import java.util.regex.Pattern;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.security.CapabilityScope;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.pangrova.shuffle.security.enums.CapabilityOperation;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;

/** Validates exact task grants and renews access against the durable lifecycle authority. */
final class ShuffleAccessControl {
    private static final Pattern RESOURCE_ID = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    private final ShuffleServiceConfig config;
    private final ShuffleLifecycleStore lifecycle;
    private final ShuffleCapabilityAuthority capabilities;
    private final Clock clock;

    ShuffleAccessControl(
            ShuffleServiceConfig config,
            ShuffleLifecycleStore lifecycle,
            ShuffleCapabilityAuthority capabilities,
            Clock clock) {
        this.config = config;
        this.lifecycle = lifecycle;
        this.capabilities = capabilities;
        this.clock = clock;
    }

    static boolean validShuffleId(String shuffleId) {
        return RESOURCE_ID.matcher(shuffleId).matches();
    }

    RenewAccessResponse renewAccess(RenewAccessRequest request, boolean coordinatorPeer) {
        ShuffleAccessOperation operation = request.getOperation();
        CapabilityOperation scopeOperation = renewalOperation(operation);
        boolean control = operation == ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_FINALIZE
                || operation == ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP;
        if (control && !coordinatorPeer) {
            throw new SecurityException("shuffle control renewal requires Coordinator mTLS identity");
        }
        ExecutionOwnerIdentity owner = request.getExecutionOwner();
        if (!validOwner(owner)) {
            throw new SecurityException("shuffle renewal owner is invalid");
        }
        long now = clock.millis();
        long expiry = Math.addExact(now, config.security().capabilityTtl().toMillis());
        if (operation == ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_CLEANUP) {
            return renewCleanupAccess(request, owner, now, expiry);
        }
        return renewShuffleAccess(request, owner, scopeOperation,
                control, coordinatorPeer, now, expiry);
    }

    private RenewAccessResponse renewCleanupAccess(
            RenewAccessRequest request,
            ExecutionOwnerIdentity owner,
            long now,
            long expiry) {
        if (!request.getShuffleId().isEmpty() || request.getQueryId().isBlank()
                || request.hasTaskAttempt()) {
            throw new IllegalArgumentException("cleanup renewal scope is invalid");
        }
        CapabilityScope scope = CapabilityScope.forQuery(request.getQueryId(), owner);
        if (request.getPreviousCapability().getToken().isEmpty()) {
            lifecycle.recoverCleanupAccess(request.getQueryId(), owner, expiry);
        } else {
            capabilities.verify(request.getPreviousCapability(), scope, now);
            lifecycle.renewAccess(request.getQueryId(), "", owner, expiry);
        }
        return renewalResponse(scope, expiry);
    }

    private RenewAccessResponse renewShuffleAccess(
            RenewAccessRequest request,
            ExecutionOwnerIdentity owner,
            CapabilityOperation operation,
            boolean control,
            boolean coordinatorPeer,
            long now,
            long expiry) {
        if (!request.getQueryId().isEmpty() || request.getShuffleId().isBlank()
                || control && request.hasTaskAttempt()
                || !control && !coordinatorPeer && !request.hasTaskAttempt()) {
            throw new IllegalArgumentException("shuffle renewal scope is invalid");
        }
        boolean recovering = control && request.getPreviousCapability().getToken().isEmpty();
        if (recovering) {
            lifecycle.recoverFinalizeAccess(request.getShuffleId(), owner, expiry);
        }
        Allocation allocation = requireAllocation(request.getShuffleId());
        if (!allocation.owner().equals(owner)
                || allocation.state() == ShuffleAllocationState.CLEANING) {
            throw new SecurityException("shuffle renewal owner is fenced");
        }
        if (!control && request.hasTaskAttempt()) {
            requireRenewalAttempt(request.getOperation(), allocation,
                    request.getTaskAttempt());
        }
        CapabilityScope scope = control
                ? renewControlAccess(request, allocation, owner, operation,
                        recovering, now, expiry)
                : renewDataAccess(request, allocation, owner, operation,
                        coordinatorPeer, now, expiry);
        return renewalResponse(scope, expiry);
    }

    private CapabilityScope renewControlAccess(
            RenewAccessRequest request,
            Allocation allocation,
            ExecutionOwnerIdentity owner,
            CapabilityOperation operation,
            boolean recovering,
            long now,
            long expiry) {
        CapabilityScope scope = CapabilityScope.forShuffle(operation,
                allocation.identity().shuffleId(), owner);
        if (!recovering) {
            capabilities.verify(request.getPreviousCapability(), scope, now);
            lifecycle.renewAccess(allocation.identity().queryId(),
                    allocation.identity().shuffleId(), owner, expiry);
        }
        return scope;
    }

    private CapabilityScope renewDataAccess(
            RenewAccessRequest request,
            Allocation allocation,
            ExecutionOwnerIdentity owner,
            CapabilityOperation operation,
            boolean coordinatorPeer,
            long now,
            long expiry) {
        CapabilityScope scope;
        if (coordinatorPeer && request.hasTaskAttempt()) {
            if (!request.getPreviousCapability().getToken().isEmpty()) {
                throw new SecurityException("Coordinator task grant cannot reuse a bearer ticket");
            }
            scope = CapabilityScope.forTask(operation,
                    allocation.identity().shuffleId(), owner,
                    request.getTaskAttempt());
            lifecycle.renewAccess(allocation.identity().queryId(),
                    allocation.identity().shuffleId(), owner, expiry);
        } else if (coordinatorPeer) {
            // Stage tickets are retained only in the Coordinator control plane.
            // Data RPCs and Worker renewal never accept them.
            scope = CapabilityScope.forShuffle(operation,
                    allocation.identity().shuffleId(), owner);
            capabilities.verify(request.getPreviousCapability(), scope, now);
            lifecycle.renewAccess(allocation.identity().queryId(),
                    allocation.identity().shuffleId(), owner, expiry);
        } else {
            scope = CapabilityScope.forTask(operation,
                    allocation.identity().shuffleId(), owner,
                    request.getTaskAttempt());
            capabilities.verify(request.getPreviousCapability(), scope, now);
            lifecycle.renewAccess(allocation.identity().queryId(),
                    allocation.identity().shuffleId(), owner, expiry);
        }
        return scope;
    }

    private RenewAccessResponse renewalResponse(CapabilityScope scope, long expiry) {
        return RenewAccessResponse.newBuilder()
                .setAccessCapability(capabilities.issue(scope, expiry)).build();
    }

    private static CapabilityOperation renewalOperation(ShuffleAccessOperation operation) {
        return switch (operation) {
            case SHUFFLE_ACCESS_OPERATION_READ -> CapabilityOperation.READ;
            case SHUFFLE_ACCESS_OPERATION_WRITE -> CapabilityOperation.WRITE;
            case SHUFFLE_ACCESS_OPERATION_FINALIZE -> CapabilityOperation.FINALIZE;
            case SHUFFLE_ACCESS_OPERATION_CLEANUP -> CapabilityOperation.CLEANUP;
            default -> throw new IllegalArgumentException("shuffle renewal operation is invalid");
        };
    }

    private static void requireRenewalAttempt(
            ShuffleAccessOperation operation,
            Allocation allocation,
            TaskAttemptId attempt) {
        if (operation == ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE) {
            requireProducerAttempt(allocation, attempt, -1);
        } else {
            requireConsumerAttempt(allocation, attempt);
        }
    }

    Allocation requireAllocation(String shuffleId) {
        if (!RESOURCE_ID.matcher(shuffleId).matches()) {
            throw new IllegalArgumentException("shuffle id is invalid");
        }
        return lifecycle.findAllocation(shuffleId)
                .orElseThrow(() -> new ShuffleServiceException(
                        FailureKind.NOT_FOUND, "shuffle does not exist"));
    }

    void verify(
            CapabilityOperation operation,
            Allocation allocation,
            org.pangrova.protocol.v2.ShuffleAccessCapability capability) {
        if (operation == CapabilityOperation.READ || operation == CapabilityOperation.WRITE) {
            throw new IllegalArgumentException("data operation requires exact task attempt");
        }
        capabilities.verify(capability, CapabilityScope.forShuffle(operation,
                allocation.identity().shuffleId(), allocation.owner()), clock.millis());
    }

    void verifyTask(
            CapabilityOperation operation,
            Allocation allocation,
            TaskAttemptId attempt,
            org.pangrova.protocol.v2.ShuffleAccessCapability capability) {
        capabilities.verify(capability, CapabilityScope.forTask(operation,
                allocation.identity().shuffleId(), allocation.owner(), attempt),
                clock.millis());
    }

    void verifyTask(
            CapabilityOperation operation,
            String shuffleId,
            TaskAttemptId attempt,
            org.pangrova.protocol.v2.ShuffleAccessCapability capability) {
        if (!validShuffleId(shuffleId)) {
            throw new IllegalArgumentException("shuffle id is invalid");
        }
        capabilities.verify(capability, CapabilityScope.forTask(
                operation, shuffleId, attempt.getExecutionOwner(), attempt), clock.millis());
    }

    org.pangrova.protocol.v2.ShuffleAccessCapability issue(
            CapabilityOperation operation,
            String shuffleId,
            ExecutionOwnerIdentity owner,
            long expiry) {
        return capabilities.issue(CapabilityScope.forShuffle(
                operation, shuffleId, owner), expiry);
    }

    static boolean validOwner(ExecutionOwnerIdentity owner) {
        return owner != null && !owner.getOwnerId().isBlank()
                && !owner.getOwnerIncarnation().isBlank() && owner.getFencingToken() > 0;
    }

    static void requireProducerAttempt(
            Allocation allocation,
            TaskAttemptId attempt,
            int outputPartitionId) {
        if (!attemptIdentityMatches(allocation, attempt)
                || !attemptFenceValid(attempt)
                || !producerPartitionValid(allocation, attempt, outputPartitionId)) {
            throw new SecurityException("producer attempt does not match allocation fence");
        }
    }

    private static boolean attemptIdentityMatches(
            Allocation allocation,
            TaskAttemptId attempt) {
        return validOwner(attempt.getExecutionOwner())
                && attempt.getExecutionOwner().equals(allocation.owner())
                && attempt.getQueryOrJobId().equals(allocation.identity().queryId())
                && attempt.getPlanEpoch() == allocation.identity().planEpoch();
    }

    private static boolean attemptFenceValid(TaskAttemptId attempt) {
        return attempt.getFencingToken() > 0 && attempt.getControlEpoch() > 0
                && !attempt.getTaskId().isBlank();
    }

    private static boolean producerPartitionValid(
            Allocation allocation,
            TaskAttemptId attempt,
            int outputPartitionId) {
        return attempt.getStageId() == allocation.identity().stageId()
                && attempt.getPartitionId() >= 0
                && attempt.getPartitionId() < allocation.shape().expectedProducerCount()
                && (outputPartitionId == -1 || outputPartitionId >= 0
                        && outputPartitionId < allocation.shape().partitionCount());
    }

    static void requireConsumerAttempt(
            Allocation allocation,
            TaskAttemptId attempt) {
        // The consumer stage has its own plan epoch; only query and live owner
        // must match the producer allocation before its exact grant is checked.
        if (!attempt.getExecutionOwner().equals(allocation.owner())
                || !attempt.getQueryOrJobId().equals(allocation.identity().queryId())) {
            throw new SecurityException("consumer attempt does not match allocation fence");
        }
        requireConsumerIdentity(attempt);
    }

    static void requireConsumerIdentity(TaskAttemptId attempt) {
        if (!validOwner(attempt.getExecutionOwner()) || attempt.getQueryOrJobId().isBlank()
                || attempt.getStageId() < 0 || attempt.getPartitionId() < 0
                || attempt.getPlanEpoch() <= 0 || !attemptFenceValid(attempt)) {
            throw new SecurityException("consumer attempt identity is incomplete");
        }
    }
}
