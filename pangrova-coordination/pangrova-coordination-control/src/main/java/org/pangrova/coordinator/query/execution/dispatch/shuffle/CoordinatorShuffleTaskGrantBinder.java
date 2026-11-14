package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.api.enums.ShuffleDataOperation;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.coordinator.ShuffleTaskAccess;
import org.pangrova.shuffle.api.identity.ExchangeAttemptFence;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

/** Delegates exact task-bound data access after Coordinator control binding. */
@org.pangrova.common.api.annotation.Internal
public final class CoordinatorShuffleTaskGrantBinder {
    private final ShuffleTaskAccess grants;

    public CoordinatorShuffleTaskGrantBinder(ShuffleTaskAccess grants) {
        this.grants = java.util.Objects.requireNonNull(grants, "grants");
    }

    public SubmitTaskRequest bind(SubmitTaskRequest request) {
        TaskRequest canonical = request.taskRequest();
        TaskAttemptId attempt = canonical.getAttempt();
        TaskRequest.Builder bound = canonical.toBuilder();
        if (canonical.hasExecutionOptions()
                && canonical.getExecutionOptions().hasShuffle()
                && canonical.getExecutionOptions().getShuffle().hasExecutionBinding()) {
            var options = canonical.getExecutionOptions();
            var shuffle = options.getShuffle();
            var binding = shuffle.getExecutionBinding();
            requireGrantAttempt(attempt, binding.getExecutionOwner());
            if (binding.getPlanEpoch() != attempt.getPlanEpoch()) {
                throw new SecurityException("shuffle producer grant plan epoch mismatch");
            }
            var granted = grantTaskAccess(binding.getCommitAddress(),
                    binding.getShuffleId(), binding.getExecutionOwner(), attempt,
                    ShuffleDataOperation.WRITE);
            bound.setExecutionOptions(options.toBuilder().setShuffle(
                    shuffle.toBuilder().setExecutionBinding(binding.toBuilder()
                            .setWriteCapability(granted))));
        }
        for (int index = 0; index < canonical.getExchangeInputsCount(); index++) {
            var input = canonical.getExchangeInputs(index);
            if (!input.hasExternal()) {
                continue;
            }
            var external = input.getExternal();
            requireGrantAttempt(attempt, external.getExecutionOwner());
            var granted = grantTaskAccess(external.getCommitAddress(),
                    external.getShuffleId(), external.getExecutionOwner(), attempt,
                    ShuffleDataOperation.READ);
            bound.setExchangeInputs(index, input.toBuilder().setExternal(
                    external.toBuilder().setReadCapability(granted)));
        }
        return SubmitTaskRequest.from(bound.build());
    }

    private ShuffleAccessCapability grantTaskAccess(
            org.pangrova.protocol.v2.ShuffleServiceAddress address,
            String shuffleId,
            ExecutionOwnerIdentity owner,
            TaskAttemptId attempt,
            ShuffleDataOperation operation) {
        if (shuffleId.isBlank() || address.getHost().isBlank()
                || address.getPort() <= 0) {
            throw new IllegalArgumentException("shuffle task grant destination is incomplete");
        }
        var worker = attempt.getWorkerExecutionIdentity();
        var identity = new ExchangeTaskAttempt(attempt.getQueryOrJobId(), attempt.getStageId(),
                attempt.getPartitionId(), attempt.getTaskId(), attempt.getControlEpoch(),
                new ExchangeAttemptFence(attempt.getPlanEpoch(), attempt.getAttemptId(), attempt.getFencingToken()),
                new ShuffleExecutionOwner(owner.getOwnerId(), owner.getOwnerIncarnation(), owner.getFencingToken()),
                worker.getAdmissionDomainId(), worker.getIncarnationId());
        var grant = grants.grant(new ShuffleServiceAddress(address.getHost(), address.getPort(), address.getTlsIdentity()),
                shuffleId, identity, operation);
        return ShuffleAccessCapability.newBuilder().setToken(com.google.protobuf.ByteString.copyFrom(grant.token()))
                .setExpiresAtEpochMs(grant.expiresAtEpochMs()).build();
    }

    private static void requireGrantAttempt(
            TaskAttemptId attempt, ExecutionOwnerIdentity owner) {
        if (attempt.getQueryOrJobId().isBlank() || attempt.getStageId() < 0
                || attempt.getPartitionId() < 0 || attempt.getPlanEpoch() <= 0
                || attempt.getFencingToken() <= 0 || attempt.getTaskId().isBlank()
                || attempt.getControlEpoch() <= 0 || !attempt.hasExecutionOwner()
                || !attempt.getExecutionOwner().equals(owner)) {
            throw new SecurityException("shuffle task grant requires a bound exact attempt");
        }
    }
}
