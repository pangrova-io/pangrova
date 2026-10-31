package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.api.ShuffleServiceAddress;

/** Delegates exact task-bound data access after Coordinator control binding. */
final class CoordinatorShuffleTaskGrantBinder {

    private static final long TASK_GRANT_MIN_MARGIN_MS = 30_000;

    private final Function<ShuffleServiceAddress,
            ShuffleServiceGrpc.ShuffleServiceBlockingStub> stubProvider;
    private final long rpcTimeoutMs;

    CoordinatorShuffleTaskGrantBinder(
            Function<ShuffleServiceAddress,
                    ShuffleServiceGrpc.ShuffleServiceBlockingStub> stubProvider,
            long rpcTimeoutMs) {
        this.stubProvider = stubProvider;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    SubmitTaskRequest bind(SubmitTaskRequest request) {
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
                    ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_WRITE);
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
                    ShuffleAccessOperation.SHUFFLE_ACCESS_OPERATION_READ);
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
            ShuffleAccessOperation operation) {
        if (shuffleId.isBlank() || address.getHost().isBlank()
                || address.getPort() <= 0) {
            throw new IllegalArgumentException("shuffle task grant destination is incomplete");
        }
        var result = stubProvider.apply(new ShuffleServiceAddress(
                address.getHost(), address.getPort(), address.getTlsIdentity()))
                .withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                .renewAccess(RenewAccessRequest.newBuilder()
                        .setOperation(operation)
                        .setShuffleId(shuffleId)
                        .setExecutionOwner(owner)
                        .setTaskAttempt(attempt)
                        .build())
                .getAccessCapability();
        if (result.getToken().isEmpty()
                || result.getExpiresAtEpochMs()
                        <= System.currentTimeMillis() + TASK_GRANT_MIN_MARGIN_MS) {
            throw new SecurityException("shuffle service returned an invalid task grant");
        }
        return result;
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
