package org.pangrova.execution.binding;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.write.NativeWriteBindings;

/** Seals task and shuffle execution identity together before transport or durable issuance. */
public final class TaskControlBinding {
    private TaskControlBinding() {}

    /** Preserves an already issued task's authority. Only missing identity fields consult current authority. */
    public static SubmitTaskRequest bind(SubmitTaskRequest request, LongSupplier controlEpochSupplier,
            Function<String, ExecutionOwnerIdentity> executionOwnerResolver) {
        Objects.requireNonNull(request, "request");
        var canonical = request.taskRequest();
        var attempt = canonical.getAttempt();
        if (canonical.getExecutionOptions().getShuffle().hasExecutionBinding()) {
            var binding = canonical.getExecutionOptions().getShuffle().getExecutionBinding();
            if (binding.getPlanEpoch() != attempt.getPlanEpoch()) {
                throw new IllegalStateException("Shuffle binding plan epoch does not match task attempt");
            }
            if (attempt.hasExecutionOwner() && binding.hasExecutionOwner()
                    && !attempt.getExecutionOwner().equals(binding.getExecutionOwner())) {
                throw new IllegalStateException("Shuffle execution owner does not match task attempt");
            }
        }
        if (!requiresControlBinding(canonical, controlEpochSupplier, executionOwnerResolver)) {
            return request.withBoundTask(NativeWriteBindings.bindAttempt(canonical));
        }
        long controlEpoch = attempt.getControlEpoch() > 0
                ? attempt.getControlEpoch()
                : controlEpochSupplier != null ? controlEpochSupplier.getAsLong() : 0L;
        ExecutionOwnerIdentity executionOwner = attempt.hasExecutionOwner()
                ? ExecutionOwnerIdentity.from(attempt.getExecutionOwner())
                : executionOwnerResolver != null
                        ? executionOwnerResolver.apply(attempt.getQueryOrJobId()) : null;
        if (controlEpoch <= 0 || executionOwner == null) {
            throw new IllegalStateException(
                    "Task dispatch requires a bound control epoch and execution owner");
        }
        var canonicalOwner = org.pangrova.protocol.v2.ExecutionOwnerIdentity
                .newBuilder()
                .setOwnerId(executionOwner.ownerId())
                .setOwnerIncarnation(executionOwner.ownerIncarnation())
                .setFencingToken(executionOwner.fencingToken())
                .build();
        var bound = canonical.toBuilder()
                .setAttempt(attempt.toBuilder()
                        .setControlEpoch(controlEpoch)
                        .setExecutionOwner(canonicalOwner));
        bindShuffleExecutionOwner(bound, canonical, canonicalOwner);
        return request.withBoundTask(NativeWriteBindings.bindAttempt(bound.build()));
    }

    private static boolean requiresControlBinding(TaskRequest request, LongSupplier controlEpochSupplier,
            Function<String, ExecutionOwnerIdentity> executionOwnerResolver) {
        var attempt = request.getAttempt();
        boolean taskNeedsBinding = (controlEpochSupplier != null
                && attempt.getControlEpoch() <= 0)
                || (executionOwnerResolver != null && !attempt.hasExecutionOwner());
        return taskNeedsBinding || shuffleNeedsOwner(request);
    }

    private static boolean shuffleNeedsOwner(
            TaskRequest request) {
        return request.hasExecutionOptions()
                && request.getExecutionOptions().hasShuffle()
                && request.getExecutionOptions().getShuffle().hasExecutionBinding()
                && !request.getExecutionOptions().getShuffle()
                        .getExecutionBinding().hasExecutionOwner();
    }

    private static void bindShuffleExecutionOwner(
            TaskRequest.Builder bound,
            TaskRequest request,
            org.pangrova.protocol.v2.ExecutionOwnerIdentity owner) {
        if (!request.hasExecutionOptions()
                || !request.getExecutionOptions().hasShuffle()
                || !request.getExecutionOptions().getShuffle().hasExecutionBinding()) {
            return;
        }
        var shuffle = request.getExecutionOptions().getShuffle();
        var binding = shuffle.getExecutionBinding();
        if (binding.hasExecutionOwner() && !binding.getExecutionOwner().equals(owner)) {
            throw new IllegalStateException("Shuffle execution owner differs from admitted execution owner");
        }
        bound.setExecutionOptions(request.getExecutionOptions().toBuilder()
                .setShuffle(shuffle.toBuilder()
                        .setExecutionBinding(binding.toBuilder()
                                .setExecutionOwner(owner))));
    }
}
