package org.pangrova.ai.worker.invocation;

import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;

import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/** Worker-authoritative resource admission, isolated by the complete canonical task attempt. */
final class AiInvocationBudgetManager {
    private static final int MAX_TRACKED_TASKS = 50_000;
    private final ConcurrentHashMap<TaskKey, AiTaskBudget> taskBudgets = new ConcurrentHashMap<>();
    private final Semaphore taskSlots;
    private final AiModelBudgetRegistry modelBudgets = new AiModelBudgetRegistry();

    AiInvocationBudgetManager() {
        this(MAX_TRACKED_TASKS);
    }

    AiInvocationBudgetManager(int maxTrackedTasks) {
        if (maxTrackedTasks <= 0) {
            throw new IllegalArgumentException("maxTrackedTasks must be positive");
        }
        this.taskSlots = new Semaphore(maxTrackedTasks);
    }

    AiPhysicalInvocationAdmission acquire(
            AiExecutionContext context,
            AiInvocationRequest request,
            AiCapabilityDescriptor capability,
            int items,
            int providerRequests,
            int concurrentCalls,
            long mediaBytes) {
        BoundAiInvocation binding =
                AiExecutionContext.boundInvocation()
                        .orElseThrow(
                                () ->
                                        new AiInvocationException(
                                                AiInvocationException.Category.AUTHORIZATION,
                                                false,
                                                "AI invocation has no exact Native OperatorBinding"));
        AiInvocationBudgetValidator.validate(context, request, binding);
        AiTaskBudget task = taskBudget(context);
        AiInvocationReservation reservation =
                AiInvocationReservation.create(
                        capability, binding, items, providerRequests, concurrentCalls, mediaBytes);
        AiModelBudgetRegistry.Lease model =
                modelBudgets.retain(
                        request.modelName(),
                        request.modelVersion(),
                        HexFormat.of().formatHex(request.modelSignatureSha256()),
                        capability.capabilityId(),
                        capability.resourceLimits().maxConcurrency());
        try {
            AiTaskBudget.Binding budgets =
                    task.reserveLogical(binding, request, capability, reservation);
            return new AiPhysicalInvocationAdmission(
                    task,
                    budgets,
                    model,
                    reservation,
                    binding.requestConstraints(),
                    request.deadline(),
                    () ->
                            removeRetired(
                                    new TaskKey(context.tenantId(), context.taskAttempt()), task));
        } catch (RuntimeException failure) {
            model.close();
            throw failure;
        }
    }

    AiTaskBudget registerTask(
            AiExecutionContext context,
            java.util.List<org.pangrova.protocol.v2.OperatorBinding> bindings) {
        TaskKey key = new TaskKey(context.tenantId(), context.taskAttempt());
        AiTaskBudget existing = taskBudgets.get(key);
        if (existing != null) {
            throw duplicateAuthority();
        }
        if (!taskSlots.tryAcquire()) {
            existing = taskBudgets.get(key);
            if (existing != null) {
                throw duplicateAuthority();
            }
            throw budgetExceeded("Worker AI task budget registry is at capacity");
        }
        boolean inserted = false;
        try {
            AiTaskBudget candidate = new AiTaskBudget(context, bindings);
            existing = taskBudgets.putIfAbsent(key, candidate);
            inserted = existing == null;
            if (!inserted) {
                throw duplicateAuthority();
            }
            return candidate;
        } finally {
            if (!inserted) {
                taskSlots.release();
            }
        }
    }

    AiTaskBudget taskBudget(AiExecutionContext context) {
        AiTaskBudget task = taskBudgets.get(new TaskKey(context.tenantId(), context.taskAttempt()));
        if (task == null) {
            throw new AiInvocationException(
                    AiInvocationException.Category.AUTHORIZATION,
                    false,
                    "AI callback has no live registered Worker task authority");
        }
        task.requireSameGrant(context);
        task.requireActive();
        return task;
    }

    private static AiInvocationException duplicateAuthority() {
        return new AiInvocationException(
                AiInvocationException.Category.AUTHORIZATION,
                false,
                "Worker already owns this exact canonical task attempt");
    }

    void cancelTask(String tenantId, TaskAttemptId attempt) {
        AiTaskBudget task = taskBudgets.get(new TaskKey(tenantId, attempt));
        if (task != null) {
            task.revoke();
        }
    }

    ResourceUsage releaseTask(String tenantId, TaskAttemptId attempt) {
        AiTaskBudget task = taskBudgets.get(new TaskKey(tenantId, attempt));
        if (task == null) {
            return ResourceUsage.getDefaultInstance();
        }
        task.terminate();
        removeRetired(new TaskKey(tenantId, attempt), task);
        return task.usage();
    }

    ResourceUsage usage(String tenantId, TaskAttemptId attempt) {
        AiTaskBudget task = taskBudgets.get(new TaskKey(tenantId, attempt));
        return task == null ? ResourceUsage.getDefaultInstance() : task.usage();
    }

    void chargeGovernedFileBytes(AiExecutionContext context, long bytes) {
        taskBudget(context).chargeGovernedFileBytes(bytes);
    }

    private void removeRetired(TaskKey key, AiTaskBudget task) {
        if (task.isTerminated() && task.isIdle() && taskBudgets.remove(key, task)) {
            taskSlots.release();
        }
    }

    static AiInvocationException budgetExceeded(String message) {
        return new AiInvocationException(
                AiInvocationException.Category.BUDGET_EXCEEDED, false, message);
    }

    private record TaskKey(String tenantId, TaskAttemptId attempt) {}
}
