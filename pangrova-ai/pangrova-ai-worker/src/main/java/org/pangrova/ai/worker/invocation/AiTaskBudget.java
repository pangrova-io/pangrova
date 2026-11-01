package org.pangrova.ai.worker.invocation;

import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.spi.ai.capability.AiCapabilityDescriptor;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.spi.ai.invocation.AiInvocationRequest;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.ai.worker.context.AiProviderCallContext;

import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

/** One full task attempt's atomic operator/model/physical-request resource ledger. */
final class AiTaskBudget {
    final long deadlineEpochMs;
    final Semaphore concurrency;
    private final AiExecutionContext context;
    private final AiResourceLedger resources;
    private final Map<String, OperatorBudget> operators = new HashMap<>();
    private final Map<ModelKey, ModelTaskBudget> models = new HashMap<>();
    private final Map<String, OperatorBinding> admittedBindings;
    private final Set<Thread> permitWaiters = new HashSet<>();
    private final Set<AiProviderCallContext.LocalExchange> exchanges = new HashSet<>();
    private boolean revoked;
    private boolean retired;
    private int activeAdmissions;
    private long reservedItems;
    private long reservedMediaBytes;
    private long providerItems;
    private long providerAttempts;
    private long providerRequestBytes;
    private long providerResponseBytes;
    private long observedProviderResponseBytes;
    private long mediaBytes;

    AiTaskBudget(AiExecutionContext context, java.util.List<OperatorBinding> admittedBindings) {
        this.context = context;
        this.deadlineEpochMs = context.deadlineEpochMs();
        this.concurrency = new Semaphore(context.maxProviderConcurrency(), true);
        this.resources =
                new AiResourceLedger(
                        new AiResourceAmounts(
                                context.maxProviderItems(),
                                context.maxProviderAttempts(),
                                context.maxProviderRequestBytes(),
                                context.maxProviderResponseBytes(),
                                context.maxMediaBytes()));
        this.admittedBindings =
                admittedBindings.stream()
                        .collect(
                                java.util.stream.Collectors.toUnmodifiableMap(
                                        OperatorBinding::getPlanNodeId,
                                        java.util.function.Function.identity()));
    }

    synchronized OperatorBinding admittedBinding(String invocationId) {
        requireActive();
        OperatorBinding binding = admittedBindings.get(invocationId);
        if (binding == null) {
            throw authorization("AI invocation has no admitted canonical OperatorBinding");
        }
        return binding;
    }

    synchronized void requireSameGrant(AiExecutionContext expected) {
        if (!context.equals(expected)) {
            throw authorization("AI task attempt was reused with a different authority or grant");
        }
    }

    synchronized Binding reserveLogical(
            BoundAiInvocation binding,
            AiInvocationRequest request,
            AiCapabilityDescriptor capability,
            AiInvocationReservation reservation) {
        requireActive();
        if (!org.pangrova.ai.worker.context.GovernedAiBindings.fromAdmitted(
                        admittedBinding(binding.invocationId()))
                .equals(binding)) {
            throw authorization(
                    "AI callback binding differs from the actual admitted OperatorBinding");
        }
        OperatorBudget operator =
                operators.computeIfAbsent(
                        binding.invocationId(), ignored -> new OperatorBudget(binding));
        operator.requireSameBinding(binding);
        ModelKey key =
                new ModelKey(
                        request.modelName(),
                        request.modelVersion(),
                        HexFormat.of().formatHex(request.modelSignatureSha256()),
                        capability.capabilityId());
        ModelTaskBudget model =
                models.computeIfAbsent(
                        key, ignored -> new ModelTaskBudget(capability.resourceLimits()));
        model.requireSameLimits(capability.resourceLimits());
        Binding budgets = new Binding(operator, model);
        AiResourceAmounts charge =
                new AiResourceAmounts(reservation.items(), 0, 0, 0, reservation.mediaBytes());
        debit(budgets, charge);
        reservedItems = Math.addExact(reservedItems, charge.items());
        reservedMediaBytes = Math.addExact(reservedMediaBytes, charge.mediaBytes());
        activeAdmissions = Math.incrementExact(activeAdmissions);
        return budgets;
    }

    synchronized void chargeAttempt(
            Binding budgets,
            long requestBytes,
            AiInvocationReservation reservation,
            boolean first) {
        requireActive();
        debit(budgets, new AiResourceAmounts(0, 1, requestBytes, 0, 0));
        providerAttempts++;
        providerRequestBytes = Math.addExact(providerRequestBytes, requestBytes);
        if (first) {
            providerItems = Math.addExact(providerItems, reservation.items());
            mediaBytes = Math.addExact(mediaBytes, reservation.mediaBytes());
            reservedItems -= reservation.items();
            reservedMediaBytes -= reservation.mediaBytes();
        }
    }

    synchronized void observeResponseBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("response bytes must not be negative");
        }
        observedProviderResponseBytes = Math.addExact(observedProviderResponseBytes, bytes);
    }

    /** Charges a governed FILE publication against the exact task's shared media grant. */
    synchronized void chargeGovernedFileBytes(long bytes) {
        requireActive();
        if (bytes < 0) {
            throw new IllegalArgumentException("FILE media bytes must not be negative");
        }
        long next = Math.addExact(mediaBytes, bytes);
        resources.debit(new AiResourceAmounts(0, 0, 0, 0, bytes));
        mediaBytes = next;
    }

    synchronized void retainResponseBytes(Binding budgets, long bytes) {
        requireActive();
        debit(budgets, new AiResourceAmounts(0, 0, 0, bytes, 0));
        providerResponseBytes = Math.addExact(providerResponseBytes, bytes);
    }

    synchronized void finishLogical(
            Binding budgets, AiInvocationReservation reservation, boolean sent) {
        AiResourceAmounts charge =
                new AiResourceAmounts(reservation.items(), 0, 0, 0, reservation.mediaBytes());
        if (!sent) {
            reservedItems -= charge.items();
            reservedMediaBytes -= charge.mediaBytes();
            resources.refund(charge);
            budgets.operator.resources.refund(charge);
            budgets.model.resources.refund(charge);
        }
        if (--activeAdmissions < 0) {
            throw new IllegalStateException("AI task admission released more than once");
        }
    }

    private void debit(Binding budgets, AiResourceAmounts charge) {
        resources.requireAvailable(charge);
        budgets.operator.resources.requireAvailable(charge);
        budgets.model.resources.requireAvailable(charge);
        resources.debit(charge);
        budgets.operator.resources.debit(charge);
        budgets.model.resources.debit(charge);
    }

    synchronized ResourceUsage usage() {
        return ResourceUsage.newBuilder()
                .setProviderItems(providerItems)
                .setProviderAttempts(providerAttempts)
                .setProviderRequestBytes(providerRequestBytes)
                .setProviderResponseBytes(providerResponseBytes)
                .setObservedProviderResponseBytes(observedProviderResponseBytes)
                .setMediaBytes(mediaBytes)
                .build();
    }

    synchronized boolean isIdle() {
        return activeAdmissions == 0;
    }

    void revoke() {
        java.util.List<AiProviderCallContext.LocalExchange> active;
        synchronized (this) {
            revoked = true;
            permitWaiters.forEach(Thread::interrupt);
            active = java.util.List.copyOf(exchanges);
        }
        // Never hold the budget monitor while joining callbacks that charge this budget.
        active.forEach(AiProviderCallContext.LocalExchange::cancel);
        active.forEach(AiProviderCallContext.LocalExchange::awaitTermination);
    }

    synchronized AiProviderCallContext.ExchangeRegistration registerExchange(
            AiProviderCallContext.LocalExchange exchange) {
        requireActive();
        if (!exchanges.add(java.util.Objects.requireNonNull(exchange))) {
            throw new IllegalStateException("AI client exchange is already registered");
        }
        return () -> {
            synchronized (AiTaskBudget.this) {
                exchanges.remove(exchange);
            }
        };
    }

    void terminate() {
        revoke();
        synchronized (this) {
            retired = true;
        }
    }

    synchronized boolean isTerminated() {
        return retired;
    }

    synchronized PermitWaiter registerPermitWaiter() {
        requireActive();
        Thread thread = Thread.currentThread();
        if (!permitWaiters.add(thread)) {
            throw new IllegalStateException("Thread already waits for this AI task's permits");
        }
        return new PermitWaiter(thread);
    }

    synchronized void requireActive() {
        if (revoked) {
            throw authorization(
                    "AI task attempt was revoked or released before provider admission");
        }
    }

    final class PermitWaiter implements AutoCloseable {
        private final Thread thread;

        private PermitWaiter(Thread thread) {
            this.thread = thread;
        }

        @Override
        public void close() {
            synchronized (AiTaskBudget.this) {
                permitWaiters.remove(thread);
            }
        }
    }

    private static AiInvocationException authorization(String message) {
        return new AiInvocationException(
                AiInvocationException.Category.AUTHORIZATION, false, message);
    }

    record Binding(OperatorBudget operator, ModelTaskBudget model) {}

    private record ModelKey(String name, String version, String signature, String capability) {}

    static final class OperatorBudget {
        final Semaphore concurrency;
        private final BoundAiInvocation binding;
        private final AiResourceLedger resources;

        OperatorBudget(BoundAiInvocation binding) {
            this.binding = binding;
            this.concurrency = new Semaphore(binding.maxProviderConcurrency(), true);
            this.resources =
                    new AiResourceLedger(
                            new AiResourceAmounts(
                                    binding.maxProviderItems(),
                                    binding.maxProviderAttempts(),
                                    binding.maxProviderRequestBytes(),
                                    binding.maxProviderResponseBytes(),
                                    binding.maxMediaBytes()));
        }

        void requireSameBinding(BoundAiInvocation expected) {
            if (!binding.equals(expected)) {
                throw authorization(
                        "AI operator identity was reused with a different exact binding");
            }
        }
    }

    static final class ModelTaskBudget {
        private final AiCapabilityDescriptor.ResourceLimits limits;
        private final AiResourceLedger resources;

        ModelTaskBudget(AiCapabilityDescriptor.ResourceLimits limits) {
            this.limits = limits;
            this.resources =
                    new AiResourceLedger(
                            new AiResourceAmounts(
                                    limits.maxProviderItemsPerTask(),
                                    limits.maxProviderAttemptsPerTask(),
                                    Long.MAX_VALUE,
                                    Long.MAX_VALUE,
                                    Long.MAX_VALUE));
        }

        void requireSameLimits(AiCapabilityDescriptor.ResourceLimits expected) {
            if (!limits.equals(expected)) {
                throw authorization(
                        "Exact model capability changed its resource limits within a task attempt");
            }
        }
    }
}
