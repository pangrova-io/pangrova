package org.pangrova.worker.ai.invocation;

import org.pangrova.spi.ai.AiRequestConstraints;
import org.pangrova.worker.ai.context.AiProviderCallContext;

import java.time.Instant;

import static org.pangrova.worker.ai.invocation.AiInvocationBudgetManager.budgetExceeded;

/** Logical invocation lifetime; each send has independent physical resource ownership. */
final class AiPhysicalInvocationAdmission implements AutoCloseable {
    private final AiTaskBudget task;
    private final AiTaskBudget.Binding budgets;
    private final AiModelBudgetRegistry.Lease model;
    private final AiInvocationReservation reservation;
    private final AiRequestConstraints constraints;
    private final Instant deadline;
    private final Runnable onFinalized;
    private long attempts;
    private int liveAttempts;
    private boolean closed;
    private boolean finalized;

    AiPhysicalInvocationAdmission(
            AiTaskBudget task,
            AiTaskBudget.Binding budgets,
            AiModelBudgetRegistry.Lease model,
            AiInvocationReservation reservation,
            AiRequestConstraints constraints,
            Instant deadline,
            Runnable onFinalized) {
        this.task = task;
        this.budgets = budgets;
        this.model = model;
        this.reservation = reservation;
        this.constraints = constraints;
        this.deadline = deadline;
        this.onFinalized = onFinalized;
    }

    AiProviderCallContext.PhysicalAttempt openAttempt(long requestBytes) {
        constraints.requireRequestBytes(requestBytes);
        synchronized (this) {
            requireOpen();
        }
        AiInvocationPermits permits =
                AiInvocationPermits.acquire(
                        task,
                        deadline,
                        task.concurrency,
                        budgets.operator().concurrency,
                        model.concurrency());
        try {
            synchronized (this) {
                requireOpen();
                if (attempts >= reservation.maxAttempts()) {
                    throw budgetExceeded(
                            "AI provider exceeded its exact invocation retry envelope");
                }
                task.chargeAttempt(budgets, requestBytes, reservation, attempts == 0);
                attempts++;
                liveAttempts++;
                return new Attempt(permits);
            }
        } catch (RuntimeException failure) {
            permits.close();
            throw failure;
        }
    }

    synchronized long physicalAttempts() {
        return attempts;
    }

    private void requireOpen() {
        if (closed) {
            throw budgetExceeded("AI provider invocation is closed");
        }
        task.requireActive();
    }

    @Override
    public synchronized void close() {
        closed = true;
        finalizeIfIdle();
    }

    private void finalizeIfIdle() {
        if (closed && liveAttempts == 0 && !finalized) {
            finalized = true;
            try {
                task.finishLogical(budgets, reservation, attempts != 0);
            } finally {
                try {
                    model.close();
                } finally {
                    onFinalized.run();
                }
            }
        }
    }

    private final class Attempt implements AiProviderCallContext.PhysicalAttempt {
        private final AiInvocationPermits permits;
        private long responseBytes;
        private boolean finished;

        Attempt(AiInvocationPermits permits) {
            this.permits = permits;
        }

        @Override
        public synchronized void acceptResponseBytes(long bytes) {
            if (finished) {
                throw new IllegalStateException("AI transport attempt is closed");
            }
            task.observeResponseBytes(bytes);
            if (bytes < 0 || bytes > constraints.maxResponseBytes() - responseBytes) {
                throw budgetExceeded("AI response body exceeds its exact request constraints");
            }
            task.retainResponseBytes(budgets, bytes);
            responseBytes += bytes;
        }

        @Override
        public synchronized void close() {
            if (!finished) {
                finished = true;
                permits.close();
                synchronized (AiPhysicalInvocationAdmission.this) {
                    liveAttempts--;
                    finalizeIfIdle();
                }
            }
        }
    }
}
