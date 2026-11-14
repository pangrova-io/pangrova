package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.exception.AiInvocationException;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Deadline-bounded ownership of task, operator and exact model send permits. */
final class AiInvocationPermits implements AutoCloseable {
    private final Semaphore[] semaphores;
    private int acquired;

    private AiInvocationPermits(Semaphore... semaphores) {
        this.semaphores = semaphores;
    }

    static AiInvocationPermits acquire(
            AiTaskBudget task, Instant deadline, Semaphore... semaphores) {
        AiInvocationPermits permits = new AiInvocationPermits(semaphores);
        try (var waiter = task.registerPermitWaiter()) {
            for (Semaphore semaphore : semaphores) {
                long nanos = remainingNanos(deadline);
                if (!semaphore.tryAcquire(1, nanos, TimeUnit.NANOSECONDS)) {
                    throw timeout("Timed out waiting for AI provider concurrency", null);
                }
                permits.acquired++;
            }
            return permits;
        } catch (InterruptedException failure) {
            permits.close();
            Thread.currentThread().interrupt();
            task.requireActive();
            throw timeout("Interrupted while waiting for AI provider concurrency", failure);
        } catch (RuntimeException failure) {
            permits.close();
            throw failure;
        }
    }

    private static long remainingNanos(Instant deadline) {
        Duration remaining = Duration.between(Instant.now(), deadline);
        if (remaining.isNegative() || remaining.isZero()) {
            throw timeout("AI provider deadline expired while waiting for concurrency", null);
        }
        try {
            return remaining.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static AiInvocationException timeout(String message, Throwable cause) {
        return new AiInvocationException(
                AiInvocationException.Category.TIMEOUT, false, message, cause);
    }

    @Override
    public void close() {
        while (acquired > 0) {
            semaphores[--acquired].release();
        }
    }
}
