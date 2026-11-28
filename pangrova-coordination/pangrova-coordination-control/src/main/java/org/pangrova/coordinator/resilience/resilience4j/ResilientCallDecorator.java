package org.pangrova.coordinator.resilience.resilience4j;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.timelimiter.TimeLimiter;
import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Composes Resilience4j decorators (bulkhead, rate limiter, time limiter, circuit breaker)
 * from a {@link ResilienceRegistry} and applies them to callables.
 *
 * <p>This resolves F-RESIL-06: parsed bulkhead/rate-limiter/time-limiter configurations
 * were never actually enforced. This decorator applies them in the canonical order:
 * <ol>
 *   <li>Bulkhead — limits concurrent access</li>
 *   <li>Rate limiter — throttles request rate</li>
 *   <li>Time limiter — times out slow operations (async path only)</li>
 *   <li>Circuit breaker — fails fast on repeated failures</li>
 * </ol>
 *
 * <p>Thread safety: this class is stateless beyond the registry reference; the underlying
 * Resilience4j instances are thread-safe by contract.</p>
 */
@ThreadSafe
public final class ResilientCallDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(ResilientCallDecorator.class);

    private final ResilienceRegistry registry;

    public ResilientCallDecorator(ResilienceRegistry registry) {
        this.registry = registry;
    }

    /**
     * Decorates a synchronous callable with bulkhead, rate limiter, and circuit breaker
     * enforcement for the given service.
     *
     * <p>Decoration order (outermost first): CircuitBreaker → RateLimiter → Bulkhead → callable.
     * This means bulkhead is acquired last (closest to the call) and released first.</p>
     *
     * @param serviceName the logical service name (must match resilience config)
     * @param callable    the operation to protect
     * @param <T>         return type
     * @return the result of the callable
     * @throws BulkheadFullException    if max concurrent calls exceeded and wait times out
     * @throws RequestNotPermitted      if rate limit exceeded
     * @throws CallNotPermittedException if circuit breaker is OPEN
     * @throws Exception                if the underlying callable throws
     */
    public <T> T call(String serviceName, Callable<T> callable) throws Exception {
        CircuitBreaker circuitBreaker = registry.getCircuitBreaker(serviceName);
        RateLimiter rateLimiter = registry.getRateLimiter(serviceName);
        Bulkhead bulkhead = registry.getBulkhead(serviceName);

        // Decorate: bulkhead → rate limiter → circuit breaker (applied inside-out)
        Callable<T> decorated = Bulkhead.decorateCallable(bulkhead, callable);
        decorated = RateLimiter.decorateCallable(rateLimiter, decorated);
        decorated = CircuitBreaker.decorateCallable(circuitBreaker, decorated);

        LOG.debug("event=resilient_call_start service={}", serviceName);
        return decorated.call();
    }

    /**
     * Decorates an asynchronous operation with bulkhead, rate limiter, time limiter,
     * and circuit breaker enforcement for the given service.
     *
     * <p>The time limiter applies a timeout to the future; if the future does not
     * complete within the configured duration, it is cancelled (if configured) and
     * a {@link TimeoutException} is raised.</p>
     *
     * @param serviceName the logical service name
     * @param futureSupplier supplier of the async operation
     * @param scheduler executor for the time limiter timeout scheduling
     * @param <T> return type
     * @return a CompletionStage that completes with the result or an exception
     */
    public <T> CompletionStage<T> callAsync(String serviceName,
                                            Supplier<CompletableFuture<T>> futureSupplier,
                                            ScheduledExecutorService scheduler) {
        CircuitBreaker circuitBreaker = registry.getCircuitBreaker(serviceName);
        RateLimiter rateLimiter = registry.getRateLimiter(serviceName);
        Bulkhead bulkhead = registry.getBulkhead(serviceName);
        TimeLimiter timeLimiter = registry.getTimeLimiter(serviceName);

        // Apply bulkhead and rate limiter synchronously before launching async work,
        // then apply time limiter to the resulting future.
        Callable<T> decorated = TimeLimiter.decorateFutureSupplier(timeLimiter, () -> {
            // Rate limiter check
            RateLimiter.waitForPermission(rateLimiter);
            // Bulkhead acquire
            bulkhead.acquirePermission();
            try {
                CompletableFuture<T> future = futureSupplier.get();
                // Release bulkhead when future completes
                future.whenComplete((result, throwable) -> bulkhead.onComplete());
                return future;
            } catch (Exception e) {
                bulkhead.onComplete();
                return CompletableFuture.failedFuture(e);
            }
        });

        // Apply circuit breaker and execute
        LOG.debug("event=resilient_call_async_start service={}", serviceName);
        return executeWithCircuitBreaker(circuitBreaker, decorated, scheduler);
    }

    private <T> CompletionStage<T> executeWithCircuitBreaker(
            CircuitBreaker circuitBreaker,
            Callable<T> callable,
            ScheduledExecutorService scheduler) {
        if (!circuitBreaker.tryAcquirePermission()) {
            CompletableFuture<T> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(
                    CallNotPermittedException.createCallNotPermittedException(circuitBreaker));
            return rejected;
        }
        long start = System.nanoTime();
        try {
            T result = callable.call();
            long durationNanos = System.nanoTime() - start;
            circuitBreaker.onSuccess(durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            long durationNanos = System.nanoTime() - start;
            circuitBreaker.onError(durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS, e);
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /**
     * Decorates a runnable (void operation) with bulkhead, rate limiter, and circuit breaker.
     *
     * @param serviceName the logical service name
     * @param runnable    the operation to protect
     * @throws BulkheadFullException    if max concurrent calls exceeded
     * @throws RequestNotPermitted      if rate limit exceeded
     * @throws CallNotPermittedException if circuit breaker is OPEN
     */
    public void run(String serviceName, Runnable runnable) throws Exception {
        call(serviceName, () -> {
            runnable.run();
            return null;
        });
    }
}
