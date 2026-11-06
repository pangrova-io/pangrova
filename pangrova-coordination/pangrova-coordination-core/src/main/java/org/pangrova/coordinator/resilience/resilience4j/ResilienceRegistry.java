package org.pangrova.coordinator.resilience.resilience4j;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.BulkheadSettings;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.CircuitBreakerSettings;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.RateLimiterSettings;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.RetrySettings;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.TimeLimiterSettings;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Central registry for Resilience4j instances, configured per-service from YAML.
 *
 * <p>Implements {@link CircuitBreakerPolicy} SPI so upstream modules can depend on
 * the stable contract. Each target (service name) gets its own circuit breaker instance.</p>
 *
 * <p>Per-host breakers are created dynamically using compound keys of the form
 * {@code "service#instance"} (see {@link #serviceInstance(String, String)}).
 * The per-host breaker inherits its configuration from the base service template.</p>
 *
 * <p>Thread safety: all registries are thread-safe by Resilience4j contract.
 * The per-target lookup uses {@link ConcurrentHashMap} for safe concurrent access.</p>
 */
@ThreadSafe
public final class ResilienceRegistry implements CircuitBreakerPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ResilienceRegistry.class);

    /** Delimiter separating service name from instance identifier in compound keys. */
    private static final String SERVICE_INSTANCE_DELIMITER = "#";

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;
    private final TimeLimiterRegistry timeLimiterRegistry;
    private final Map<String, ResiliencePolicyConfig> serviceConfigs;
    private final ConcurrentHashMap<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    private final MetricsProvider metricsProvider;

    /**
     * Creates a registry with the given per-service configurations and metrics provider.
     *
     * @param serviceConfigs per-service resilience policy configurations
     * @param metricsProvider metrics provider for exporting Resilience4j metrics
     */
    public ResilienceRegistry(Map<String, ResiliencePolicyConfig> serviceConfigs,
                              MetricsProvider metricsProvider) {
        this.serviceConfigs = Map.copyOf(serviceConfigs);
        this.metricsProvider = metricsProvider;
        this.circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
        this.retryRegistry = RetryRegistry.ofDefaults();
        this.bulkheadRegistry = BulkheadRegistry.ofDefaults();
        this.rateLimiterRegistry = RateLimiterRegistry.ofDefaults();
        this.timeLimiterRegistry = TimeLimiterRegistry.ofDefaults();
        registerAllServices();
    }

    /** Creates a registry with default configs for all services (no metrics). */
    public ResilienceRegistry(Map<String, ResiliencePolicyConfig> serviceConfigs) {
        this(serviceConfigs, null);
    }

    /**
     * Builds a compound key for a per-host circuit breaker instance.
     *
     * <p>The resulting key has the form {@code "service#instance"}, where the service
     * prefix is used to look up the configuration template.</p>
     *
     * @param service  the logical service name (e.g., {@code "coordinator-to-worker"})
     * @param instance the instance identifier (e.g., host address {@code "10.0.0.5"})
     * @return the compound key
     */
    public static String serviceInstance(String service, String instance) {
        return service + SERVICE_INSTANCE_DELIMITER + instance;
    }

    // -- CircuitBreakerPolicy SPI --

    @Override
    public boolean allowRequest(String target) {
        CircuitBreaker cb = getCircuitBreaker(target);
        return cb.tryAcquirePermission();
    }

    @Override
    public void recordSuccess(String target) {
        CircuitBreaker cb = getCircuitBreaker(target);
        cb.onSuccess(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void recordFailure(String target, Throwable cause) {
        CircuitBreaker cb = getCircuitBreaker(target);
        cb.onError(0, TimeUnit.MILLISECONDS, cause);
    }

    // -- Public accessors for Resilience4j instances --

    /** Returns the circuit breaker for the given service, creating with defaults if absent. */
    public CircuitBreaker getCircuitBreaker(String serviceName) {
        return circuitBreakers.computeIfAbsent(serviceName, name -> {
            CircuitBreaker cb = createCircuitBreaker(name);
            registerMetrics(name, cb);
            return cb;
        });
    }

    /** Returns the retry instance for the given service. */
    public Retry getRetry(String serviceName) {
        return retryRegistry.retry(serviceName, buildRetryConfig(serviceName));
    }

    /** Returns the bulkhead for the given service. */
    public Bulkhead getBulkhead(String serviceName) {
        return bulkheadRegistry.bulkhead(serviceName, buildBulkheadConfig(serviceName));
    }

    /** Returns the rate limiter for the given service. */
    public RateLimiter getRateLimiter(String serviceName) {
        return rateLimiterRegistry.rateLimiter(serviceName, buildRateLimiterConfig(serviceName));
    }

    /** Returns the time limiter for the given service. */
    public TimeLimiter getTimeLimiter(String serviceName) {
        return timeLimiterRegistry.timeLimiter(serviceName, buildTimeLimiterConfig(serviceName));
    }

    /** Returns the underlying circuit breaker registry for metrics export. */
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        return circuitBreakerRegistry;
    }

    /** Returns the underlying retry registry for metrics export. */
    public RetryRegistry retryRegistry() {
        return retryRegistry;
    }

    /** Returns the underlying bulkhead registry for metrics export. */
    public BulkheadRegistry bulkheadRegistry() {
        return bulkheadRegistry;
    }

    /** Returns the underlying rate limiter registry for metrics export. */
    public RateLimiterRegistry rateLimiterRegistry() {
        return rateLimiterRegistry;
    }

    /** Returns the underlying time limiter registry for metrics export. */
    public TimeLimiterRegistry timeLimiterRegistry() {
        return timeLimiterRegistry;
    }

    // -- Private --

    private void registerAllServices() {
        for (Map.Entry<String, ResiliencePolicyConfig> entry : serviceConfigs.entrySet()) {
            String name = entry.getKey();
            CircuitBreaker cb = createCircuitBreaker(name);
            circuitBreakers.put(name, cb);
            registerMetrics(name, cb);
            LOG.info("event=resilience_service_registered service={}", name);
        }
    }

    private CircuitBreaker createCircuitBreaker(String serviceName) {
        CircuitBreakerConfig config = buildCircuitBreakerConfig(serviceName);
        return circuitBreakerRegistry.circuitBreaker(serviceName, config);
    }

    /**
     * Extracts the base service name from a potentially compound key.
     * For {@code "coordinator-to-worker#10.0.0.5"} returns {@code "coordinator-to-worker"}.
     * For plain keys like {@code "coordinator-to-worker"} returns the key unchanged.
     */
    private static String baseServiceName(String serviceName) {
        int idx = serviceName.indexOf(SERVICE_INSTANCE_DELIMITER);
        return idx >= 0 ? serviceName.substring(0, idx) : serviceName;
    }

    private CircuitBreakerConfig buildCircuitBreakerConfig(String serviceName) {
        String baseName = baseServiceName(serviceName);
        ResiliencePolicyConfig policy = serviceConfigs.get(baseName);
        if (policy == null || policy.circuitBreaker() == null) {
            return CircuitBreakerConfig.ofDefaults();
        }
        CircuitBreakerSettings s = policy.circuitBreaker();
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(s.failureRateThreshold())
                .slowCallRateThreshold(s.slowCallRateThreshold())
                .slowCallDurationThreshold(Duration.ofMillis(s.slowCallDurationMs()))
                .minimumNumberOfCalls(s.minimumNumberOfCalls())
                .slidingWindowSize(s.slidingWindowSize())
                .waitDurationInOpenState(Duration.ofMillis(s.waitDurationInOpenStateMs()))
                .permittedNumberOfCallsInHalfOpenState(s.permittedNumberOfCallsInHalfOpenState())
                .build();
    }

    private RetryConfig buildRetryConfig(String serviceName) {
        ResiliencePolicyConfig policy = serviceConfigs.get(serviceName);
        if (policy == null || policy.retry() == null) {
            return RetryConfig.ofDefaults();
        }
        RetrySettings s = policy.retry();
        return RetryConfig.custom()
                .maxAttempts(s.maxAttempts())
                .waitDuration(Duration.ofMillis(s.waitDurationMs()))
                .build();
    }

    private BulkheadConfig buildBulkheadConfig(String serviceName) {
        ResiliencePolicyConfig policy = serviceConfigs.get(serviceName);
        if (policy == null || policy.bulkhead() == null) {
            return BulkheadConfig.ofDefaults();
        }
        BulkheadSettings s = policy.bulkhead();
        return BulkheadConfig.custom()
                .maxConcurrentCalls(s.maxConcurrentCalls())
                .maxWaitDuration(Duration.ofMillis(s.maxWaitDurationMs()))
                .build();
    }

    private RateLimiterConfig buildRateLimiterConfig(String serviceName) {
        ResiliencePolicyConfig policy = serviceConfigs.get(serviceName);
        if (policy == null || policy.rateLimiter() == null) {
            return RateLimiterConfig.ofDefaults();
        }
        RateLimiterSettings s = policy.rateLimiter();
        return RateLimiterConfig.custom()
                .limitForPeriod(s.limitForPeriod())
                .limitRefreshPeriod(Duration.ofMillis(s.limitRefreshPeriodMs()))
                .timeoutDuration(Duration.ofMillis(s.timeoutDurationMs()))
                .build();
    }

    private TimeLimiterConfig buildTimeLimiterConfig(String serviceName) {
        String baseName = baseServiceName(serviceName);
        ResiliencePolicyConfig policy = serviceConfigs.get(baseName);
        if (policy == null || policy.timeLimiter() == null) {
            return TimeLimiterConfig.ofDefaults();
        }
        TimeLimiterSettings s = policy.timeLimiter();
        return TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(s.timeoutDurationMs()))
                .cancelRunningFuture(s.cancelRunningFuture())
                .build();
    }

    private void registerMetrics(String serviceName, CircuitBreaker cb) {
        if (metricsProvider == null) {
            return;
        }
        registerGaugeMetrics(cb);

        LongCounter stateTransitionCounter = metricsProvider.createCounter(
                "resilience.circuitbreaker.state_transitions",
                "Circuit breaker state transition events",
                "transitions");
        LongCounter callResultCounter = metricsProvider.createCounter(
                "resilience.circuitbreaker.calls",
                "Circuit breaker call outcome events",
                "calls");

        subscribeToEvents(serviceName, cb, stateTransitionCounter, callResultCounter);
    }

    private void registerGaugeMetrics(CircuitBreaker cb) {
        metricsProvider.createGauge(
                "resilience.circuitbreaker.state",
                "Circuit breaker state ordinal (0=CLOSED, 1=OPEN, 2=HALF_OPEN)",
                "state",
                () -> (double) cb.getState().getOrder());
        metricsProvider.createGauge(
                "resilience.circuitbreaker.failure_rate",
                "Current failure rate percentage",
                "percent",
                () -> (double) cb.getMetrics().getFailureRate());
        metricsProvider.createGauge(
                "resilience.circuitbreaker.buffered_calls",
                "Number of buffered calls in the sliding window",
                "calls",
                () -> (double) cb.getMetrics().getNumberOfBufferedCalls());
    }

    private void subscribeToEvents(String serviceName, CircuitBreaker cb,
                                   LongCounter stateTransitionCounter,
                                   LongCounter callResultCounter) {
        cb.getEventPublisher()
                .onStateTransition(event -> {
                    MetricAttributes attrs = MetricAttributes.of(
                            "name", serviceName,
                            "transition", event.getStateTransition().toString());
                    stateTransitionCounter.add(1, attrs);
                })
                .onSuccess(event -> {
                    MetricAttributes attrs = MetricAttributes.of(
                            "name", serviceName,
                            "outcome", "success");
                    callResultCounter.add(1, attrs);
                })
                .onError(event -> {
                    MetricAttributes attrs = MetricAttributes.of(
                            "name", serviceName,
                            "outcome", "error");
                    callResultCounter.add(1, attrs);
                })
                .onCallNotPermitted(event -> {
                    MetricAttributes attrs = MetricAttributes.of(
                            "name", serviceName,
                            "outcome", "not_permitted");
                    callResultCounter.add(1, attrs);
                });
    }
}
