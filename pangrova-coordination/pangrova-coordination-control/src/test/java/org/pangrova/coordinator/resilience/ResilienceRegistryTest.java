package org.pangrova.coordinator.resilience;

import org.pangrova.coordinator.enums.CircuitBreakerPhase;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.retry.Retry;
import org.pangrova.coordinator.resilience.config.ResilienceConfigLoader;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying Resilience4j circuit breaker, retry, bulkhead,
 * and rate limiter behavior through the ResilienceRegistry.
 */
class ResilienceRegistryTest {

    private ResilienceRegistry registry;

    @BeforeEach
    void setUp() {
        InputStream config = getClass().getClassLoader().getResourceAsStream("resilience-defaults.yml");
        Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
        registry = new ResilienceRegistry(policies);
    }

    @Nested
    class CircuitBreakerBehavior {

        @Test
        void closedStateAllowsRequests() {
            assertThat(registry.allowRequest("coordinator-to-metadata")).isTrue();
        }

        @Test
        void tripsOpenAfterFailureThresholdExceeded() {
            CircuitBreaker cb = registry.getCircuitBreaker("coordinator-to-metadata");
            // Fill the sliding window with minimum calls (10) all failing
            for (int i = 0; i < 10; i++) {
                cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("simulated failure"));
            }
            // Circuit should now be OPEN
            assertThat(cb.getState()).isEqualTo(CircuitBreakerPhase.OPEN);
            assertThat(cb.tryAcquirePermission()).isFalse();
        }

        @Test
        void recordSuccessKeepsCircuitClosed() {
            CircuitBreaker cb = registry.getCircuitBreaker("coordinator-to-worker");
            for (int i = 0; i < 20; i++) {
                cb.onSuccess(100, TimeUnit.MILLISECONDS);
            }
            assertThat(cb.getState()).isEqualTo(CircuitBreakerPhase.CLOSED);
        }

        @Test
        void spiInterfaceRecordFailureTripsBreaker() {
            // Use the CircuitBreakerPolicy SPI methods against a configured service
            // coordinator-to-metadata has minimumNumberOfCalls=10, failureRateThreshold=50%
            String target = "coordinator-to-metadata";
            CircuitBreaker cb = registry.getCircuitBreaker(target);
            for (int i = 0; i < 10; i++) {
                cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("fail"));
            }
            assertThat(registry.allowRequest(target)).isFalse();
        }

        @Test
        void spiInterfaceRecordSuccessKeepsClosed() {
            String target = "healthy-service";
            for (int i = 0; i < 20; i++) {
                registry.recordSuccess(target);
            }
            assertThat(registry.allowRequest(target)).isTrue();
        }
    }

    @Nested
    class RetryBehavior {

        @Test
        void retryInstanceCreatedWithConfiguredAttempts() {
            Retry retry = registry.getRetry("coordinator-to-metadata");
            assertThat(retry).isNotNull();
            assertThat(retry.getName()).isEqualTo("coordinator-to-metadata");
            assertThat(retry.getRetryConfig().getMaxAttempts()).isEqualTo(3);
        }

        @Test
        void retryWithDefaultsForUnknownService() {
            Retry retry = registry.getRetry("unknown-service");
            assertThat(retry).isNotNull();
            // Default Resilience4j retry config has 3 max attempts
            assertThat(retry.getRetryConfig().getMaxAttempts()).isEqualTo(3);
        }
    }

    @Nested
    class BulkheadBehavior {

        @Test
        void bulkheadCreatedWithConfiguredConcurrency() {
            Bulkhead bulkhead = registry.getBulkhead("coordinator-to-metadata");
            assertThat(bulkhead).isNotNull();
            assertThat(bulkhead.getBulkheadConfig().getMaxConcurrentCalls()).isEqualTo(20);
        }

        @Test
        void bulkheadRejectsWhenFull() {
            Bulkhead bulkhead = registry.getBulkhead("coordinator-to-metadata");
            // Acquire all permits
            for (int i = 0; i < 20; i++) {
                assertThat(bulkhead.tryAcquirePermission()).isTrue();
            }
            // Next should be rejected
            assertThat(bulkhead.tryAcquirePermission()).isFalse();
        }
    }

    @Nested
    class RateLimiterBehavior {

        @Test
        void rateLimiterCreatedWithConfiguredLimit() {
            RateLimiter rateLimiter = registry.getRateLimiter("coordinator-to-metadata");
            assertThat(rateLimiter).isNotNull();
            assertThat(rateLimiter.getRateLimiterConfig().getLimitForPeriod()).isEqualTo(200);
        }

        @Test
        void rateLimiterPermitsWithinLimit() {
            RateLimiter rateLimiter = registry.getRateLimiter("coordinator-to-metadata");
            assertThat(rateLimiter.acquirePermission()).isTrue();
        }
    }

    @Nested
    class ConfigLoading {

        @Test
        void loadsAllServicesFromYaml() {
            InputStream config = getClass().getClassLoader().getResourceAsStream("resilience-defaults.yml");
            Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
            assertThat(policies).containsKeys(
                    "coordinator-to-metadata",
                    "coordinator-to-worker",
                    "worker-to-shuffle");
        }

        @Test
        void parsesCircuitBreakerSettings() {
            InputStream config = getClass().getClassLoader().getResourceAsStream("resilience-defaults.yml");
            Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
            ResiliencePolicyConfig metadata = policies.get("coordinator-to-metadata");
            assertThat(metadata.circuitBreaker()).isNotNull();
            assertThat(metadata.circuitBreaker().failureRateThreshold()).isEqualTo(50.0f);
            assertThat(metadata.circuitBreaker().slidingWindowSize()).isEqualTo(100);
        }

        @Test
        void returnsEmptyMapForNullInput() {
            Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(null);
            assertThat(policies).isEmpty();
        }
    }

    @Nested
    class RegistryAccessors {

        @Test
        void exposesUnderlyingRegistries() {
            assertThat(registry.circuitBreakerRegistry()).isNotNull();
            assertThat(registry.retryRegistry()).isNotNull();
            assertThat(registry.bulkheadRegistry()).isNotNull();
            assertThat(registry.rateLimiterRegistry()).isNotNull();
        }
    }
}
