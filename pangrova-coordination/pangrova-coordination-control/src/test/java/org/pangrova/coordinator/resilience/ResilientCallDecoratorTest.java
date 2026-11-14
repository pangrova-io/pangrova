package org.pangrova.coordinator.resilience;

import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.coordinator.resilience.resilience4j.ResilientCallDecorator;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.pangrova.coordinator.resilience.config.ResilienceConfigLoader;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.BulkheadSettings;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.RateLimiterSettings;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig.TimeLimiterSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests verifying that bulkhead, rate limiter, and time limiter configurations
 * are actually enforced by {@link ResilientCallDecorator} (F-RESIL-06 fix).
 */
class ResilientCallDecoratorTest {

    private static final String BULKHEAD_SERVICE = "bulkhead-test-service";
    private static final String RATELIMIT_SERVICE = "ratelimit-test-service";
    private static final String TIMELIMIT_SERVICE = "timelimit-test-service";

    @Nested
    class BulkheadEnforcement {

        private ResilienceRegistry registry;
        private ResilientCallDecorator decorator;

        @BeforeEach
        void setUp() {
            // Bulkhead tests: tight concurrency, generous rate limit
            ResiliencePolicyConfig config = new ResiliencePolicyConfig(
                    BULKHEAD_SERVICE,
                    null,
                    null,
                    new BulkheadSettings(2, 50),           // max 2 concurrent, 50ms wait
                    new RateLimiterSettings(1000, 1000, 0), // generous rate limit
                    null
            );
            registry = new ResilienceRegistry(Map.of(BULKHEAD_SERVICE, config));
            decorator = new ResilientCallDecorator(registry);
        }

        @Test
        void allowsCallsWithinConcurrencyLimit() throws Exception {
            String result = decorator.call(BULKHEAD_SERVICE, () -> "success");
            assertThat(result).isEqualTo("success");
        }

        @Test
        void rejectsWhenMaxConcurrentCallsExceeded() throws Exception {
            // Config: maxConcurrentCalls=2, maxWaitDurationMs=50
            CountDownLatch blockingLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(2);
            AtomicInteger completedCalls = new AtomicInteger(0);

            // Launch 2 threads that block inside the bulkhead
            Thread t1 = new Thread(() -> {
                try {
                    decorator.call(BULKHEAD_SERVICE, () -> {
                        startedLatch.countDown();
                        blockingLatch.await();
                        completedCalls.incrementAndGet();
                        return null;
                    });
                } catch (Exception ignored) {}
            });
            Thread t2 = new Thread(() -> {
                try {
                    decorator.call(BULKHEAD_SERVICE, () -> {
                        startedLatch.countDown();
                        blockingLatch.await();
                        completedCalls.incrementAndGet();
                        return null;
                    });
                } catch (Exception ignored) {}
            });

            t1.start();
            t2.start();
            // Wait for both threads to enter the bulkhead
            startedLatch.await(5, TimeUnit.SECONDS);

            // Third call should be rejected (bulkhead full, wait timeout is 50ms)
            assertThatThrownBy(() -> decorator.call(BULKHEAD_SERVICE, () -> "should fail"))
                    .isInstanceOf(BulkheadFullException.class);

            // Cleanup
            blockingLatch.countDown();
            t1.join(5000);
            t2.join(5000);
        }

        @Test
        void releasesPermitAfterCallCompletes() throws Exception {
            // Fill and release the bulkhead multiple times — proves permits are returned
            for (int i = 0; i < 10; i++) {
                String result = decorator.call(BULKHEAD_SERVICE, () -> "iteration");
                assertThat(result).isEqualTo("iteration");
            }
        }
    }

    @Nested
    class RateLimiterEnforcement {

        private ResilientCallDecorator decorator;

        @BeforeEach
        void setUp() {
            // Rate limiter tests: tight rate, generous bulkhead
            ResiliencePolicyConfig config = new ResiliencePolicyConfig(
                    RATELIMIT_SERVICE,
                    null,
                    null,
                    new BulkheadSettings(100, 500),          // generous bulkhead
                    new RateLimiterSettings(3, 1000, 0),     // 3 per second, 0ms timeout
                    null
            );
            ResilienceRegistry registry = new ResilienceRegistry(Map.of(RATELIMIT_SERVICE, config));
            decorator = new ResilientCallDecorator(registry);
        }

        @Test
        void allowsCallsWithinRateLimit() throws Exception {
            // Config allows 3 per period; 3 calls should succeed
            for (int i = 0; i < 3; i++) {
                String result = decorator.call(RATELIMIT_SERVICE, () -> "ok");
                assertThat(result).isEqualTo("ok");
            }
        }

        @Test
        void rejectsWhenRateLimitExceeded() {
            // Config: limitForPeriod=3, timeoutDuration=0ms (fail immediately when over limit)
            // Exhaust the 3 permits
            for (int i = 0; i < 3; i++) {
                try {
                    decorator.call(RATELIMIT_SERVICE, () -> "ok");
                } catch (Exception ignored) {}
            }
            // Fourth call in the same period should be rejected
            assertThatThrownBy(() -> decorator.call(RATELIMIT_SERVICE, () -> "over limit"))
                    .isInstanceOf(RequestNotPermitted.class);
        }
    }

    @Nested
    class TimeLimiterEnforcement {

        private ResilientCallDecorator decorator;

        @BeforeEach
        void setUp() {
            // Time limiter tests: tight timeout, generous bulkhead and rate limit
            ResiliencePolicyConfig config = new ResiliencePolicyConfig(
                    TIMELIMIT_SERVICE,
                    null,
                    null,
                    new BulkheadSettings(100, 500),
                    new RateLimiterSettings(1000, 1000, 0),
                    new TimeLimiterSettings(200, true)       // 200ms timeout
            );
            ResilienceRegistry registry = new ResilienceRegistry(Map.of(TIMELIMIT_SERVICE, config));
            decorator = new ResilientCallDecorator(registry);
        }

        @Test
        void timesOutSlowAsyncOperations() {
            // Config: timeoutDurationMs=200, cancelRunningFuture=true
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                var stage = decorator.callAsync(
                        TIMELIMIT_SERVICE,
                        () -> {
                            CompletableFuture<String> future = new CompletableFuture<>();
                            // Never completes — simulates a slow operation
                            return future;
                        },
                        scheduler
                );

                assertThatThrownBy(() -> stage.toCompletableFuture().get(2, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            } finally {
                scheduler.shutdownNow();
            }
        }

        @Test
        void allowsFastAsyncOperations() throws Exception {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                var stage = decorator.callAsync(
                        TIMELIMIT_SERVICE,
                        () -> CompletableFuture.completedFuture("fast"),
                        scheduler
                );
                String result = stage.toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(result).isEqualTo("fast");
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Nested
    class ConfigFromYaml {

        @Test
        void yamlConfigIsEnforcedThroughDecorator() throws Exception {
            // Load from the default YAML config
            InputStream config = getClass().getClassLoader()
                    .getResourceAsStream("resilience-defaults.yml");
            Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
            ResilienceRegistry yamlRegistry = new ResilienceRegistry(policies);
            ResilientCallDecorator yamlDecorator = new ResilientCallDecorator(yamlRegistry);

            // coordinator-to-metadata has bulkhead maxConcurrentCalls=20
            String result = yamlDecorator.call("coordinator-to-metadata", () -> "works");
            assertThat(result).isEqualTo("works");

            // Verify the bulkhead is actually configured from YAML
            var bulkhead = yamlRegistry.getBulkhead("coordinator-to-metadata");
            assertThat(bulkhead.getBulkheadConfig().getMaxConcurrentCalls()).isEqualTo(20);
        }
    }
}
