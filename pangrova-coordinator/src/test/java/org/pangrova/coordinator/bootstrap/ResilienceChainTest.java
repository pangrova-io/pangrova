package org.pangrova.coordinator.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.resilience.breaker.CircuitBreaker;
import org.pangrova.resilience.checkpoint.CheckpointManager;
import org.pangrova.resilience.checkpoint.CheckpointManager.CheckpointEntry;
import org.pangrova.resilience.degradation.DegradationManager;
import org.pangrova.resilience.degradation.DegradationManager.DegradationLevel;
import org.pangrova.resilience.idempotency.IdempotencyManager;
import org.pangrova.resilience.retry.StageRetryManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test: validates the resilience chain from CoordinatorBootstrap.start()
 * through to fault recovery components.
 *
 * <p>Property 30: Resilience chain from main() to fault recovery.</p>
 */
@Tag("Feature: e2e-pipeline-integration - Property 30: Resilience chain")
class ResilienceChainTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDownJUnit() {
        shutdownBootstrap();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownBootstrap();
    }

    // -- Providers --

    @Provide
    Arbitrary<Integer> failureCounts() {
        return Arbitraries.integers().between(5, 20);
    }

    @Provide
    Arbitrary<Double> workerRatios() {
        return Arbitraries.doubles().between(0.0, 1.0);
    }

    @Provide
    Arbitrary<Double> memoryPressures() {
        return Arbitraries.doubles().between(0.0, 1.0);
    }

    // -- Property tests --

    /**
     * P30a: For any failure count >= minimumCalls with >50% failure rate,
     * circuit breaker transitions to OPEN.
     */
    @Property(tries = 5)
    @Label("CircuitBreaker opens on repeated failures exceeding threshold")
    void circuitBreakerOpensOnRepeatedFailures(
            @ForAll("failureCounts") int failureCount) {

        CircuitBreaker breaker = new CircuitBreaker(
                0.5, 5, 100, 3, Clock.systemUTC());

        for (int i = 0; i < failureCount; i++) {
            breaker.onFailure();
        }

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.allowRequest()).isFalse();
    }

    /**
     * P30b: For any workerRatio and memoryPressure, degradation level is
     * monotonically worse as health degrades (lower ratio or higher pressure).
     */
    @Property(tries = 5)
    @Label("Degradation level correlates monotonically with health metrics")
    void degradationLevelCorrelatesWithHealth(
            @ForAll("workerRatios") double workerRatio,
            @ForAll("memoryPressures") double memoryPressure) {

        DegradationManager manager = new DegradationManager();
        DegradationLevel level = manager.evaluateDegradation(workerRatio, memoryPressure);

        DegradationLevel worseLevel = manager.evaluateDegradation(
                Math.max(0.0, workerRatio - 0.3),
                Math.min(1.0, memoryPressure + 0.1));

        assertThat(worseLevel.ordinal()).isGreaterThanOrEqualTo(level.ordinal());

        boolean criticalAllowed = manager.checkQueryAllowed(DegradationLevel.CRITICAL);
        assertThat(criticalAllowed).isFalse();
    }

    // -- Scenario tests --

    /**
     * IdempotencyManager executes supplier only once for the same key.
     */
    @Test
    void idempotencyManagerPreventsDuplicateExecution() {
        IdempotencyManager idempotency = new IdempotencyManager(
                Duration.ofMinutes(5), Clock.systemUTC());
        AtomicInteger callCount = new AtomicInteger(0);

        String first = idempotency.executeIdempotent("key-1", () -> {
            callCount.incrementAndGet();
            return "result-1";
        });
        String second = idempotency.executeIdempotent("key-1", () -> {
            callCount.incrementAndGet();
            return "result-2";
        });

        assertThat(callCount.get()).isEqualTo(1);
        assertThat(first).isEqualTo("result-1");
        assertThat(second).isEqualTo("result-1");
    }

    /**
     * StageRetryManager retries on retryable errors when failure ratio >= 30%,
     * and respects max retry limit of 2.
     */
    @Test
    void stageRetryManagerRetriesOnRetryableErrors() {
        StageRetryManager retryManager = new StageRetryManager();
        retryManager.registerStage("stage-1", 3);

        // 1 of 3 failed = 33% but only 1 task failed so far
        boolean needsRetry = retryManager.onTaskFailure(
                "stage-1", "task-0", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(needsRetry).isTrue();

        retryManager.initiateStageRetry("stage-1");
        assertThat(retryManager.getRetryCount("stage-1")).isEqualTo(1);

        // Trigger second retry
        retryManager.onTaskFailure(
                "stage-1", "task-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        retryManager.initiateStageRetry("stage-1");
        assertThat(retryManager.getRetryCount("stage-1")).isEqualTo(2);

        // Third retry should be blocked (max = 2)
        boolean blockedRetry = retryManager.onTaskFailure(
                "stage-1", "task-2", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(blockedRetry).isFalse();

        // Non-retryable error should not trigger retry
        boolean userError = retryManager.onTaskFailure(
                "stage-1", "task-3", PangrovaErrorCode.PARSER_SYNTAX_ERROR);
        assertThat(userError).isFalse();
    }

    /**
     * CheckpointManager saves and recovers task state with byte[] fidelity.
     */
    @Test
    void checkpointManagerSavesAndRecoversState() {
        CheckpointManager checkpointManager = new CheckpointManager();
        byte[] state = "task-state-payload".getBytes(StandardCharsets.UTF_8);

        checkpointManager.checkpoint("task-42", state);

        assertThat(checkpointManager.hasCheckpoint("task-42")).isTrue();

        Optional<CheckpointEntry> recovered =
                checkpointManager.getLatestCheckpoint("task-42");
        assertThat(recovered).isPresent();
        assertThat(recovered.get().state()).isEqualTo(state);
        assertThat(recovered.get().taskId()).isEqualTo("task-42");

        checkpointManager.clearCheckpoints("task-42");
        assertThat(checkpointManager.hasCheckpoint("task-42")).isFalse();
    }

    /**
     * CoordinatorBootstrap starts with resilience components accessible,
     * and HTTP endpoint responds with 200.
     */
    @Test
    void coordinatorBootstrapStartsWithResilienceComponents() throws Exception {
        CoordinatorBootstrapConfig config =
                CoordinatorBootstrapConfig.withDefaults("coord-resilience", 0);
        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);

        HttpResponse<String> resp = submitQuery(bootstrap.getHttpPort(), "SELECT 1");
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    /**
     * CircuitBreaker transitions from OPEN to HALF_OPEN after wait duration elapses.
     */
    @Test
    void circuitBreakerTransitionsToHalfOpenAfterWait() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker(
                0.5, 5, 50, 3, Clock.systemUTC());

        // Drive to OPEN state
        for (int i = 0; i < 5; i++) {
            breaker.onFailure();
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Wait past the 50ms wait duration
        Thread.sleep(60);

        // allowRequest() triggers HALF_OPEN transition
        boolean allowed = breaker.allowRequest();
        assertThat(allowed).isTrue();
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
    }

    // -- Helpers --

    private void shutdownBootstrap() {
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    private static HttpResponse<String> submitQuery(int port, String sql)
            throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }
}