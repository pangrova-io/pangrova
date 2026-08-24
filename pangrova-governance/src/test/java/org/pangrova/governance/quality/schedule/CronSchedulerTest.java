package org.pangrova.governance.quality.schedule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.InMemoryQualityRuleStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CronScheduler} leader election integration.
 * Verifies that scheduled quality checks only execute on the leader node.
 */
class CronSchedulerTest {

    private CronScheduler scheduler;

    @AfterEach
    void cleanup() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    @Test
    void leaderExecutesScheduledCheck() {
        var counter = new AtomicInteger(0);
        GovernanceStatementHandler handler = countingHandler(counter);

        scheduler = new CronScheduler(handler, () -> true);
        scheduler.runCheck(rule(1, "check_rule"));

        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    void nonLeaderSkipsScheduledCheck() {
        var counter = new AtomicInteger(0);
        GovernanceStatementHandler handler = countingHandler(counter);

        scheduler = new CronScheduler(handler, () -> false);
        scheduler.runCheck(rule(1, "check_rule"));

        assertThat(counter.get()).isZero();
    }

    @Test
    void dynamicLeaderSwitchAllowsExecution() {
        var counter = new AtomicInteger(0);
        GovernanceStatementHandler handler = countingHandler(counter);
        var isLeader = new AtomicBoolean(false);

        scheduler = new CronScheduler(handler, isLeader::get);

        // Not leader — skip
        scheduler.runCheck(rule(1, "check_rule"));
        assertThat(counter.get()).isZero();

        // Become leader — execute
        isLeader.set(true);
        scheduler.runCheck(rule(1, "check_rule"));
        assertThat(counter.get()).isEqualTo(1);

        // Step down — skip again
        isLeader.set(false);
        scheduler.runCheck(rule(1, "check_rule"));
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    void defaultConstructorAlwaysExecutes() {
        var counter = new AtomicInteger(0);
        GovernanceStatementHandler handler = countingHandler(counter);

        scheduler = new CronScheduler(handler);
        scheduler.runCheck(rule(1, "check_rule"));

        assertThat(counter.get())
                .as("default constructor implies single-coordinator mode (always leader)")
                .isEqualTo(1);
    }

    @Test
    void twoCoordinatorsExactlyOnceExecution() {
        var leaderCounter = new AtomicInteger(0);
        var followerCounter = new AtomicInteger(0);
        GovernanceStatementHandler leaderHandler = countingHandler(leaderCounter);
        GovernanceStatementHandler followerHandler = countingHandler(followerCounter);

        try (var leader = new CronScheduler(leaderHandler, () -> true);
             var follower = new CronScheduler(followerHandler, () -> false)) {

            DataQualityRule rule = rule(1, "shared_rule");
            leader.runCheck(rule);
            follower.runCheck(rule);

            assertThat(leaderCounter.get()).isEqualTo(1);
            assertThat(followerCounter.get()).isZero();
            assertThat(leaderCounter.get() + followerCounter.get())
                    .as("with 2 coordinators, quality check runs exactly once per interval")
                    .isEqualTo(1);
        }
    }

    @Test
    void nullLeaderCheckThrowsNullPointerException() {
        assertThatThrownBy(() -> new CronScheduler(simpleHandler(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("leaderCheck");
    }

    @Test
    void nullHandlerThrowsNullPointerException() {
        assertThatThrownBy(() -> new CronScheduler(null, () -> true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("handler");
    }

    @Test
    void scheduleAndCancelRule() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        DataQualityRule rule = rule(1, "cancel_test");
        scheduler.scheduleRule(rule);
        assertThatCode(() -> scheduler.cancelRule(rule.ruleId()))
                .doesNotThrowAnyException();
    }

    @Test
    void computeNextDelayForEveryMinuteCron() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        long delayMs = scheduler.computeNextDelayMs("* * * * *");
        // Every-minute cron: next execution is within 60 seconds
        assertThat(delayMs).isBetween(0L, 60_000L);
    }

    @Test
    void computeNextDelayForEveryFiveMinutesCron() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        long delayMs = scheduler.computeNextDelayMs("*/5 * * * *");
        // Every 5 minutes: next execution is within 5 minutes
        assertThat(delayMs).isBetween(0L, 300_000L);
    }

    @Test
    void computeNextDelayForHourlyCron() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        long delayMs = scheduler.computeNextDelayMs("0 * * * *");
        // Hourly at minute 0: next execution is within 1 hour
        assertThat(delayMs).isBetween(0L, 3_600_000L);
    }

    @Test
    void invalidCronExpressionFallsBackToDefaultInterval() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        long delayMs = scheduler.computeNextDelayMs("invalid_expression");
        // Invalid expression falls back to default 1 hour
        assertThat(delayMs).isEqualTo(3_600_000L);
    }

    @Test
    void numericOnlyScheduleFallsBackToDefaultInterval() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        // Old-style numeric schedule (e.g. "60") is no longer valid cron
        long delayMs = scheduler.computeNextDelayMs("60");
        assertThat(delayMs).isEqualTo(3_600_000L);
    }

    @Test
    void closeIsIdempotent() {
        scheduler = new CronScheduler(simpleHandler(), () -> true);
        assertThatCode(() -> {
            scheduler.close();
            scheduler.close();
        }).doesNotThrowAnyException();
        scheduler = null; // prevent double-close in @AfterEach
    }

    @Test
    void handlerExceptionDoesNotPropagateFromRunCheck() {
        GovernanceStatementHandler throwingHandler = new GovernanceStatementHandler(
                new InMemoryQualityRuleStore(), new DefaultQualityRuleExecutor(),
                new DataQualityAlerter(new AlertChannelRegistry())) {
            @Override
            public List<QualityCheckResult> handleRunCheck(String c, String s, String t) {
                throw new RuntimeException("simulated failure");
            }
        };

        scheduler = new CronScheduler(throwingHandler, () -> true);
        assertThatCode(() -> scheduler.runCheck(rule(1, "failing_rule")))
                .doesNotThrowAnyException();
    }

    // --- Helpers ---

    private GovernanceStatementHandler countingHandler(AtomicInteger counter) {
        var ruleStore = new InMemoryQualityRuleStore();
        ruleStore.saveRule(rule(1, "check_rule"));
        var alerter = new DataQualityAlerter(new AlertChannelRegistry());
        return new GovernanceStatementHandler(ruleStore, new DefaultQualityRuleExecutor(), alerter) {
            @Override
            public List<QualityCheckResult> handleRunCheck(String catalogName,
                                                           String schemaName,
                                                           String tableName) {
                counter.incrementAndGet();
                return super.handleRunCheck(catalogName, schemaName, tableName);
            }
        };
    }

    private GovernanceStatementHandler simpleHandler() {
        return new GovernanceStatementHandler(
                new InMemoryQualityRuleStore(), new DefaultQualityRuleExecutor(),
                new DataQualityAlerter(new AlertChannelRegistry()));
    }

    private static DataQualityRule rule(long id, String name) {
        return new DataQualityRule(id, name, "catalog", "schema", "table",
                "expression", Severity.WARNING, "msg", true, "0 * * * *",
                Instant.now(), Instant.now());
    }
}
