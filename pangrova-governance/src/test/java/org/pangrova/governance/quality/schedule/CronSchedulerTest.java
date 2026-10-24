package org.pangrova.governance.quality.schedule;

import org.pangrova.governance.quality.rule.QualityExecutionIdentity;
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
import java.util.Optional;
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
    void onlyCurrentLeaderAdmitsExecution() {
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
                    .as("the current leader admits the check while the follower skips it")
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
            public Optional<QualityCheckResult> handleRunRule(long ruleId) {
                throw new RuntimeException("simulated failure");
            }
        };

        scheduler = new CronScheduler(throwingHandler, () -> true);
        assertThatCode(() -> scheduler.runCheck(rule(1, "failing_rule")))
                .doesNotThrowAnyException();
    }

    @Test
    void scheduledCheckRunsOnlyItsCurrentRuleAndAuditsFailedEvaluation() {
        var store = new InMemoryQualityRuleStore();
        DataQualityRule scheduled = rule(1, "scheduled");
        store.saveRule(scheduled);
        store.saveRule(rule(2, "sibling"));
        var observed = new java.util.ArrayList<DataQualityRule>();
        var handler = new GovernanceStatementHandler(store, current -> {
            observed.add(current);
            return new QualityCheckResult(0, current.ruleId(), current.ruleName(), current.tableName(),
                    current.severity(), Instant.now(), false, 5, 2, 1, "2 violations");
        }, new DataQualityAlerter(new AlertChannelRegistry()));
        var audit = new org.pangrova.governance.audit.InMemoryGovernanceAuditTrail();
        scheduler = new CronScheduler(handler, () -> true, audit);
        store.saveRule(rule(1, "changed_after_scheduling"));

        scheduler.runCheck(scheduled);

        assertThat(observed).extracting(DataQualityRule::ruleName).containsExactly("changed_after_scheduling");
        assertThat(store.getRecentResults()).hasSize(1);
        assertThat(audit.getEntriesForResource("catalog.schema.table")).singleElement()
                .satisfies(entry -> assertThat(entry.afterState()).containsEntry("outcome", "FAILED"));
    }

    @Test
    void disabledAndDeletedRulesDoNotExecuteStaleCallbacks() {
        var store = new InMemoryQualityRuleStore();
        var executions = new AtomicInteger();
        var handler = new GovernanceStatementHandler(store, current -> {
            executions.incrementAndGet();
            throw new AssertionError("Disabled or deleted rule reached executor");
        }, new DataQualityAlerter(new AlertChannelRegistry()));
        var audit = new org.pangrova.governance.audit.InMemoryGovernanceAuditTrail();
        scheduler = new CronScheduler(handler, () -> true, audit);
        DataQualityRule scheduled = rule(1, "scheduled");
        store.saveRule(new DataQualityRule(1, "disabled", "catalog", "schema", "table",
                "true", Severity.WARNING, "msg", false, "0 * * * *", Instant.now(), Instant.now(), null));
        scheduler.runCheck(scheduled);
        scheduler.runCheck(rule(2, "deleted"));
        assertThat(executions).hasValue(0);
        assertThat(store.getRecentResults()).isEmpty();
        assertThat(audit.getEntriesForResource("catalog.schema.table")).hasSize(2)
                .allSatisfy(entry -> assertThat(entry.afterState()).containsEntry("outcome", "SKIPPED"));
    }

    // --- Helpers ---

    private GovernanceStatementHandler countingHandler(AtomicInteger counter) {
        var ruleStore = new InMemoryQualityRuleStore();
        ruleStore.saveRule(rule(1, "check_rule"));
        var alerter = new DataQualityAlerter(new AlertChannelRegistry());
        return new GovernanceStatementHandler(ruleStore, new DefaultQualityRuleExecutor(), alerter) {
            @Override
            public Optional<QualityCheckResult> handleRunRule(long ruleId) {
                counter.incrementAndGet();
                return super.handleRunRule(ruleId);
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
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
    }
}
