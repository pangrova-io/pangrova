package org.pangrova.observability.alerting;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertRuleEvaluatorTest {

    @Test
    void rulesAreCopiedAndCannotBeChangedThroughAccessor() {
        var rule = new AlertRule("test", "metric", AlertCondition.GREATER_THAN,
                0.0, Duration.ofMinutes(1), AlertSeverity.INFO);
        var rules = new ArrayList<>(List.of(rule));
        var evaluator = new AlertRuleEvaluator(rules, List.of((r, v) -> {}));
        rules.clear();
        assertThat(evaluator.getRules()).containsExactly(rule);
        assertThatThrownBy(() -> evaluator.getRules().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void ruleFiresWhenConditionMet() {
        AlertRule rule = new AlertRule("high-failure-rate", "query.failure.rate",
                AlertCondition.GREATER_THAN, 0.05, Duration.ofMinutes(5), AlertSeverity.CRITICAL);

        List<AlertRule> firedRules = new ArrayList<>();
        AlertNotificationChannel channel = (r, v) -> firedRules.add(r);

        AlertRuleEvaluator evaluator = new AlertRuleEvaluator(List.of(rule), List.of(channel));
        int fired = evaluator.evaluate("query.failure.rate", 0.10);

        assertThat(fired).isEqualTo(1);
        assertThat(firedRules).hasSize(1);
        assertThat(firedRules.get(0).name()).isEqualTo("high-failure-rate");
    }

    @Test
    void ruleDoesNotFireWhenConditionNotMet() {
        AlertRule rule = new AlertRule("high-failure-rate", "query.failure.rate",
                AlertCondition.GREATER_THAN, 0.05, Duration.ofMinutes(5), AlertSeverity.WARNING);

        List<AlertRule> firedRules = new ArrayList<>();
        AlertNotificationChannel channel = (r, v) -> firedRules.add(r);

        AlertRuleEvaluator evaluator = new AlertRuleEvaluator(List.of(rule), List.of(channel));
        int fired = evaluator.evaluate("query.failure.rate", 0.02);

        assertThat(fired).isEqualTo(0);
        assertThat(firedRules).isEmpty();
    }

    @Test
    void ruleDoesNotFireForDifferentMetric() {
        AlertRule rule = new AlertRule("high-memory", "memory.usage",
                AlertCondition.GREATER_THAN, 0.90, Duration.ofMinutes(1), AlertSeverity.CRITICAL);

        List<AlertRule> firedRules = new ArrayList<>();
        AlertNotificationChannel channel = (r, v) -> firedRules.add(r);

        AlertRuleEvaluator evaluator = new AlertRuleEvaluator(List.of(rule), List.of(channel));
        int fired = evaluator.evaluate("cpu.usage", 0.95);

        assertThat(fired).isEqualTo(0);
        assertThat(firedRules).isEmpty();
    }

    @Test
    void multipleRulesCanFireForSameMetric() {
        AlertRule warning = new AlertRule("mem-warn", "memory.usage",
                AlertCondition.GREATER_THAN, 0.80, Duration.ofMinutes(5), AlertSeverity.WARNING);
        AlertRule critical = new AlertRule("mem-crit", "memory.usage",
                AlertCondition.GREATER_THAN, 0.95, Duration.ofMinutes(1), AlertSeverity.CRITICAL);

        List<AlertRule> firedRules = new ArrayList<>();
        AlertNotificationChannel channel = (r, v) -> firedRules.add(r);

        AlertRuleEvaluator evaluator = new AlertRuleEvaluator(
                List.of(warning, critical), List.of(channel));
        int fired = evaluator.evaluate("memory.usage", 0.97);

        assertThat(fired).isEqualTo(2);
        assertThat(firedRules).hasSize(2);
    }

    @Test
    void channelFailureDoesNotPreventOtherChannels() {
        AlertRule rule = new AlertRule("test-rule", "test.metric",
                AlertCondition.GREATER_THAN, 0.0, Duration.ofMinutes(1), AlertSeverity.INFO);

        List<Double> received = new ArrayList<>();
        AlertNotificationChannel failingChannel = (r, v) -> {
            throw new RuntimeException("channel down");
        };
        AlertNotificationChannel workingChannel = (r, v) -> received.add(v);

        AlertRuleEvaluator evaluator = new AlertRuleEvaluator(
                List.of(rule), List.of(failingChannel, workingChannel));
        int fired = evaluator.evaluate("test.metric", 1.0);

        assertThat(fired).isEqualTo(1);
        assertThat(received).containsExactly(1.0);
    }

    @Test
    void nullRulesThrowsException() {
        AlertNotificationChannel channel = (r, v) -> {};
        assertThatThrownBy(() -> new AlertRuleEvaluator(null, List.of(channel)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyChannelsThrowsException() {
        assertThatThrownBy(() -> new AlertRuleEvaluator(List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
