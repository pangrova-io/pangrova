package org.pangrova.observability.alerting;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates configured alert rules against current metric values and dispatches
 * notifications through registered channels when rules fire.
 */
public final class AlertRuleEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(AlertRuleEvaluator.class);

    private final List<AlertRule> rules;
    private final List<AlertNotificationChannel> channels;

    /**
     * Creates an evaluator with the given rules and notification channels.
     *
     * @param rules    the alert rules to evaluate
     * @param channels the notification channels to dispatch alerts to
     */
    public AlertRuleEvaluator(List<AlertRule> rules, List<AlertNotificationChannel> channels) {
        if (rules == null) {
            throw new IllegalArgumentException("rules must not be null");
        }
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("channels must not be null or empty");
        }
        this.rules = List.copyOf(rules);
        this.channels = List.copyOf(channels);
    }

    /**
     * Returns the configured rules (unmodifiable).
     */
    public List<AlertRule> getRules() {
        return rules;
    }

    /**
     * Evaluates a single metric observation against all rules matching the metric name.
     * Fires notifications for any rule whose condition is satisfied.
     *
     * @param metricName    the name of the metric being observed
     * @param observedValue the current metric value
     * @return the number of rules that fired
     */
    public int evaluate(String metricName, double observedValue) {
        int fired = 0;
        for (AlertRule rule : rules) {
            if (!rule.metricName().equals(metricName)) {
                continue;
            }
            if (rule.condition().evaluate(observedValue, rule.threshold())) {
                fired++;
                LOG.warn("Alert fired: rule={} metric={} observed={} threshold={} severity={}",
                        rule.name(), metricName, observedValue, rule.threshold(), rule.severity());
                dispatchNotifications(rule, observedValue);
            }
        }
        return fired;
    }

    private void dispatchNotifications(AlertRule rule, double observedValue) {
        for (AlertNotificationChannel channel : channels) {
            try {
                channel.send(rule, observedValue);
            } catch (RuntimeException e) {
                LOG.error("Failed to send alert notification: rule={} channel={} error={}",
                        rule.name(), channel.getClass().getSimpleName(), e.getMessage(), e);
            }
        }
    }
}
