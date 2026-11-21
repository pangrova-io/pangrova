package org.pangrova.observability.sdk.alerting;

/**
 * Channel for delivering alert notifications (webhook, email, etc.).
 */
public interface AlertNotificationChannel {

    /**
     * Sends an alert notification for the given fired rule and observed value.
     *
     * @param rule          the rule that fired
     * @param observedValue the metric value that triggered the alert
     */
    void send(AlertRule rule, double observedValue);
}
