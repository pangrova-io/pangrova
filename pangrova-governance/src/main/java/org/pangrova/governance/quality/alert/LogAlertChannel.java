package org.pangrova.governance.quality.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link AlertChannel} that delivers alerts via SLF4J logging.
 * Used when no external alert backend (Slack, PagerDuty, etc.) is configured.
 */
public final class LogAlertChannel implements AlertChannel {

    private static final Logger LOG = LoggerFactory.getLogger(LogAlertChannel.class);

    @Override
    public String name() {
        return "log";
    }

    @Override
    public void send(Alert alert) {
        LOG.warn("[DATA_QUALITY] rule={} table={} severity={} failed={}/{} message={}",
                alert.ruleName(), alert.tableName(), alert.severity(),
                alert.failedRows(), alert.totalRows(), alert.message());
    }

    @Override
    public boolean supports(Severity severity) {
        return true;
    }
}
