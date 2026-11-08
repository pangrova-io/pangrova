package org.pangrova.governance.core.quality;

import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.Alert;
import org.pangrova.governance.api.quality.AlertChannel;
import org.pangrova.governance.api.quality.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Sends alerts to matching channels when quality checks fail.
 */
public class DataQualityAlerter {

    private static final Logger LOG = LoggerFactory.getLogger(DataQualityAlerter.class);

    private final AlertChannelRegistry channelRegistry;

    public DataQualityAlerter(AlertChannelRegistry channelRegistry) {
        this.channelRegistry = channelRegistry;
    }

    public void alert(QualityCheckResult result) {
        if (result.passed()) return;
        Alert alert = buildAlert(result);
        sendToMatchingChannels(alert, result.severity());
    }

    private Alert buildAlert(QualityCheckResult result) {
        return new Alert(
            result.ruleName(), result.tableName(), result.severity(),
            result.errorMessage() != null ? result.errorMessage() : "Quality check failed",
            result.failedRows(), result.totalRows(), result.checkTime());
    }

    private void sendToMatchingChannels(Alert alert, Severity severity) {
        List<AlertChannel> channels = channelRegistry.getChannels(severity);
        for (AlertChannel channel : channels) {
            trySend(channel, alert);
        }
    }

    private void trySend(AlertChannel channel, Alert alert) {
        try {
            channel.send(alert);
        } catch (Exception e) {
            LOG.warn("Failed to send alert via channel {}: {}", channel.name(), e.getMessage());
        }
    }
}
