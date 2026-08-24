package org.pangrova.governance.lineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logging-based {@link LineageBackend} for bootstrap scenarios.
 * Logs lineage events via SLF4J when no external backend (Marquez, DataHub) is configured.
 */
public final class LogLineageBackend implements LineageBackend {

    private static final Logger LOG = LoggerFactory.getLogger(LogLineageBackend.class);

    @Override
    public void send(LineageEvent event) {
        LOG.info("Lineage event: type={}, runId={}, inputs={}, outputs={}",
                event.eventType(), event.runId(),
                event.inputDatasets().size(), event.outputDatasets().size());
    }

    @Override
    public boolean isAvailable() {
        return true;
    }
}
