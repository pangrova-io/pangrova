package org.pangrova.governance.lineage;

import java.util.List;
import java.util.Objects;

import org.pangrova.spi.execution.eventlistener.QueryCompletedEvent;
import org.pangrova.spi.execution.eventlistener.QueryCreatedEvent;
import org.pangrova.spi.execution.eventlistener.QueryEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges the EventListener SPI ({@link QueryEventListener}) to the governance
 * lineage subsystem ({@link LineageEventEmitter}).
 *
 * <p>Receives query lifecycle events via the standard SPI path and translates
 * them into lineage START/COMPLETE/FAIL events. This is the integration point
 * that wires OpenLineage emission through the same EventListener bus used by
 * audit and cost subsystems (Task 182).</p>
 *
 * <p>Column-level lineage data flows through {@link DatasetRef.ColumnLineage}
 * on the input datasets, populated upstream by the planner's
 * {@code ColumnLineageExtractor}.</p>
 */
public final class LineageQueryEventListener implements QueryEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(LineageQueryEventListener.class);

    private final LineageEventEmitter lineageEmitter;

    public LineageQueryEventListener(LineageEventEmitter lineageEmitter) {
        this.lineageEmitter = Objects.requireNonNull(lineageEmitter, "lineageEmitter must not be null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        // F-GOV-01: Emit OpenLineage START event at query creation time.
        // Dataset lists may be incomplete at this stage (planner has not run),
        // so we emit a minimal START with job name only. The COMPLETE event
        // (from QueryAuditDispatcher.emitLineageOnFinished) will carry full datasets.
        try {
            lineageEmitter.emitStartEvent(
                    event.queryId(),
                    event.sql(),
                    List.of(),  // inputs unknown at creation time
                    List.of()); // outputs unknown at creation time
            LOG.debug("event=lineage_start_emitted queryId={}", event.queryId());
        } catch (RuntimeException e) {
            LOG.warn("event=lineage_start_emission_failed queryId={} error={}",
                    event.queryId(), e.getMessage());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        try {
            if (event.succeeded()) {
                // Lineage COMPLETE emitted — the actual START+COMPLETE with dataset
                // info is driven by QueryAuditDispatcher.emitLineageOnFinished
                // which has access to the QueryLineageInfo containing table refs.
                // This listener provides the event-bus bridge for additional
                // lineage consumers that only observe the SPI.
                lineageEmitter.emitCompleteEvent(event.queryId());
            } else {
                String errorMsg = event.failureMessage().orElse("unknown");
                lineageEmitter.emitFailEvent(event.queryId(), errorMsg);
            }
        } catch (RuntimeException e) {
            LOG.warn("event=lineage_listener_failed queryId={} error={}",
                    event.queryId(), e.getMessage(), e);
        }
    }
}
