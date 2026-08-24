package org.pangrova.coordinator.bootstrap;

import javax.sql.DataSource;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal;
import org.pangrova.coordinator.query.task.DefaultWriteCommitReconciler;
import org.pangrova.coordinator.query.task.WriteCommitFencer;
import org.pangrova.coordinator.query.task.WriteCommitReconciler;
import org.pangrova.coordinator.query.task.WriteCommitRecoveryService;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.coordination.JdbcWriteCommitJournal;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.coordination.WriteCommitJournal;

/**
 * Wires the distributed write-commit path: the commit journal, startup recovery of intents a
 * crashed predecessor left behind, and the leadership fence. Extracted from QueryBootstrap to
 * keep bootstrap wiring under the 500-line quality gate.
 */
final class WriteCommitWiring {

    private WriteCommitWiring() {
    }

    /**
     * A-COMMIT-01: selects the DURABLE journal when a metadata datasource is configured, so
     * distributed-write intents survive coordinator crash/failover. Falls back to the in-memory
     * journal only for single-node / no-datasource deployments.
     */
    static WriteCommitJournal createJournal(DataSource metadataDataSource) {
        return metadataDataSource != null
                ? new JdbcWriteCommitJournal(metadataDataSource)
                : InMemoryWriteCommitJournal.INSTANCE;
    }

    /**
     * A-COMMIT-01: on startup/promotion, consumes any durable PENDING write-commit intents a
     * crashed predecessor left behind before this coordinator accepts new writes. The reconciler
     * AUTO-FINISHES recoverable intents (rehydrate the connector handle from the structured
     * durable identity + re-drive the idempotent catalog commit); only genuinely unrecoverable
     * or ambiguous intents are quarantined (raised as CRITICAL alarms).
     *
     * <p>A no-op without a metadata datasource, because the in-memory journal never carries
     * intents from a previous process.</p>
     */
    static void recoverPendingIntents(DataSource metadataDataSource,
                                      WriteCommitJournal journal,
                                      CatalogRegistry catalog) {
        if (metadataDataSource == null) {
            return;
        }
        WriteCommitReconciler reconciler = new DefaultWriteCommitReconciler(
                (catalogName, session) -> resolveMetadata(catalog, catalogName, session));
        new WriteCommitRecoveryService(journal, reconciler).recover();
    }

    /**
     * Resolves connector metadata for recovery, or {@code null} when the catalog is unknown or
     * unavailable — the reconciler then quarantines that intent rather than guessing.
     */
    private static ConnectorMetadata resolveMetadata(CatalogRegistry catalog, String catalogName,
                                                     ConnectorSession session) {
        if (catalog == null) {
            return null;
        }
        try {
            var catalogHandle = catalog.getCatalog(catalogName);
            return catalogHandle == null ? null : catalogHandle.getMetadata(session);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * A-FENCE-01: builds the leadership fence for the catalog-commit path. Returns {@code null}
     * when there is no HA service, which leaves the commit path unfenced for single-node
     * deployments.
     */
    static WriteCommitFencer createFencer(HighAvailabilityCoordinationService coordinationService) {
        return coordinationService == null ? null : new LeaderTermFencer(coordinationService);
    }

    /**
     * Fences on live HA leadership state. While HA has not started (local node id still null) the
     * node is treated as leader, so single-node writes are never fenced; once demoted,
     * {@link #isActiveLeader()} flips false and the commit is left PENDING for the new leader's
     * reconciler.
     */
    private static final class LeaderTermFencer implements WriteCommitFencer {

        private final HighAvailabilityCoordinationService hac;

        LeaderTermFencer(HighAvailabilityCoordinationService hac) {
            this.hac = hac;
        }

        @Override
        public long currentTerm() {
            return hac.getCurrentEpoch();
        }

        @Override
        public String coordinatorId() {
            return hac.getLocalNodeId();
        }

        @Override
        public boolean isActiveLeader() {
            String id = hac.getLocalNodeId();
            return id == null || hac.isLeader(id);
        }
    }
}
