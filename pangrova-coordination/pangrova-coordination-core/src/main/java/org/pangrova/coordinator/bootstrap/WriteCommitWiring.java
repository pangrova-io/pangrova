package org.pangrova.coordinator.bootstrap;

import javax.sql.DataSource;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.memory.query.transaction.InMemoryWriteCommitJournal;
import org.pangrova.coordinator.query.task.DefaultWriteCommitReconciler;
import org.pangrova.persistence.memory.query.transaction.InMemoryWriteCommitQuarantineStore;
import org.pangrova.persistence.jdbc.query.transaction.JdbcWriteCommitQuarantineStore;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;
import org.pangrova.persistence.api.query.transaction.WriteCommitQuarantineStore;
import org.pangrova.coordinator.query.task.write.WriteCommitReconciler;
import org.pangrova.coordinator.query.task.write.WriteCommitRecoveryService;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.persistence.jdbc.query.transaction.JdbcWriteCommitJournal;
import org.pangrova.spi.connector.coordination.WriteCommitJournal;

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
                : new InMemoryWriteCommitJournal();
    }

    /**
     * Quarantine decisions must outlive the coordinator that made them: a peer that
     * takes over recovery must see them too. Only datasource-less deployments fall
     * back to the process-local store.
     */
    static WriteCommitQuarantineStore createQuarantineStore(DataSource metadataDataSource) {
        return metadataDataSource != null
                ? new JdbcWriteCommitQuarantineStore(metadataDataSource)
                : new InMemoryWriteCommitQuarantineStore();
    }

    static WriteCommitRecoveryService createRecoveryService(
            WriteCommitJournal journal, CatalogRegistry catalog,
            WriteCommitQuarantineStore quarantineStore) {
        WriteCommitReconciler reconciler = new DefaultWriteCommitReconciler(journal,
                catalogName -> resolveCatalog(catalog, catalogName));
        return new WriteCommitRecoveryService(journal, reconciler, quarantineStore);
    }

    /**
     * Resolves the exact connector catalog for recovery, or {@code null} when unknown or
     * unavailable — the reconciler then quarantines that intent rather than guessing.
     */
    private static ConnectorCatalogHandle resolveCatalog(
            CatalogRegistry catalog, String catalogName) {
        if (catalog == null) {
            return null;
        }
        try {
            var catalogHandle = catalog.getCatalog(catalogName);
            return catalogHandle instanceof ConnectorCatalogHandle connector ? connector : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Builds the durable per-execution fence for the catalog-commit path.
     */
    static WriteCommitFencer createFencer(HighAvailabilityCoordinationService coordinationService) {
        return coordinationService == null ? null : new ExecutionAnchorFencer(coordinationService);
    }

    /**
     * Fences on the live durable execution anchor. The singleton control term is
     * intentionally not consulted here.
     */
    private static final class ExecutionAnchorFencer implements WriteCommitFencer {

        private final HighAvailabilityCoordinationService hac;

        ExecutionAnchorFencer(HighAvailabilityCoordinationService hac) {
            this.hac = hac;
        }

        @Override
        public org.pangrova.spi.query.coordination.ExecutionAnchor currentAnchor(
                String executionId) {
            return hac.requireExecutionAnchor(executionId);
        }

        @Override
        public boolean isCurrent(String executionId, long fencingToken) {
            return hac.validateDurableExecutionToken(executionId, fencingToken);
        }
    }
}
