package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.statement.StatementConnectorSession;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Real {@link WriteCommitReconciler} (A-COMMIT-01): AUTO-FINISHES a distributed-write intent left
 * {@code PENDING} by a crashed coordinator, instead of always quarantining for manual intervention.
 *
 * <p>Using the structured, rehydratable identity now recorded in the durable intent
 * ({@code catalog} + {@code schema.table} + {@code writeMode} + worker {@code fragments}), it
 * reconstructs the connector write handle from the live table metadata via
 * {@link ConnectorMetadata#reconstructOutputHandle} and re-drives the catalog commit via
 * {@link ConnectorMetadata#finishDistributedWrite}, which is contractually idempotent (re-driving
 * an already-landed commit with the same fragments is a no-op).</p>
 *
 * <p>Outcomes:
 * <ul>
 *   <li>{@code COMMITTED} — handle rehydrated and the (idempotent) commit succeeded;</li>
 *   <li>{@code ABORTED} — the target table no longer exists, so the write can never land;</li>
 *   <li>{@code QUARANTINED} — the connector cannot rehydrate, the identity is unparseable/legacy,
 *       the catalog is unknown, or the outcome is otherwise ambiguous (manual intervention).</li>
 * </ul>
 */
public final class DefaultWriteCommitReconciler implements WriteCommitReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteCommitReconciler.class);

    /**
     * Resolves connector metadata for a catalog under a recovery session, or {@code null} if the
     * catalog is unknown. Injected (rather than a concrete {@code CatalogRegistry}) so the reconciler
     * is decoupled from the sealed catalog-handle hierarchy and unit-testable.
     */
    @FunctionalInterface
    public interface MetadataResolver {
        ConnectorMetadata metadataFor(String catalogName, ConnectorSession session);
    }

    private final MetadataResolver metadataResolver;

    public DefaultWriteCommitReconciler(MetadataResolver metadataResolver) {
        this.metadataResolver = checkNotNull(metadataResolver, "metadataResolver must not be null");
    }

    @Override
    public RecoveryOutcome reconcile(WriteCommitEntry entry) {
        if (!entry.isReplayable()) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=no_fragments", entry.queryId(), entry.tableName());
            return RecoveryOutcome.QUARANTINED;
        }
        SchemaTableName table = parseTable(entry.tableName());
        if (table == null) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=unparseable_identity", entry.queryId(), entry.tableName());
            return RecoveryOutcome.QUARANTINED;
        }
        ConnectorSession session = new StatementConnectorSession(entry.catalogName(), table.schema());
        ConnectorMetadata metadata = metadataResolver.metadataFor(entry.catalogName(), session);
        if (metadata == null) {
            LOG.warn("event=write_commit_reconcile queryId={} catalog={} outcome=QUARANTINED "
                    + "reason=unknown_catalog", entry.queryId(), entry.catalogName());
            return RecoveryOutcome.QUARANTINED;
        }
        try {
            ConnectorOutputTableHandle handle =
                    metadata.reconstructOutputHandle(session, table, entry.writeMode());
            // Idempotent by contract: re-driving a committed write (same fragments) is a no-op, so
            // this both finishes crashed-mid-commit writes and safely re-confirms landed ones.
            metadata.finishDistributedWrite(session, handle, entry.fragments());
            LOG.info("event=write_commit_reconciled queryId={} table={} mode={} outcome=COMMITTED",
                    entry.queryId(), table, entry.writeMode());
            return RecoveryOutcome.COMMITTED;
        } catch (UnsupportedOperationException e) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=connector_cannot_rehydrate", entry.queryId(), table);
            return RecoveryOutcome.QUARANTINED;
        } catch (RuntimeException e) {
            if (isTableGone(e)) {
                LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=ABORTED "
                        + "reason=table_gone", entry.queryId(), table);
                return RecoveryOutcome.ABORTED;
            }
            LOG.error("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=ambiguous error={}", entry.queryId(), table, e.toString());
            return RecoveryOutcome.QUARANTINED;
        }
    }

    /**
     * Parses a {@code "schema.table"} identity (as produced by {@link SchemaTableName#toString()});
     * returns {@code null} for anything else (e.g. legacy {@code handle.toString()} records or
     * multi-dot names) so such intents safely quarantine rather than mis-rehydrate.
     */
    private static SchemaTableName parseTable(String tableName) {
        if (tableName == null) {
            return null;
        }
        int dot = tableName.indexOf('.');
        if (dot <= 0 || dot >= tableName.length() - 1) {
            return null;
        }
        String schema = tableName.substring(0, dot);
        String table = tableName.substring(dot + 1);
        if (table.indexOf('.') >= 0) {
            return null;
        }
        return new SchemaTableName(schema, table);
    }

    /** Connector-agnostic heuristic: does the failure chain indicate the table no longer exists? */
    private static boolean isTableGone(RuntimeException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String n = t.getClass().getSimpleName();
            if (n.contains("NoSuchTable") || n.contains("TableNotFound")) {
                return true;
            }
        }
        return false;
    }
}
