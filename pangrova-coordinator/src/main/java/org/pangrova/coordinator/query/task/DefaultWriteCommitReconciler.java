package org.pangrova.coordinator.query.task;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Arrays;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Real {@link WriteCommitReconciler} (A-COMMIT-01): AUTO-FINISHES a distributed-write intent left
 * {@code PENDING} by a crashed coordinator, instead of always quarantining for manual intervention.
 *
 * <p>Recovery decodes the exact canonical v2 write binding sent to Workers. A changed
 * catalog incarnation or revision is quarantined, never rebound to the current table.</p>
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
     * Resolves the current connector catalog, or {@code null} if unavailable.
     */
    @FunctionalInterface
    public interface CatalogResolver {
        ConnectorCatalogHandle catalogFor(String catalogName);
    }

    private final CatalogResolver catalogResolver;

    public DefaultWriteCommitReconciler(CatalogResolver catalogResolver) {
        this.catalogResolver = checkNotNull(catalogResolver, "catalogResolver must not be null");
    }

    @Override
    public RecoveryOutcome reconcile(WriteCommitEntry entry) {
        SchemaTableName table = parseTable(entry.tableName());
        if (table == null) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=unparseable_identity", entry.queryId(), entry.tableName());
            return RecoveryOutcome.QUARANTINED;
        }
        ConnectorSession session = new RecoveryConnectorSession(
                entry.catalogName(), table.schema(), entry.tenantId(), entry.principal());
        ConnectorCatalogHandle catalog = catalogResolver.catalogFor(entry.catalogName());
        if (catalog == null) {
            LOG.warn("event=write_commit_reconcile queryId={} catalog={} outcome=QUARANTINED "
                    + "reason=unknown_catalog", entry.queryId(), entry.catalogName());
            return RecoveryOutcome.QUARANTINED;
        }
        try {
            ConnectorOutputTableHandle handle = decodeExactHandle(entry, table, catalog);
            catalog.getMetadata(session).finishDistributedWrite(
                    session, handle, entry.fragments(), entry.commitContext());
            LOG.info("event=write_commit_reconciled queryId={} table={} mode={} outcome=COMMITTED",
                    entry.queryId(), table, entry.writeMode());
            return RecoveryOutcome.COMMITTED;
        } catch (UnsupportedOperationException e) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=connector_cannot_decode", entry.queryId(), table);
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

    static ConnectorOutputTableHandle decodeExactHandle(
            WriteCommitEntry entry, SchemaTableName table,
            ConnectorCatalogHandle catalog) {
        WriteOutputBinding binding;
        try {
            binding = WriteOutputBinding.parseFrom(entry.writeOutputBinding());
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Malformed durable write binding", malformed);
        }
        if (!binding.hasConnector() && !binding.hasLance() && !binding.hasPhysical()) {
            throw new IllegalArgumentException("Durable write binding has no physical output allocation");
        }
        var catalogBinding = binding.hasPhysical() ? binding.getPhysical().getCatalog() : (binding.hasLance()
                ? binding.getLance().getCatalog() : binding.getConnector().getCatalog());
        validateCatalog(entry, catalog, catalogBinding);
        var outputBinding = binding.hasPhysical() ? binding.getPhysical().getOutputHandle() : (binding.hasLance()
                ? binding.getLance().getOutputHandle() : binding.getConnector().getOutputHandle());
        ConnectorOutputHandleCodec codec = catalog.getOutputHandleCodec();
        byte[] payload = outputBinding.getPayload().toByteArray();
        if (!codec.typeUrl().equals(outputBinding.getTypeUrl())
                || codec.codecVersion() != outputBinding.getCodecVersion()
                || !CatalogDeploymentFingerprint.sha256(payload)
                        .equals(outputBinding.getPayloadChecksum())) {
            throw new IllegalArgumentException("Connector output handle codec or checksum differs");
        }
        ConnectorOutputTableHandle handle = codec.deserialize(
                outputBinding.getCodecVersion(), payload);
        if (!Arrays.equals(codec.serialize(handle), payload)
                || !codec.writeColumnNames(handle).equals(outputBinding.getWriteColumnNamesList())
                || !handle.targetTable().equals(Optional.of(table))
                || !handle.writeMode().equals(entry.writeMode())
                || handle.writeBaseSnapshotId().orElse(-1L) != entry.baseSnapshotId()
                || !org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory
                        .create(catalog, handle).equals(binding)) {
            throw new IllegalArgumentException("Decoded output allocation differs from durable write intent");
        }
        return handle;
    }

    private static void validateCatalog(WriteCommitEntry entry, ConnectorCatalogHandle catalog,
            org.pangrova.protocol.v2.CatalogExecutionBinding catalogBinding) {
        CatalogDescriptor descriptor = catalog.getDescriptor();
        if (!entry.catalogName().equals(catalogBinding.getCatalogName())
                || !descriptor.name().equals(catalogBinding.getCatalogName())
                || !descriptor.provider().equals(catalogBinding.getConnectorProvider())
                || !descriptor.incarnationId().equals(catalogBinding.getCatalogIncarnationId())
                || descriptor.revision() != catalogBinding.getCatalogRevision()
                || !CatalogDeploymentFingerprint.matches(
                        catalogBinding.getConfigurationFingerprint(),
                        new CatalogDeploymentResponse(
                                descriptor.name(), descriptor.provider(),
                                descriptor.incarnationId(), descriptor.revision(),
                                descriptor.properties(), descriptor.allowedTenants()))) {
            throw new IllegalArgumentException("Catalog deployment differs from durable write binding");
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

    /** Recovery session whose tenant is the immutable identity persisted before the first commit. */
    private record RecoveryConnectorSession(
            String catalog, String schema, String tenantId, String principal) implements ConnectorSession {

        @Override
        public String getUser() {
            return principal;
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.of(catalog);
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.of(schema);
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of("tenant_id", tenantId);
        }
    }
}
