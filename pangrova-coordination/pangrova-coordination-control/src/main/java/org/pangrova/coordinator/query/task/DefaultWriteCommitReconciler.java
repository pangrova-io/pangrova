package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.enums.WriteRecoveryOutcome;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.pangrova.coordinator.query.task.write.WriteCommitReconciler;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.protocol.catalog.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.common.file.CopyFilePublication;
import org.pangrova.connector.api.metadata.CopyFilePublicationMetadata;
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
 *   <li>{@code ABORTED} — the connector committed an exact durable abort barrier;</li>
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
    private final WriteCommitJournal journal;

    public DefaultWriteCommitReconciler(WriteCommitJournal journal, CatalogResolver catalogResolver) {
        this.journal = checkNotNull(journal, "journal must not be null");
        this.catalogResolver = checkNotNull(catalogResolver, "catalogResolver must not be null");
    }

    @Override
    public WriteRecoveryOutcome reconcile(WriteCommitEntry entry) {
        Optional<CopyFilePublication> recorded = entry.copyFile() == null
                ? Optional.empty() : journal.findCopyFilePublication(entry);
        if (isCommittedCopyMissingPublication(entry, recorded)) {
            LOG.error("event=write_commit_reconcile queryId={} outcome=QUARANTINED "
                    + "reason=committed_copy_missing_publication", entry.queryId());
            return WriteRecoveryOutcome.QUARANTINED;
        }
        SchemaTableName table = parseTable(entry.tableName());
        if (table == null) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=unparseable_identity", entry.queryId(), entry.tableName());
            return WriteRecoveryOutcome.QUARANTINED;
        }
        ConnectorSession session = new RecoveryConnectorSession(
                entry.catalogName(), table.schema(), entry.tenantId(), entry.principal());
        ConnectorCatalogHandle catalog = catalogResolver.catalogFor(entry.catalogName());
        if (catalog == null) {
            LOG.warn("event=write_commit_reconcile queryId={} catalog={} outcome=QUARANTINED "
                    + "reason=unknown_catalog", entry.queryId(), entry.catalogName());
            return WriteRecoveryOutcome.QUARANTINED;
        }
        try {
            var metadata = catalog.getMetadata(session);
            if (entry.cdcSource() != null && !"INSERT".equals(entry.writeMode())
                    && !metadata.supportsCdcMutations()) {
                LOG.error("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                        + "reason=cdc_mutation_sink_unavailable", entry.queryId(), table);
                return WriteRecoveryOutcome.QUARANTINED;
            }
            ConnectorOutputTableHandle handle = decodeExactHandle(entry, table, catalog);
            if (recorded.isPresent()) {
                confirmRecordedCopyPublication(metadata, session, handle, entry, recorded.orElseThrow());
                return WriteRecoveryOutcome.COMMITTED;
            }
            RequiredWritePublication.commit(journal, metadata, session, handle, entry);
            if (entry.copyFile() != null) {
                if (!(metadata instanceof CopyFilePublicationMetadata copyMetadata)) {
                    throw new IllegalStateException("COPY target connector cannot prove snapshot publication");
                }
                journal.recordCopyFilePublication(entry,
                        copyMetadata.confirmCopyFilePublication(session, handle, entry.commitContext()));
            }
            LOG.info("event=write_commit_reconciled queryId={} table={} mode={} outcome=COMMITTED",
                    entry.queryId(), table, entry.writeMode());
            return WriteRecoveryOutcome.COMMITTED;
        } catch (UnsupportedOperationException e) {
            LOG.warn("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=connector_cannot_decode", entry.queryId(), table);
            return WriteRecoveryOutcome.QUARANTINED;
        } catch (RuntimeException e) {
            if (mayConfirmOrdinaryAbort(entry, e)) {
                try {
                    ConnectorOutputTableHandle handle = decodeExactHandle(entry, table, catalog);
                    var receipt = catalog.getMetadata(session).abortDistributedWrite(
                            session, handle, entry.fragments(), entry.commitContext());
                    if (!receipt.executionId().equals(entry.queryId())
                            || !receipt.commitFingerprint().equals(entry.commitFingerprint())) {
                        throw new IllegalStateException("Durable abort receipt differs from journal intent");
                    }
                    return WriteRecoveryOutcome.ABORTED;
                } catch (RuntimeException barrierFailure) {
                    LOG.warn("event=write_commit_reconcile queryId={} outcome=QUARANTINED "
                            + "reason=source_abort_unconfirmed error={}",
                            entry.queryId(), barrierFailure.toString());
                    return WriteRecoveryOutcome.QUARANTINED;
                }
            }
            LOG.error("event=write_commit_reconcile queryId={} table={} outcome=QUARANTINED "
                    + "reason=ambiguous error={}", entry.queryId(), table, e.toString());
            return WriteRecoveryOutcome.QUARANTINED;
        }
    }

    private static boolean isCommittedCopyMissingPublication(
            WriteCommitEntry entry, Optional<CopyFilePublication> recorded) {
        return entry.copyFile() != null && entry.state() == WriteCommitState.COMMITTED
                && recorded.isEmpty();
    }

    private static boolean mayConfirmOrdinaryAbort(WriteCommitEntry entry, RuntimeException failure) {
        return entry.copyFile() == null && (failure
                instanceof org.pangrova.connector.api.exception.SourceTableUnavailableException
                || isTableGone(failure));
    }

    private static void confirmRecordedCopyPublication(
            org.pangrova.connector.api.metadata.ConnectorMetadata metadata,
            ConnectorSession session,
            ConnectorOutputTableHandle handle,
            WriteCommitEntry entry,
            CopyFilePublication recorded) {
        if (!(metadata instanceof CopyFilePublicationMetadata copyMetadata)) {
            throw new IllegalStateException("COPY target connector cannot prove snapshot publication");
        }
        CopyFilePublication current = copyMetadata.confirmCopyFilePublication(
                session, handle, entry.commitContext());
        if (!recorded.equals(current)) {
            throw new IllegalStateException("Current Iceberg snapshot differs from durable COPY publication");
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
                || !ProtocolChecksums.sha256(payload)
                        .equals(outputBinding.getPayloadChecksum())) {
            throw new IllegalArgumentException("Connector output handle codec or checksum differs");
        }
        ConnectorOutputTableHandle handle = codec.deserialize(
                outputBinding.getCodecVersion(), payload);
        validateDecodedHandle(entry, table, catalog, binding, outputBinding, codec, payload, handle);
        return handle;
    }

    private static void validateDecodedHandle(WriteCommitEntry entry, SchemaTableName table,
            ConnectorCatalogHandle catalog, WriteOutputBinding binding,
            org.pangrova.protocol.v2.SerializedConnectorOutputHandle outputBinding,
            ConnectorOutputHandleCodec codec, byte[] payload, ConnectorOutputTableHandle handle) {
        if (!Arrays.equals(codec.serialize(handle), payload)
                || !codec.writeColumnNames(handle).equals(outputBinding.getWriteColumnNamesList())
                || !handle.targetTable().equals(Optional.of(table))
                || !handle.writeMode().equals(entry.writeMode())
                || handle.writeBaseSnapshotId().orElse(-1L) != entry.baseSnapshotId()
                || !org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory
                        .create(catalog, handle).equals(binding)) {
            throw new IllegalArgumentException("Decoded output allocation differs from durable write intent");
        }
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
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

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
