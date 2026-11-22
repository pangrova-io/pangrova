package org.pangrova.coordinator.streaming;

import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory;
import org.pangrova.coordinator.query.task.write.WriteAllocationIntent;
import org.pangrova.execution.write.WriteAuthorityAdmission;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.resolution.ConnectorTableLookup;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.query.api.write.WriteAuthorityRequest;
import org.pangrova.query.api.write.WriteAuthorityResolver;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.streaming.api.checkpoint.StreamState;

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Resolves explicit CDC stream write authority and allocates its bound connector handle.
 *
 * <p>Privileges are never inferred. The stream must declare
 * {@code cdc.write.privileges} as a comma-separated subset of INSERT, UPDATE and
 * DELETE. The existing write-authority resolver rechecks current authority
 * against the live table declaration before any output handle is allocated.</p>
 */
public final class CdcWriteAuthority {

    public static final String PRIVILEGES_PROPERTY = "cdc.write.privileges";

    private CdcWriteAuthority() {}

    /** Parses the explicit stream write actions; missing or invalid actions fail closed. */
    public static Set<Privilege> privileges(StreamState state) {
        String configured = state.streamProperties().get(PRIVILEGES_PROPERTY);
        if (configured == null || configured.isBlank()) {
            throw new AccessDeniedException(
                    "CDC stream requires explicit " + PRIVILEGES_PROPERTY);
        }
        EnumSet<Privilege> actions = EnumSet.noneOf(Privilege.class);
        for (String token : configured.split(",")) {
            String normalized = token.trim().toUpperCase(Locale.ROOT);
            if (normalized.isEmpty()) {
                continue;
            }
            try {
                Privilege action = Privilege.valueOf(normalized);
                if (action != Privilege.INSERT && action != Privilege.UPDATE
                        && action != Privilege.DELETE) {
                    throw new IllegalArgumentException("Unsupported CDC write action: " + normalized);
                }
                actions.add(action);
            } catch (IllegalArgumentException invalid) {
                throw new AccessDeniedException(
                        "Invalid CDC write action '" + normalized + "' in " + PRIVILEGES_PROPERTY);
            }
        }
        if (actions.isEmpty()) {
            throw new AccessDeniedException(
                    "CDC stream requires at least one write action in " + PRIVILEGES_PROPERTY);
        }
        return Set.copyOf(actions);
    }

    /** Allocates the exact connector output handle after current authority is resolved. */
    public static CdcWritePlan allocate(
            StreamState state,
            CatalogHandle catalogHandle,
            ConnectorSession session,
            WriteAuthorityResolver resolver) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(catalogHandle, "catalogHandle");
        Objects.requireNonNull(session, "session");
        Set<Privilege> actions = privileges(state);
        QualifiedTableName table = QualifiedTableName.resolve(
                List.of(state.targetTable().split("\\.")));
        ConnectorMetadata metadata = catalogHandle.getMetadata(session);
        var tableHandle = ConnectorTableLookup.requireTableHandle(metadata, session, table);
        var tableMetadata = metadata.getTableMetadata(session, tableHandle);
        var definitions = GeneratedColumnMetadataCodec.decode(tableMetadata.properties());
        WriteDeclarationSnapshot declaration = WriteDeclarationSnapshot.capture(
                table, tableMetadata, definitions.toByteArray(),
                definitions.getDefinitionsCount() != 0);
        boolean appendOnly = actions.equals(Set.of(Privilege.INSERT));
        if (!appendOnly && !metadata.supportsCdcMutations()) {
            throw new UnsupportedOperationException("Connector does not support atomic CDC UPDATE/DELETE");
        }
        String writeMode = appendOnly ? "INSERT" : "MERGE";
        WriteAuthorizationReceipt receipt = WriteAuthorityAdmission.resolve(
                new WriteAuthorityRequest(table, session, writeMode, List.of(), actions,
                        declaration, false),
                definitions, resolver);
        ConnectorOutputTableHandle outputHandle = appendOnly
                ? metadata.beginInsert(session, tableHandle)
                : metadata.beginCdcMerge(session, tableHandle);
        WriteOutputBinding binding = ConnectorWriteBindingFactory.create(
                catalogHandle, outputHandle);
        return new CdcWritePlan(outputHandle, binding, receipt,
                new WriteAllocationIntent(writeMode, java.util.Map.of()));
    }

    /** Exact authority and allocation needed to register a CDC write context. */
    public record CdcWritePlan(
            ConnectorOutputTableHandle outputHandle,
            WriteOutputBinding writeBinding,
            WriteAuthorizationReceipt authorizationReceipt,
            WriteAllocationIntent allocationIntent) {
        public CdcWritePlan {
            Objects.requireNonNull(outputHandle, "outputHandle");
            Objects.requireNonNull(writeBinding, "writeBinding");
            Objects.requireNonNull(authorizationReceipt, "authorizationReceipt");
            Objects.requireNonNull(allocationIntent, "allocationIntent");
        }

        /** Registers the exact allocated write context and its CDC source identity. */
        public void register(
                org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker tracker,
                String queryId,
                String catalogName,
                ConnectorSession session,
                org.pangrova.persistence.query.api.commit.CdcSourceIdentity source) {
            tracker.registerWriteContext(queryId, catalogName, outputHandle, session,
                    writeBinding.toByteArray(), authorizationReceipt, allocationIntent,
                    null, source);
        }
    }
}
