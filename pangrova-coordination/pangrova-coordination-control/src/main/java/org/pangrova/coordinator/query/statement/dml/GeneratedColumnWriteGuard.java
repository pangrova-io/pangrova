package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.identity.UserIdentity;

import java.util.HashMap;

/** Fail-closed gate used until generated expressions are present in the Native write plan. */
public final class GeneratedColumnWriteGuard {

    private GeneratedColumnWriteGuard() {}

    /**
     * Rejects a write that would otherwise commit source values without
     * materializing or durably scheduling its generated columns.
     */
    public static void requireNativeMaterializationPlan(
            ConnectorMetadata metadata,
            ConnectorSession session,
            ConnectorTableHandle table,
            QualifiedTableName tableName,
            String operation,
            org.pangrova.coordinator.ai.embedding.GeneratedValueMaterializationCoordinator
                    materializationCoordinator,
            UserIdentity user) {
        var definitions = GeneratedColumnMetadataCodec.decode(
                metadata.getTableMetadata(session, table).properties());
        if (definitions.getDefinitionsCount() == 0) {
            return;
        }
        String tenantId = session.getProperties().get("tenant_id");
        if (user == null || user.principal() == null || user.principal().isBlank()
                || tenantId == null || tenantId.isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    operation + " requires an authenticated user and resolved tenant "
                            + "for generated-column materialization");
        }
        if (materializationCoordinator == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    operation + " targets generated columns, but the durable Native "
                            + "materialization control plane is not installed");
        }
        var attributes = new HashMap<>(user.attributes());
        attributes.remove("tenantId");
        attributes.put("tenant_id", tenantId);
        materializationCoordinator.requireWriteAdmission(
                tableName, definitions,
                new UserIdentity(user.principal(), user.groups(), attributes,
                        user.authenticatedAt()), operation);
    }
}
