package org.pangrova.coordinator.query.execution;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.coordination.WriteAuthorizationReceipt;
import org.pangrova.connector.spi.coordination.WriteDeclarationSnapshot;

import java.util.Objects;
import java.util.List;
import java.util.Set;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.policy.Privilege;

/** Resolves durable generated-write authority before allocating a connector write. */
@Internal
@FunctionalInterface
public interface GeneratedWriteAuthoritySupplier {
    /** Returns one current authority and declaration receipt, including ordinary writes. */
    WriteAuthorizationReceipt resolve(WriteAuthorityRequest request);

    /** Authenticated write intent; contains no source rows or bearer credentials. */
    record WriteAuthorityRequest(
            QualifiedTableName table, ConnectorSession session, String writeMode,
            List<TablePermission> observedReadPermissions, Set<Privilege> requiredWritePrivileges,
            WriteDeclarationSnapshot compiledDeclaration, boolean internalMaterializationExecution) {
        public WriteAuthorityRequest {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(session, "session");
            Objects.requireNonNull(writeMode, "writeMode");
            Objects.requireNonNull(compiledDeclaration, "compiledDeclaration");
            if (!compiledDeclaration.table().equals(table)) {
                throw new IllegalArgumentException("Compiled write declaration must identify the requested table");
            }
            observedReadPermissions = List.copyOf(observedReadPermissions);
            requiredWritePrivileges = Set.copyOf(requiredWritePrivileges);
            if (writeMode.isBlank() || session.getUser() == null || session.getUser().isBlank()) {
                throw new IllegalArgumentException(
                        "Generated write authority requires a real principal and write mode");
            }
            if (requiredWritePrivileges.isEmpty() || !Set.of(Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE)
                    .containsAll(requiredWritePrivileges)) {
                throw new IllegalArgumentException("Original write actions must be explicit");
            }
            if (observedReadPermissions.stream().anyMatch(permission -> permission.privilege() != Privilege.SELECT
                    || permission.readMode() != org.pangrova.security.api.authz.DeferredAuthorizationScope
                            .ReadMode.SECURITY_TRANSFORMED)) {
                throw new IllegalArgumentException("Write reads require actual protected compilation observations");
            }
        }
    }
}
