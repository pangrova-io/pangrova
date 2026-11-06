package org.pangrova.query.api.write;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

import java.util.Objects;
import java.util.List;
import java.util.Set;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.policy.Privilege;

/** Authenticated write intent; contains no source rows or bearer credentials. */
@Internal
public record WriteAuthorityRequest(
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
                    "Write authority requires a real principal and write mode");
        }
        if (requiredWritePrivileges.isEmpty() || !Set.of(Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE)
                .containsAll(requiredWritePrivileges)) {
            throw new IllegalArgumentException("Original write actions must be explicit");
        }
        if (observedReadPermissions.stream().anyMatch(permission -> permission.privilege() != Privilege.SELECT
                || permission.readMode() != org.pangrova.security.api.authorization.DeferredAuthorizationScope
                        .ReadMode.SECURITY_TRANSFORMED)) {
            throw new IllegalArgumentException("Write reads require actual protected compilation observations");
        }
    }
}
