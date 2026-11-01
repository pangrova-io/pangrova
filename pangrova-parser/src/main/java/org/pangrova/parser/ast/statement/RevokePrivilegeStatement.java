package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

import java.util.Objects;

/**
 * Represents {@code REVOKE privilege ON [catalog.]schema.table FROM role [CASCADE]}.
 *
 * <p>Part of the RBAC DDL surface (G-SEC-03 fix). The coordinator's
 * security statement handler routes this to
 * {@link org.pangrova.persistence.api.security.policy.GrantStore#revokeGrants} and publishes a
 * {@link org.pangrova.spi.security.event.SecurityPolicyChangeEvent.GrantChanged}.
 *
 * @param privilege  privilege keyword (SELECT, INSERT, ALL, etc.)
 * @param objectType TABLE, SCHEMA, or CATALOG
 * @param objectName fully qualified object reference
 * @param roleName   role to revoke from
 * @param cascade    if true, cascades to dependent grants
 */
public record RevokePrivilegeStatement(
        String privilege,
        String objectType,
        Identifier objectName,
        Identifier roleName,
        boolean cascade
) implements Statement {

    public RevokePrivilegeStatement {
        Objects.requireNonNull(privilege, "privilege must not be null");
        Objects.requireNonNull(objectType, "objectType must not be null");
        Objects.requireNonNull(objectName, "objectName must not be null");
        Objects.requireNonNull(roleName, "roleName must not be null");
    }

    @Override
    public StatementType type() {
        return StatementType.REVOKE_PRIVILEGE;
    }
}
