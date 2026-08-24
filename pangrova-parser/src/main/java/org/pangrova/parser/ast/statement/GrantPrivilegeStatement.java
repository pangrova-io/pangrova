package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

import java.util.Objects;

/**
 * Represents {@code GRANT privilege ON [catalog.]schema.table TO role [WITH GRANT OPTION]}.
 *
 * <p>Part of the RBAC DDL surface (G-SEC-03 fix). The coordinator's
 * security statement handler routes this to {@link org.pangrova.security.authz.GrantStore#addGrant}
 * and publishes a {@link org.pangrova.security.event.SecurityPolicyChangeEvent.GrantChanged}.
 *
 * @param privilege       privilege keyword (SELECT, INSERT, ALL, etc.)
 * @param objectType      TABLE, SCHEMA, or CATALOG
 * @param objectName      fully qualified object reference
 * @param roleName        grantee role
 * @param withGrantOption if true, the grantee can further grant this privilege
 */
public record GrantPrivilegeStatement(
        String privilege,
        String objectType,
        Identifier objectName,
        Identifier roleName,
        boolean withGrantOption
) implements Statement {

    public GrantPrivilegeStatement {
        Objects.requireNonNull(privilege, "privilege must not be null");
        Objects.requireNonNull(objectType, "objectType must not be null");
        Objects.requireNonNull(objectName, "objectName must not be null");
        Objects.requireNonNull(roleName, "roleName must not be null");
    }
}
