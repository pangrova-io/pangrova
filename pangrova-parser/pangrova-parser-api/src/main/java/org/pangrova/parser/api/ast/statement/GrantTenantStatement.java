package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents GRANT TENANT tenantName TO principal.
 */
public record GrantTenantStatement(Identifier tenantName, Identifier principal) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.GRANT_TENANT;
    }
}
