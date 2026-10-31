package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents GRANT TENANT tenantName TO principal.
 */
public record GrantTenantStatement(Identifier tenantName, Identifier principal) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.GRANT_TENANT;
    }
}
