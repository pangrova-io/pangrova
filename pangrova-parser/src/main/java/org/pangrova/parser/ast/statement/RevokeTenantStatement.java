package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents REVOKE TENANT tenantName FROM principal.
 */
public record RevokeTenantStatement(Identifier tenantName, Identifier principal) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.REVOKE_TENANT;
    }
}
