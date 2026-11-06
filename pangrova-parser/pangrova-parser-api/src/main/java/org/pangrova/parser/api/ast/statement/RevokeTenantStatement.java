package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents REVOKE TENANT tenantName FROM principal.
 */
public record RevokeTenantStatement(Identifier tenantName, Identifier principal) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.REVOKE_TENANT;
    }
}
