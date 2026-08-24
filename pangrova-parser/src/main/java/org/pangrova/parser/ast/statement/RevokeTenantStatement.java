package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents REVOKE TENANT tenantName FROM principal.
 */
public record RevokeTenantStatement(Identifier tenantName, Identifier principal) implements Statement {}
