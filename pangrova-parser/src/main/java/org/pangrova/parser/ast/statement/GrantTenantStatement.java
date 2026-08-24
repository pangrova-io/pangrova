package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents GRANT TENANT tenantName TO principal.
 */
public record GrantTenantStatement(Identifier tenantName, Identifier principal) implements Statement {}
