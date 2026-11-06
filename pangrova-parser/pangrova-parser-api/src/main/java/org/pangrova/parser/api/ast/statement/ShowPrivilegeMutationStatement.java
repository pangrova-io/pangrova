package org.pangrova.parser.api.ast.statement;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

import java.util.Objects;
import java.util.UUID;

/** Confirmation of the authenticated submitter's own durable privilege mutation receipt. */
@Experimental
public record ShowPrivilegeMutationStatement(UUID requestId) implements Statement {
    public ShowPrivilegeMutationStatement { Objects.requireNonNull(requestId, "requestId"); }
    @Override
    public StatementType type() { return StatementType.SHOW_PRIVILEGE_MUTATION; }
}
