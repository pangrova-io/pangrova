package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents CREATE [OR REPLACE] VIEW [IF NOT EXISTS] name AS query.
 */
public record CreateViewStatement(
        Identifier viewName,
        boolean ifNotExists,
        boolean orReplace,
        String viewSql) implements Statement {

    /** Backward-compatible constructor without orReplace. */
    public CreateViewStatement(Identifier viewName, boolean ifNotExists, String viewSql) {
        this(viewName, ifNotExists, false, viewSql);
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_VIEW;
    }
}
