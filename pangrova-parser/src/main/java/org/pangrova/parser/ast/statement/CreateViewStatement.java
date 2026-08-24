package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

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
}
