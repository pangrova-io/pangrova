package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents CREATE TABLE [IF NOT EXISTS] name AS query (CTAS).
 *
 * <p>Creates a new table whose schema is derived from the query result
 * and populates it with the query output.</p>
 */
public record CreateTableAsSelectStatement(
        Identifier tableName,
        boolean ifNotExists,
        Statement query) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CREATE_TABLE_AS_SELECT;
    }
}
