package org.pangrova.parser.ast.statement.dml;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.SetQuery;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents INSERT INTO/OVERWRITE table [(columns)] SELECT ...
 */
public record InsertSelectStatement(
        Identifier tableName,
        Optional<List<String>> columns,
        boolean overwrite,
        SetQuery selectQuery) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.INSERT_SELECT;
    }
}
