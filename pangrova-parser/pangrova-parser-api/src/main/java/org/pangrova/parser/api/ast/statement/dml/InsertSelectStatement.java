package org.pangrova.parser.api.ast.statement.dml;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.SetQuery;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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
