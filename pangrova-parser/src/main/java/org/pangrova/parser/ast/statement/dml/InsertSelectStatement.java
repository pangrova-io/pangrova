package org.pangrova.parser.ast.statement.dml;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.SetQuery;
import org.pangrova.parser.ast.Statement;

/**
 * Represents INSERT INTO/OVERWRITE table [(columns)] SELECT ...
 */
public record InsertSelectStatement(
        Identifier tableName,
        Optional<List<String>> columns,
        boolean overwrite,
        SetQuery selectQuery) implements Statement {}
