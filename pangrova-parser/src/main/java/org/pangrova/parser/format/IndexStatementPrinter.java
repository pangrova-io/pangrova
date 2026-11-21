package org.pangrova.parser.format;

import org.pangrova.parser.api.ast.statement.AlterIndexStatement;
import org.pangrova.parser.api.ast.statement.CreateIndexStatement;
import org.pangrova.parser.api.ast.statement.DropIndexStatement;

import java.util.stream.Collectors;

/** SQL text for index lifecycle statements. */
final class IndexStatementPrinter {

    private final AstPrinter printer;

    IndexStatementPrinter(AstPrinter printer) {
        this.printer = printer;
    }

    String printCreateIndex(CreateIndexStatement statement) {
        var sql = new StringBuilder("CREATE INDEX ");
        if (statement.ifNotExists()) {
            sql.append("IF NOT EXISTS ");
        }
        sql.append(printer.quote(statement.indexName()));
        sql.append(" ON ").append(printer.printId(statement.tableName()));
        sql.append(" (");
        sql.append(statement.columnNames().stream()
                .map(printer::quote).collect(Collectors.joining(", ")));
        sql.append(") USING ").append(statement.indexType());
        if (!statement.properties().isEmpty()) {
            sql.append(" WITH (");
            sql.append(statement.properties().entrySet().stream()
                    .map(entry -> "'" + entry.getKey().replace("'", "''") + "' = '"
                            + entry.getValue().replace("'", "''") + "'")
                    .collect(Collectors.joining(", ")));
            sql.append(")");
        }
        return sql.toString();
    }

    String printDropIndex(DropIndexStatement statement) {
        var sql = new StringBuilder("DROP INDEX ");
        if (statement.ifExists()) {
            sql.append("IF EXISTS ");
        }
        sql.append(printer.quote(statement.indexName()));
        sql.append(" ON ").append(printer.printId(statement.tableName()));
        return sql.toString();
    }

    String printAlterIndex(AlterIndexStatement statement) {
        return "ALTER INDEX " + printer.quote(statement.indexName())
                + " ON " + printer.printId(statement.tableName()) + " REBUILD";
    }
}
