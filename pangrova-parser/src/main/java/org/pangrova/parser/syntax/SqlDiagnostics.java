package org.pangrova.parser.syntax;

import org.antlr.v4.runtime.CharStreams;
import org.pangrova.parser.build.antlr.PangrovaSqlLexer;

/** SQL suitable for reporting externally, where arbitrary connector properties may contain credentials. */
public final class SqlDiagnostics {
    private SqlDiagnostics() {}

    /** Redacts {@code SET} and catalog DDL statements, whose properties may carry credentials. */
    public static String forReporting(String sql) {
        if (sql == null) return null;
        var lexer = new PangrovaSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        int action = lexer.nextToken().getType();
        if (action == PangrovaSqlLexer.SET) {
            return "SET [redacted]";
        }
        if ((action == PangrovaSqlLexer.CREATE || action == PangrovaSqlLexer.ALTER)
                && lexer.nextToken().getType() == PangrovaSqlLexer.CATALOG) {
            return (action == PangrovaSqlLexer.CREATE ? "CREATE" : "ALTER") + " CATALOG [redacted]";
        }
        return sql;
    }
}
