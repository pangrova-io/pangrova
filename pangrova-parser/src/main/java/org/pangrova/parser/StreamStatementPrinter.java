package org.pangrova.parser;

import org.pangrova.parser.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Prints Stream DDL statements (CREATE/ALTER/DROP STREAM) as SQL text.
 */
final class StreamStatementPrinter {

    private final AstPrinter printer;

    StreamStatementPrinter(AstPrinter printer) {
        this.printer = printer;
    }

    String printCreateStream(CreateStreamStatement s) {
        var sb = new StringBuilder("CREATE STREAM ");
        if (s.ifNotExists()) sb.append("IF NOT EXISTS ");
        sb.append(printer.printId(s.name()));
        sb.append(" FROM KAFKA (");
        sb.append(printStringProps(s.sourceProperties()));
        sb.append(") FORMAT '").append(s.sourceFormat().replace("'", "''")).append("'");
        sb.append(" INTO ").append(printer.printId(s.targetTable()));
        if (!s.withProperties().isEmpty()) {
            sb.append(" WITH (").append(printStringProps(s.withProperties())).append(")");
        }
        return sb.toString();
    }

    String printAlterStream(AlterStreamStatement s) {
        var sb = new StringBuilder("ALTER STREAM ");
        sb.append(printer.printId(s.name()));
        sb.append(' ').append(printAlterStreamAction(s.action()));
        return sb.toString();
    }

    String printDropStream(DropStreamStatement s) {
        var sb = new StringBuilder("DROP STREAM ");
        if (s.ifExists()) sb.append("IF EXISTS ");
        sb.append(printer.printId(s.name()));
        return sb.toString();
    }

    private String printAlterStreamAction(AlterStreamAction action) {
        if (action instanceof AlterStreamAction.Suspend) return "SUSPEND";
        if (action instanceof AlterStreamAction.Resume) return "RESUME";
        if (action instanceof AlterStreamAction.SetProperties sp) {
            return "SET (" + printStringProps(sp.properties()) + ")";
        }
        throw new IllegalArgumentException("Unknown alter stream action: " + action.getClass());
    }

    String printStringProps(Map<String, String> props) {
        return props.entrySet().stream()
                .map(e -> "'" + e.getKey().replace("'", "''") + "' = '"
                        + e.getValue().replace("'", "''") + "'")
                .collect(Collectors.joining(", "));
    }
}
