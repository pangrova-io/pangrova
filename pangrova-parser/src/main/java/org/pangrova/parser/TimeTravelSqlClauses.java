package org.pangrova.parser;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.ast.TableRef;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Locates source clauses and named references using the existing SQL grammar. */
public final class TimeTravelSqlClauses {
    private TimeTravelSqlClauses() {}

    /** A named reference and whether its name resolves to a visible logical CTE. */
    public record Reference(TableRef table, boolean cte) {}

    /** Exact original SQL outside time-travel spans and its named references. */
    public record Analysis(String strippedSql, List<Reference> references) {
        public Analysis {
            references = List.copyOf(references);
        }
    }

    /** Strictly parses the same grammar; malformed SQL is never returned as current SQL. */
    public static Analysis extract(String sql) {
        if (sql == null || sql.length() > SqlParser.DEFAULT_MAX_SQL_LENGTH) {
            throw invalid("SQL text exceeds the parser input limit");
        }
        var lexer = new PangrovaSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);
        var tokens = new CommonTokenStream(lexer);
        var parser = new PangrovaSqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);
        parser.setErrorHandler(new BailErrorStrategy());
        try {
            var tree = parser.singleStatement();
            if (SqlParser.measureDepth(tree) > SqlParser.DEFAULT_MAX_TREE_DEPTH) {
                throw invalid("SQL parse tree exceeds the parser depth limit");
            }
            return collect(sql, tree, new AstBuilder(tokens));
        } catch (ParseCancellationException | StackOverflowError failure) {
            throw new ParseException(PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Cannot locate time-travel clauses in invalid SQL", null, failure);
        }
    }

    private static Analysis collect(String sql, ParseTree tree, AstBuilder builder) {
        var pending = new ArrayDeque<ParseTree>();
        var references = new ArrayList<Reference>();
        var spans = new ArrayList<Span>();
        pending.push(tree);
        while (!pending.isEmpty()) {
            var node = pending.pop();
            if (node instanceof PangrovaSqlParser.NamedTableRefContext named) {
                var table = builder.visitNamedTableRef(named);
                references.add(new Reference(table, isCteReference(named, table, builder)));
            }
            if (node instanceof PangrovaSqlParser.TimeTravelSpecContext clause) {
                spans.add(new Span(clause.getStart().getStartIndex(), clause.getStop().getStopIndex() + 1));
            }
            for (int i = node.getChildCount() - 1; i >= 0; --i) {
                pending.push(node.getChild(i));
            }
        }
        return new Analysis(strip(sql, spans), references);
    }

    private static boolean isCteReference(PangrovaSqlParser.NamedTableRefContext named,
                                          TableRef table, AstBuilder builder) {
        if (table.name().parts().size() != 1) {
            return false;
        }
        String name = table.name().parts().get(0);
        ParseTree child = named;
        for (ParseTree parent = child.getParent(); parent != null; parent = parent.getParent()) {
            if (parent instanceof PangrovaSqlParser.CteQueryContext cte) {
                for (var definition : cte.cteDefinition()) {
                    if (definition == child && cte.RECURSIVE() == null) {
                        break;
                    }
                    // Match the current planner's case-insensitive identifier resolution,
                    // while retaining the source and AST spelling (including quoted names).
                    if (builder.extractIdentifierText(definition.identifier()).equalsIgnoreCase(name)) {
                        return true;
                    }
                }
            }
            child = parent;
        }
        return false;
    }

    private static String strip(String sql, List<Span> spans) {
        if (spans.isEmpty()) {
            return sql;
        }
        // CharStreams.fromString uses code-point token indices, not Java UTF-16 offsets.
        int[] offsets = new int[sql.codePointCount(0, sql.length()) + 1];
        for (int i = 0, offset = 0; i < offsets.length; ++i) {
            offsets[i] = offset;
            if (offset < sql.length()) {
                offset += Character.charCount(sql.codePointAt(offset));
            }
        }
        spans.sort(Comparator.comparingInt(Span::start));
        var stripped = new StringBuilder(sql.length());
        int cursor = 0;
        for (Span span : spans) {
            if (span.start() < cursor || span.end() >= offsets.length) {
                throw invalid("Overlapping or invalid time-travel source spans");
            }
            stripped.append(sql, offsets[cursor], offsets[span.start()]);
            cursor = span.end();
        }
        return stripped.append(sql, offsets[cursor], sql.length()).toString();
    }

    private static ParseException invalid(String message) {
        return new ParseException(PangrovaErrorCode.PARSER_INPUT_TOO_LARGE, message, 0, 0);
    }

    private record Span(int start, int end) {}
}
