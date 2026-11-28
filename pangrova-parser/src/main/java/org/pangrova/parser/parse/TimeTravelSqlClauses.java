package org.pangrova.parser.parse;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.enums.SqlDialect;
import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.build.AstBuilder;
import org.pangrova.parser.build.antlr.PangrovaSqlParser;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/** Source references and SQL spans captured while constructing the statement AST. */
public final class TimeTravelSqlClauses {
    private static final int MIN_ENTRY_WEIGHT = 4096;
    private static final long SOURCE_CACHE_MAX_WEIGHT = 4L * 1024 * 1024;
    private static final AntlrSqlParser PARSER = new AntlrSqlParser();

    // Source offsets depend on the full SQL spelling, including whitespace and comments.
    // Weight is an SQL-length estimate, not an exact heap byte count.
    private static final Cache<String, Analysis> SOURCES = Caffeine.newBuilder()
                    .maximumWeight(SOURCE_CACHE_MAX_WEIGHT)
                    .weigher((String sql, Analysis ignored) -> Math.max(MIN_ENTRY_WEIGHT, sql.length()))
                    .build();

    private TimeTravelSqlClauses() {}

    /** One named relation and the source span of its qualified name, in code points. */
    public record Reference(TableRef table, boolean cte, int nameStart, int nameEnd) {}

    /** One statement and its exact source information. */
    public record Analysis(String sql, Statement statement, String strippedSql, List<Reference> references) {
        public Analysis {
            references = List.copyOf(references);
        }

        /** Replaces named relation tokens and parses the new input only when changed. */
        public Analysis rewriteNames(Function<Reference, String> replacement) {
            StringBuilder rewritten = null;
            int cursor = 0;
            int cursorCodePoints = 0;
            for (Reference reference : references) {
                String name = replacement.apply(reference);
                if (name == null) {
                    continue;
                }
                if (rewritten == null) {
                    rewritten = new StringBuilder(sql.length());
                }
                int start = sql.offsetByCodePoints(cursor, reference.nameStart() - cursorCodePoints);
                int end = sql.offsetByCodePoints(start, reference.nameEnd() - reference.nameStart());
                rewritten.append(sql, cursor, start).append(name);
                cursor = end;
                cursorCodePoints = reference.nameEnd();
            }
            return rewritten == null ? this : extract(rewritten.append(sql, cursor, sql.length()).toString());
        }
    }

    /** Strictly parses the built-in grammar once, retaining the original AST and source spans. */
    public static Analysis extract(String sql) {
        if (sql == null || sql.length() >= AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH) {
            return PARSER.analyzeSources(sql);
        }
        return SOURCES.get(sql, PARSER::analyzeSources);
    }

    static Analysis collect(String sql, CommonTokenStream tokens,
            PangrovaSqlParser.SingleStatementContext tree, SqlDialect dialect) {
        var builder = new SourceBuilder(tokens, dialect);
        var statement = builder.visitSingleStatement(tree);
        builder.references.sort(Comparator.comparingInt(Reference::nameStart));
        return new Analysis(sql, statement, strip(sql, builder.spans), builder.references);
    }

    private static final class SourceBuilder extends AstBuilder {
        private final List<Reference> references = new ArrayList<>();
        private final List<Span> spans = new ArrayList<>();
        private final CommonTokenStream tokens;
        private final SqlDialect dialect;

        private SourceBuilder(CommonTokenStream tokens, SqlDialect dialect) {
            super(tokens, dialect);
            this.tokens = tokens;
            this.dialect = dialect;
        }

        @Override
        public TableRef visitNamedTableRef(PangrovaSqlParser.NamedTableRefContext named) {
            var table = super.visitNamedTableRef(named);
            recordReference(named, table);
            return table;
        }

        @Override
        public TableRef visitPivotTableRef(PangrovaSqlParser.PivotTableRefContext pivot) {
            var table = super.visitPivotTableRef(pivot);
            retainDecoratedIdentity(pivot.tableRef(), table);
            return table;
        }

        @Override
        public TableRef visitUnpivotTableRef(PangrovaSqlParser.UnpivotTableRefContext unpivot) {
            var table = super.visitUnpivotTableRef(unpivot);
            retainDecoratedIdentity(unpivot.tableRef(), table);
            return table;
        }

        private void retainDecoratedIdentity(PangrovaSqlParser.TableRefContext source, TableRef table) {
            while (true) {
                if (source instanceof PangrovaSqlParser.NamedTableRefContext named) {
                    replaceDecoratedReference(named.qualifiedName().getStart().getStartIndex(), table);
                    return;
                }
                if (source instanceof PangrovaSqlParser.PivotTableRefContext pivot) {
                    source = pivot.tableRef();
                } else if (source instanceof PangrovaSqlParser.UnpivotTableRefContext unpivot) {
                    source = unpivot.tableRef();
                } else {
                    return;
                }
            }
        }

        private void replaceDecoratedReference(int start, TableRef table) {
            // Only a decorator wraps an existing relation in a new TableRef. Nested
            // subqueries in its expressions can contribute named references of their own.
            for (int i = references.size() - 1; i >= 0; i--) {
                Reference reference = references.get(i);
                if (reference.nameStart() == start) {
                    references.set(i, new Reference(table, reference.cte(), start, reference.nameEnd()));
                    return;
                }
            }
        }

        @Override
        public Statement visitCreateViewStatement(PangrovaSqlParser.CreateViewStatementContext context) {
            Statement statement = super.visitCreateViewStatement(context);
            // The view statement retains its body as SQL, so the normal AST visitor skips it.
            var bodyBuilder = new AstBuilder(tokens, dialect);
            var pending = new ArrayDeque<ParseTree>();
            pending.push(context.createView().setQuery());
            while (!pending.isEmpty()) {
                ParseTree node = pending.pop();
                if (node instanceof PangrovaSqlParser.NamedTableRefContext named) {
                    recordReference(named, bodyBuilder.visitNamedTableRef(named));
                }
                for (int i = node.getChildCount() - 1; i >= 0; i--) {
                    pending.push(node.getChild(i));
                }
            }
            return statement;
        }

        private void recordReference(PangrovaSqlParser.NamedTableRefContext named, TableRef table) {
            var name = named.qualifiedName();
            references.add(new Reference(table, isCteReference(named, table, this),
                    name.getStart().getStartIndex(), name.getStop().getStopIndex() + 1));
            var clause = named.timeTravelSpec();
            if (clause != null) {
                spans.add(new Span(clause.getStart().getStartIndex(), clause.getStop().getStopIndex() + 1));
            }
        }
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
