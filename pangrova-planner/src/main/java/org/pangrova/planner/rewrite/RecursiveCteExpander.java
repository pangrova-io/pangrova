package org.pangrova.planner.rewrite;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.parser.api.ast.CteQuery;
import org.pangrova.parser.api.ast.SetQuery;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.SetOperatorType;
import org.pangrova.parser.format.AstPrinter;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.planner.parse.PangrovaParserFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Turns a top-level {@code WITH RECURSIVE} fixed point into levels the native
 * engine can execute.
 *
 * <p>Substrait has no recursive relation operator, so the planner resolves the
 * fixed point instead: the seed executes once, the recursive term executes once
 * per level against the previous level only (the SQL working table), and the CTE
 * itself becomes the UNION ALL of every level. Each level is a plain query that
 * follows the normal Substrait/native path.</p>
 *
 * <p>Recursion is bounded by {@link #MAX_ITERATIONS} levels. Terminating
 * queries converge earlier (later levels are empty); a query that would need
 * more iterations is limited support and is documented as such rather than
 * silently switched to a different execution model.</p>
 */
public final class RecursiveCteExpander {

    /**
     * Seed plus recursive applications available to a recursive CTE. Each
     * level repeats the recursive term (including any table reads), so the
     * budget keeps planning and authorization within the submit deadline.
     */
    public static final int MAX_ITERATIONS = 16;

    private static final String LEVEL_PREFIX = "__pangrova_rc_";

    private final PangrovaParserFactory parserFactory;
    private final AstPrinter printer = new AstPrinter();
    private final int maxIterations;

    public RecursiveCteExpander(PangrovaParserFactory parserFactory) {
        this(parserFactory, MAX_ITERATIONS);
    }

    RecursiveCteExpander(PangrovaParserFactory parserFactory, int maxIterations) {
        if (maxIterations < 2) {
            throw new IllegalArgumentException("maxIterations must be at least 2");
        }
        this.parserFactory = parserFactory;
        this.maxIterations = maxIterations;
    }

    /**
     * Expands a recursive CTE into executable levels; other statements are returned unchanged.
     */
    public String expand(String sql) {
        if (!containsRecursiveKeyword(sql)) {
            return sql;
        }
        Statement statement = parserFactory.parseToAst(sql);
        if (!(statement instanceof CteQuery cte) || !cte.recursive()) {
            return sql;
        }
        return expand(cte);
    }

    /** Reuses a parsed statement and parses only the generated recursive levels. */
    public Statement expand(Statement statement) {
        if (statement instanceof CteQuery cte && cte.recursive()) {
            return parserFactory.parseToAst(expand(cte));
        }
        return statement;
    }

    private static boolean containsRecursiveKeyword(String sql) {
        return sql.toUpperCase(Locale.ROOT).contains("RECURSIVE");
    }

    private String expand(CteQuery cte) {
        int recursiveIndex = findRecursiveDefinition(cte);
        if (recursiveIndex < 0) {
            return printWith(cte, -1, null, List.of());
        }
        CteQuery.CteDefinition definition = cte.ctes().get(recursiveIndex);
        requireSupportedShape(definition);
        SetQuery union = (SetQuery) definition.query();
        String seed = printer.print(union.queries().get(0));
        String step = printer.print(union.queries().get(1));
        List<String> levels = buildLevels(definition, seed, step);
        return printWith(cte, recursiveIndex, definition, levels);
    }

    private int findRecursiveDefinition(CteQuery cte) {
        int found = -1;
        for (int index = 0; index < cte.ctes().size(); index++) {
            CteQuery.CteDefinition definition = cte.ctes().get(index);
            String printed = printer.print(definition.query());
            if (containsIdentifier(printed, definition.name())) {
                if (found >= 0) {
                    throw unsupported("only one recursive CTE per statement is supported");
                }
                found = index;
            }
        }
        return found;
    }

    private static void requireSupportedShape(CteQuery.CteDefinition definition) {
        if (definition.searchClause().isPresent() || definition.cycleClause().isPresent()) {
            throw unsupported(
                    "SEARCH and CYCLE clauses have no native execution operator for CTE '"
                            + definition.name() + "'");
        }
        if (!(definition.query() instanceof SetQuery setQuery)
                || setQuery.queries().size() != 2
                || setQuery.operators().size() != 1) {
            throw unsupported("recursive CTE '" + definition.name()
                    + "' needs a seed and one recursive term joined by UNION ALL");
        }
        if (setQuery.operators().get(0) != SetOperatorType.UNION_ALL) {
            throw unsupported("recursive CTE '" + definition.name()
                    + "' requires UNION ALL; UNION DISTINCT recursion changes the working table");
        }
    }

    private List<String> buildLevels(CteQuery.CteDefinition definition,
                                     String seed,
                                     String step) {
        List<String> levels = new ArrayList<>(maxIterations);
        levels.add(seed);
        for (int level = 1; level < maxIterations; level++) {
            levels.add(replaceIdentifier(step, definition.name(), levelName(level - 1)));
        }
        return levels;
    }

    private String printWith(CteQuery cte,
                             int recursiveIndex,
                             CteQuery.CteDefinition recursive,
                             List<String> levels) {
        StringBuilder sql = new StringBuilder("WITH ");
        boolean first = true;
        for (int index = 0; index < cte.ctes().size(); index++) {
            CteQuery.CteDefinition definition = cte.ctes().get(index);
            if (index == recursiveIndex) {
                first = appendLevels(sql, first, recursive, levels);
                continue;
            }
            first = appendDefinition(sql, first, definition.name(),
                    definition.columnList(), printer.print(definition.query()));
        }
        sql.append(' ').append(printer.print(cte.body()));
        return sql.toString();
    }

    private boolean appendLevels(StringBuilder sql,
                                 boolean first,
                                 CteQuery.CteDefinition definition,
                                 List<String> levels) {
        if (definition == null || levels.isEmpty()) {
            return first;
        }
        List<String> columns = definition.columnList();
        boolean next = first;
        for (int level = 0; level < levels.size(); level++) {
            next = appendDefinition(sql, next, levelName(level), columns, levels.get(level));
        }
        StringBuilder combined = new StringBuilder("SELECT * FROM ").append(levelName(0));
        for (int level = 1; level < levels.size(); level++) {
            combined.append(" UNION ALL SELECT * FROM ").append(levelName(level));
        }
        return appendDefinition(sql, next, definition.name(), columns, combined.toString());
    }

    private boolean appendDefinition(StringBuilder sql,
                                     boolean first,
                                     String name,
                                     List<String> columns,
                                     String query) {
        if (!first) {
            sql.append(", ");
        }
        sql.append(quote(name));
        if (!columns.isEmpty()) {
            sql.append(" (");
            for (int index = 0; index < columns.size(); index++) {
                if (index > 0) {
                    sql.append(", ");
                }
                sql.append(quote(columns.get(index)));
            }
            sql.append(')');
        }
        sql.append(" AS (").append(query).append(')');
        return false;
    }

    private static String levelName(int level) {
        return LEVEL_PREFIX + level;
    }

    private static PlannerException unsupported(String detail) {
        return new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "Unsupported recursive CTE: " + detail);
    }

    private static boolean containsIdentifier(String sql, String name) {
        return !replaceIdentifier(sql, name, "\u0000").equals(sql);
    }

    /**
     * Replaces standalone references to a CTE name while leaving string literals untouched.
     */
    static String replaceIdentifier(String sql, String name, String replacement) {
        String quotedName = "\"" + name.replace("\"", "\"\"") + "\"";
        StringBuilder out = new StringBuilder(sql.length());
        int index = 0;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            if (current == '\'') {
                int end = closingQuote(sql, index, '\'');
                out.append(sql, index, end);
                index = end;
                continue;
            }
            if (current == '"') {
                int end = closingQuote(sql, index, '"');
                String token = sql.substring(index, end);
                out.append(token.equals(quotedName) ? quote(replacement) : token);
                index = end;
                continue;
            }
            if (isIdentifierStart(current)) {
                int end = index + 1;
                while (end < sql.length() && isIdentifierPart(sql.charAt(end))) {
                    end++;
                }
                String token = sql.substring(index, end);
                out.append(token.equalsIgnoreCase(name) ? replacement : token);
                index = end;
                continue;
            }
            out.append(current);
            index++;
        }
        return out.toString();
    }

    private static int closingQuote(String sql, int start, char quote) {
        int index = start + 1;
        while (index < sql.length()) {
            if (sql.charAt(index) == quote) {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                    index += 2;
                    continue;
                }
                return index + 1;
            }
            index++;
        }
        return sql.length();
    }

    private static boolean isIdentifierStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    private static boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_' || value == '$';
    }

    private static String quote(String name) {
        boolean simple = !name.isEmpty()
                && (Character.isLetter(name.charAt(0)) || name.charAt(0) == '_')
                && name.chars().allMatch(ch -> Character.isLetterOrDigit(ch)
                        || ch == '_' || ch == '$');
        return simple ? name : "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
