package org.pangrova.planner.rewrite;

import org.pangrova.parser.ast.CteQuery;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.JoinClause;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.parser.PangrovaSqlLexer;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pre-parse SQL rewriter that replaces Dynamic Table names with their
 * physical target table ({@code __dt_NAME}).
 *
 * <p>Extracted from {@link PangrovaPlanner} to keep the planner focused on
 * plan generation. Supports chained DT resolution (DT_C → DT_B → source)
 * with a max depth of 10 to prevent infinite loops.</p>
 */
public final class DynamicTableSqlRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableSqlRewriter.class);
    private static final int MAX_CHAIN_DEPTH = 10;

    private final PangrovaParserFactory parserFactory;
    private final DynamicTableRewriter dynamicTableRewriter;

    public DynamicTableSqlRewriter(PangrovaParserFactory parserFactory,
                                   DynamicTableRewriter dynamicTableRewriter) {
        this.parserFactory = parserFactory;
        this.dynamicTableRewriter = dynamicTableRewriter;
    }

    /**
     * Returns the underlying DT rewriter for merge-on-read resolution.
     */
    public DynamicTableRewriter getDynamicTableRewriter() {
        return dynamicTableRewriter;
    }

    /**
     * Extracts table names referenced in the SQL for merge-on-read resolution.
     * Returns simple (unqualified) table names from the AST.
     */
    public List<String> extractTableNames(String sql) {
        try {
            Statement ast = parserFactory.parseToAst(sql);
            List<TableRef> refs = extractTableRefs(ast);
            List<String> names = new ArrayList<>();
            for (TableRef ref : refs) {
                String name = extractSimpleName(ref.name());
                if (name != null) {
                    names.add(name);
                }
            }
            return names;
        } catch (Exception e) {
            LOG.debug("event=dt_extract_table_names_failed reason={}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Rewrites Dynamic Table references in SQL text to physical target tables.
     * Iterates to resolve chained DTs; returns original SQL unchanged if no DTs found.
     */
    public String rewrite(String sql) {
        String rewritten = sql;
        for (int depth = 0; depth < MAX_CHAIN_DEPTH; depth++) {
            String before = rewritten;
            rewritten = singlePassRewrite(rewritten);
            if (rewritten.equals(before)) {
                break;
            }
        }
        return rewritten;
    }

    private String singlePassRewrite(String sql) {
        Statement ast;
        try {
            ast = parserFactory.parseToAst(sql);
        } catch (Exception e) {
            LOG.debug("event=dt_rewrite_ast_fallback reason={}", e.getMessage());
            return fallbackRewrite(sql);
        }
        List<TableRef> tableRefs = extractTableRefs(ast);
        if (tableRefs.isEmpty()) {
            return sql;
        }
        String rewritten = sql;
        for (TableRef ref : tableRefs) {
            rewritten = rewriteTableRef(rewritten, ref);
        }
        return rewritten;
    }

    private String fallbackRewrite(String sql) {
        LOG.warn("event=dt_rewrite_ast_failed action=skip_fallback reason=regex_replacement_unsafe");
        return sql;
    }

    private String rewriteTableRef(String sql, TableRef ref) {
        String tableName = extractSimpleName(ref.name());
        if (tableName == null) {
            return sql;
        }
        Optional<String> target = dynamicTableRewriter.resolveTargetTable(tableName);
        if (target.isPresent()) {
            LOG.info("event=dt_sql_rewrite original={} target={}", tableName, target.get());
            return replaceTableRef(sql, ref.name(), target.get());
        }
        return sql;
    }

    private List<TableRef> extractTableRefs(Statement ast) {
        if (ast instanceof Query query) {
            return collectTableRefs(query);
        }
        if (ast instanceof CteQuery cte) {
            return collectCteTableRefs(cte);
        }
        return List.of();
    }

    private List<TableRef> collectTableRefs(Query query) {
        List<TableRef> refs = new ArrayList<>(query.from());
        for (JoinClause join : query.joins()) {
            refs.add(join.right());
        }
        return refs;
    }

    private List<TableRef> collectCteTableRefs(CteQuery cte) {
        if (cte.body() instanceof Query query) {
            return collectTableRefs(query);
        }
        return List.of();
    }

    private static String extractSimpleName(Identifier id) {
        List<String> parts = id.parts();
        if (parts.isEmpty()) {
            return null;
        }
        return parts.get(parts.size() - 1);
    }

    private static String replaceTableRef(String sql, Identifier id, String newName) {
        List<String> parts = id.parts();
        if (parts.isEmpty()) {
            return sql;
        }
        // The AST deliberately does not carry source spans. Match its table name
        // against lexer tokens at a FROM/JOIN (or comma-separated FROM) position,
        // rather than searching raw SQL: a string, comment, CTE body or projected
        // column may contain the same text before the actual table reference.
        List<? extends Token> tokens = new PangrovaSqlLexer(CharStreams.fromString(sql)).getAllTokens();
        int[] depths = new int[tokens.size()];
        int depth = 0;
        int queryDepth = Integer.MAX_VALUE;
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.getType() == PangrovaSqlLexer.RPAREN) depth--;
            depths[i] = depth;
            if (token.getType() == PangrovaSqlLexer.FROM) {
                queryDepth = Math.min(queryDepth, depth);
            }
            if (token.getType() == PangrovaSqlLexer.LPAREN) depth++;
        }
        boolean inFrom = false;
        for (int i = 0; i < tokens.size(); i++) {
            if (depths[i] != queryDepth) continue;
            int type = tokens.get(i).getType();
            if (type == PangrovaSqlLexer.FROM) {
                inFrom = true;
            } else if (type == PangrovaSqlLexer.WHERE || type == PangrovaSqlLexer.GROUP
                    || type == PangrovaSqlLexer.HAVING || type == PangrovaSqlLexer.ORDER
                    || type == PangrovaSqlLexer.LIMIT || type == PangrovaSqlLexer.OFFSET
                    || type == PangrovaSqlLexer.UNION || type == PangrovaSqlLexer.INTERSECT
                    || type == PangrovaSqlLexer.EXCEPT || type == PangrovaSqlLexer.QUALIFY) {
                inFrom = false;
            }
            if (!inFrom || (type != PangrovaSqlLexer.FROM
                    && type != PangrovaSqlLexer.JOIN && type != PangrovaSqlLexer.COMMA)) {
                continue;
            }
            int last = matchingIdentifier(tokens, depths, i + 1, queryDepth, parts);
            if (last >= 0) {
                return sql.substring(0, tokens.get(i + 1).getStartIndex()) + newName
                        + sql.substring(tokens.get(last).getStopIndex() + 1);
            }
        }
        throw new IllegalStateException("Cannot locate parsed Dynamic Table reference: " + id);
    }

    private static int matchingIdentifier(List<? extends Token> tokens, int[] depths,
                                          int start, int queryDepth, List<String> parts) {
        int index = start;
        for (int part = 0; part < parts.size(); part++) {
            if (part > 0) {
                if (index >= tokens.size() || tokens.get(index).getType() != PangrovaSqlLexer.DOT) {
                    return -1;
                }
                index++;
            }
            if (index >= tokens.size() || depths[index] != queryDepth) return -1;
            Token token = tokens.get(index++);
            String actual = switch (token.getType()) {
                case PangrovaSqlLexer.IDENTIFIER -> token.getText();
                case PangrovaSqlLexer.QUOTED_IDENTIFIER ->
                        token.getText().substring(1, token.getText().length() - 1).replace("\"\"", "\"");
                case PangrovaSqlLexer.BACKTICK_IDENTIFIER ->
                        token.getText().substring(1, token.getText().length() - 1).replace("``", "`");
                default -> null;
            };
            if (actual == null || !actual.equalsIgnoreCase(parts.get(part))) return -1;
        }
        return index - 1;
    }
}
