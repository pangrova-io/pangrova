package org.pangrova.planner.rewrite;

import org.pangrova.parser.ast.CteQuery;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.JoinClause;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        StringBuilder patternBuilder = new StringBuilder("(?i)");
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                patternBuilder.append("\\s*\\.\\s*");
            }
            String part = Pattern.quote(parts.get(i));
            patternBuilder.append("(?:\"").append(part).append("\"|").append(part).append(")");
        }
        return sql.replaceFirst(patternBuilder.toString(), Matcher.quoteReplacement(newName));
    }
}
