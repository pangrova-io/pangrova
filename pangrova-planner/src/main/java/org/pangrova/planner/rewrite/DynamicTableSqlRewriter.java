package org.pangrova.planner.rewrite;

import org.pangrova.parser.api.ast.CteQuery;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.JoinClause;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.parser.build.antlr.PangrovaSqlLexer;
import org.pangrova.parser.parse.TimeTravelSqlClauses.Analysis;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pre-parse SQL rewriter that replaces Dynamic Table names with their
 * published physical generation.
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
     * Rewrites Dynamic Table references in SQL text to published physical generations.
     * Iterates to resolve chained DTs; returns original SQL unchanged if no DTs found.
     */
    public String rewrite(String sql) {
        return rewriteWithAuthority(sql).sql();
    }

    /** Returns the rewritten SQL and the durable logical identity of each physical scan. */
    public RewriteWithAuthority rewriteWithAuthority(String sql) {
        if (!dynamicTableRewriter.allowDirectGenerationAccess()) {
            rejectExplicitGenerationReference(sql);
        }
        String rewritten = sql;
        Map<QualifiedTableName, QualifiedTableName> physicalToLogical = new HashMap<>();
        Map<QualifiedTableName, Optional<DynamicTableRewriter.PublishedTarget>> pinnedTargets =
                new HashMap<>();
        for (int depth = 0; depth < MAX_CHAIN_DEPTH; depth++) {
            String before = rewritten;
            rewritten = singlePassRewrite(rewritten, physicalToLogical, pinnedTargets);
            if (rewritten.equals(before)) {
                break;
            }
        }
        return new RewriteWithAuthority(rewritten, physicalToLogical);
    }

    /** Rewrites a parsed query using exact relation identities and query-local publication pins. */
    public DynamicTableAstRewriter.RewriteWithAuthority rewriteWithAuthority(Analysis input) {
        if (!dynamicTableRewriter.allowDirectGenerationAccess()) {
            for (var reference : input.references()) {
                var parts = reference.table().name().parts();
                if (parts.stream().anyMatch(RESERVED_SCHEMA::equalsIgnoreCase)
                        || isGenerationName(parts.get(parts.size() - 1))) {
                    throw generationAccessDenied();
                }
            }
        }
        // Match the existing SQL planner's Dynamic Table lookup scope. The guard above also
        // checks nested references, where no publication lookup is needed.
        List<TableRef> candidates = extractTableRefs(input.statement());
        if (candidates.isEmpty()) {
            return new DynamicTableAstRewriter.RewriteWithAuthority(input.statement(), Map.of());
        }
        Set<TableRef> candidateSites = Collections.newSetFromMap(new IdentityHashMap<>());
        candidateSites.addAll(candidates);
        var physicalToLogical = new HashMap<QualifiedTableName, QualifiedTableName>();
        var pinnedTargets = new HashMap<QualifiedTableName, Optional<DynamicTableRewriter.PublishedTarget>>();
        Map<TableRef, TableRef> replacements = new IdentityHashMap<>();
        for (var reference : input.references()) {
            var ref = reference.table();
            if (reference.cte() || ref.systemTableRef().isPresent()
                    || !candidateSites.contains(ref)) {
                continue;
            }
            var target = publishedTarget(ref, pinnedTargets);
            if (target.isEmpty()) {
                continue;
            }
            if (ref.timeTravelSpec().isPresent()) {
                throw new org.pangrova.planner.api.exception.PlannerException(
                        org.pangrova.common.enums.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Time travel cannot target a logical Dynamic Table: " + ref.name().parts());
            }
            var published = target.orElseThrow();
            recordAuthority(published, physicalToLogical);
            replacements.put(ref, physicalReference(ref, published.physical()));
        }
        return new DynamicTableAstRewriter.RewriteWithAuthority(
                replaceCandidates(input.statement(), replacements), physicalToLogical);
    }

    private static TableRef physicalReference(TableRef ref, QualifiedTableName physical) {
        var name = new Identifier(List.of(physical.catalog(), physical.schema(), physical.table()));
        var alias = ref.alias().or(() -> Optional.of(ref.name().parts().get(ref.name().parts().size() - 1)));
        return new TableRef(name, ref.timeTravelSpec(), alias, ref.subquery(), ref.lateral(),
                ref.tableFunctionRef(), ref.systemTableRef(), ref.tableSample(), ref.pivotClause(),
                ref.unpivotClause(), ref.valuesTable());
    }

    private static Statement replaceCandidates(Statement statement, Map<TableRef, TableRef> replacements) {
        if (replacements.isEmpty()) {
            return statement;
        }
        if (statement instanceof Query query) {
            return new Query(query.distinct(), query.selectItems(),
                    query.from().stream().map(ref -> replacements.getOrDefault(ref, ref)).toList(),
                    query.joins().stream().map(join -> new JoinClause(join.joinType(),
                            replacements.getOrDefault(join.right(), join.right()),
                            join.condition(), join.usingColumns())).toList(),
                    query.where(), query.groupBy(), query.having(), query.qualify(), query.windowDefinitions(),
                    query.orderBy(), query.rerankBy(), query.limit(), query.offset());
        }
        if (statement instanceof CteQuery cte) {
            return new CteQuery(cte.recursive(), cte.ctes(), replaceCandidates(cte.body(), replacements));
        }
        return statement;
    }

    private static void rejectExplicitGenerationReference(String originalSql) {
        List<? extends Token> tokens = new PangrovaSqlLexer(
                CharStreams.fromString(originalSql)).getAllTokens();
        java.util.Set<Integer> fromDepths = new HashSet<>();
        int depth = 0;
        for (int i = 0; i < tokens.size(); i++) {
            int type = tokens.get(i).getType();
            if (type == PangrovaSqlLexer.RPAREN) {
                fromDepths.remove(depth);
                depth--;
                continue;
            }
            if (type == PangrovaSqlLexer.LPAREN) {
                depth++;
                continue;
            }
            if (type == PangrovaSqlLexer.FROM) {
                fromDepths.add(depth);
            } else if (type == PangrovaSqlLexer.WHERE || type == PangrovaSqlLexer.GROUP
                    || type == PangrovaSqlLexer.HAVING || type == PangrovaSqlLexer.ORDER
                    || type == PangrovaSqlLexer.LIMIT || type == PangrovaSqlLexer.OFFSET
                    || type == PangrovaSqlLexer.UNION || type == PangrovaSqlLexer.INTERSECT
                    || type == PangrovaSqlLexer.EXCEPT || type == PangrovaSqlLexer.QUALIFY) {
                fromDepths.remove(depth);
            }
            if ((type == PangrovaSqlLexer.FROM || type == PangrovaSqlLexer.JOIN
                    || type == PangrovaSqlLexer.COMMA && fromDepths.contains(depth))
                    && generationTableName(tokens, i + 1)) {
                throw generationAccessDenied();
            }
        }
    }

    private static boolean generationTableName(List<? extends Token> tokens, int start) {
        String last = null;
        boolean reservedSchema = false;
        int index = start;
        while (index < tokens.size()) {
            Token token = tokens.get(index);
            if (token.getType() == PangrovaSqlLexer.LPAREN) {
                return false;
            }
            String part;
            if (token.getType() == PangrovaSqlLexer.QUOTED_IDENTIFIER
                    || token.getType() == PangrovaSqlLexer.BACKTICK_IDENTIFIER) {
                part = token.getText().substring(1, token.getText().length() - 1);
            } else {
                part = token.getText();
                if (!part.matches("[A-Za-z_][A-Za-z_0-9]*")) {
                    break;
                }
            }
            last = part;
            reservedSchema |= RESERVED_SCHEMA.equalsIgnoreCase(part);
            index++;
            if (index >= tokens.size() || tokens.get(index).getType() != PangrovaSqlLexer.DOT) {
                break;
            }
            index++;
        }
        if (last == null) {
            return false;
        }
        return reservedSchema || isGenerationName(last);
    }

    private static boolean isGenerationName(String name) {
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        return lower.startsWith(CANDIDATE_PREFIX) || lower.startsWith(LEGACY_CANDIDATE_PREFIX);
    }

    private static org.pangrova.security.api.exception.AccessDeniedException generationAccessDenied() {
        return new org.pangrova.security.api.exception.AccessDeniedException(
                "Direct access to a Dynamic Table generation is forbidden");
    }

    // Mirrors the kernel's InternalNamespace; the planner must not depend on materialization.
    private static final String RESERVED_SCHEMA = "__pangrova_materializations";
    private static final String CANDIDATE_PREFIX = "__pg_mat_";
    private static final String LEGACY_CANDIDATE_PREFIX = "__pg_dt_";

    /** Query-owned, immutable table identity mapping consumed by protected planning. */
    public record RewriteWithAuthority(String sql,
            Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        public RewriteWithAuthority {
            physicalToLogical = Map.copyOf(physicalToLogical);
        }
    }

    private String singlePassRewrite(String sql,
            Map<QualifiedTableName, QualifiedTableName> physicalToLogical,
            Map<QualifiedTableName, Optional<DynamicTableRewriter.PublishedTarget>> pinnedTargets) {
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
            rewritten = rewriteTableRef(rewritten, ref, physicalToLogical, pinnedTargets);
        }
        return rewritten;
    }

    private String fallbackRewrite(String sql) {
        LOG.warn("event=dt_rewrite_ast_failed action=skip_fallback reason=regex_replacement_unsafe");
        return sql;
    }

    private String rewriteTableRef(String sql, TableRef ref,
            Map<QualifiedTableName, QualifiedTableName> physicalToLogical,
            Map<QualifiedTableName, Optional<DynamicTableRewriter.PublishedTarget>> pinnedTargets) {
        var target = publishedTarget(ref, pinnedTargets);
        if (target.isEmpty()) {
            return sql;
        }
        var published = target.orElseThrow();
        String replaced = replaceTableRef(sql, ref.name(), published.physical().toString());
        if (replaced.equals(sql)) {
            return sql;
        }
        recordAuthority(published, physicalToLogical);
        return replaced;
    }

    private Optional<DynamicTableRewriter.PublishedTarget> publishedTarget(TableRef ref,
            Map<QualifiedTableName, Optional<DynamicTableRewriter.PublishedTarget>> pinnedTargets) {
        List<String> nameParts = ref.name().parts();
        return dynamicTableRewriter.canonicalPublishedLogical(nameParts)
                .map(logical -> pinnedTargets.computeIfAbsent(logical,
                        ignored -> dynamicTableRewriter.resolvePublishedTarget(nameParts)))
                .orElseGet(() -> dynamicTableRewriter.resolvePublishedTarget(nameParts));
    }

    private static void recordAuthority(DynamicTableRewriter.PublishedTarget published,
            Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        QualifiedTableName previous = physicalToLogical.putIfAbsent(
                published.physical(), published.logical());
        if (previous != null && !previous.equals(published.logical())) {
            throw new IllegalStateException("Physical Dynamic Table generation has ambiguous authority");
        }
        LOG.info("event=dt_sql_rewrite original={} target={}",
                published.logical(), published.physical());
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
                for (int nameToken = i + 1; nameToken <= last; nameToken++) {
                    if (tokens.get(nameToken).getType() == PangrovaSqlLexer.QUOTED_IDENTIFIER
                            || tokens.get(nameToken).getType() == PangrovaSqlLexer.BACKTICK_IDENTIFIER) {
                        return sql;
                    }
                }
                String replacement = newName.contains(".") ? newName
                        : sql.substring(tokens.get(i + 1).getStartIndex(), tokens.get(last).getStartIndex())
                                + newName;
                return sql.substring(0, tokens.get(i + 1).getStartIndex()) + replacement
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
                // The parsed AST has already established that this is an identifier.
                // Non-reserved SQL keywords such as DEFAULT are legal name parts but
                // have their own lexer token type rather than IDENTIFIER.
                default -> token.getText();
            };
            if (actual == null || !actual.equalsIgnoreCase(parts.get(part))) return -1;
        }
        return index - 1;
    }
}
