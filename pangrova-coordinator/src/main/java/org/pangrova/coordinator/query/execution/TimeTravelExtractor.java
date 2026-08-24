package org.pangrova.coordinator.query.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.regex.Pattern;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.planner.SqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts time-travel specifications (FOR VERSION/SYSTEM_TIME AS OF) from SQL
 * and produces a stripped SQL string plus snapshot override map.
 */
final class TimeTravelExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(TimeTravelExtractor.class);

    @FunctionalInterface
    interface RefSnapshotResolver {
        OptionalLong resolve(String catalogName, String schemaName,
                             String tableName, String refType, String refName);
    }

    private TimeTravelExtractor() {}

    static CompilationPhase.TimeTravelResult extract(String sql, SqlCompiler compiler) {
        return extract(sql, compiler, null);
    }

    static CompilationPhase.TimeTravelResult extract(String sql, SqlCompiler compiler,
                                                      RefSnapshotResolver resolver) {
        org.pangrova.parser.ast.Statement ast;
        try {
            ast = compiler.parseToAst(sql);
        } catch (Exception e) {
            LOG.debug("event=time_travel_parse_fallback sql_length={} error={}",
                    sql.length(), e.getMessage());
            return CompilationPhase.TimeTravelResult.passthrough(sql);
        }
        if (ast == null) {
            return CompilationPhase.TimeTravelResult.passthrough(sql);
        }
        List<org.pangrova.parser.ast.TableRef> refs = extractAllTimeTravelRefs(ast);
        if (refs.isEmpty()) {
            return CompilationPhase.TimeTravelResult.passthrough(sql);
        }
        Map<String, Long> overrides = new HashMap<>();
        String stripped = sql;
        for (org.pangrova.parser.ast.TableRef ref : refs) {
            org.pangrova.parser.ast.TimeTravelSpec spec = ref.timeTravelSpec().orElseThrow();
            String fqn = String.join(".", ref.name().parts()).toLowerCase();
            long snapshotId;
            if (spec.type() == org.pangrova.parser.ast.TimeTravelSpec.Type.BRANCH
                    || spec.type() == org.pangrova.parser.ast.TimeTravelSpec.Type.TAG) {
                snapshotId = resolveRefSnapshot(fqn, spec, resolver);
                String keyword = spec.type() == org.pangrova.parser.ast.TimeTravelSpec.Type.BRANCH
                        ? "BRANCH" : "TAG";
                String pattern = "(?i)VERSION\\s+AS\\s+OF\\s+" + keyword
                        + "\\s+'" + Pattern.quote(spec.refName()) + "'";
                stripped = stripped.replaceFirst(pattern, "");
            } else {
                snapshotId = extractSnapshotId(spec.expression());
                String keyword = spec.type() == org.pangrova.parser.ast.TimeTravelSpec.Type.SYSTEM_VERSION
                        ? "VERSION" : "SYSTEM_TIME";
                String pattern = "(?i)FOR\\s+" + keyword + "\\s+AS\\s+OF\\s+" + snapshotId;
                stripped = stripped.replaceFirst(pattern, "");
            }
            overrides.put(fqn, snapshotId);
            LOG.info("event=time_travel_extracted table={} snapshotId={} type={}",
                    fqn, snapshotId, spec.type());
        }
        return new CompilationPhase.TimeTravelResult(stripped.trim(), Map.copyOf(overrides));
    }

    private static long resolveRefSnapshot(String fqn,
                                            org.pangrova.parser.ast.TimeTravelSpec spec,
                                            RefSnapshotResolver resolver) {
        if (resolver == null) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "BRANCH/TAG time travel requires a ref resolver but none is available");
        }
        String[] parts = fqn.split("\\.");
        if (parts.length < 3) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "BRANCH/TAG time travel requires fully-qualified table name (catalog.schema.table), got: " + fqn);
        }
        OptionalLong resolved = resolver.resolve(
                parts[0], parts[1], parts[2], spec.type().name(), spec.refName());
        if (resolved.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    spec.type().name().toLowerCase() + " '" + spec.refName()
                            + "' not found on table " + fqn);
        }
        return resolved.getAsLong();
    }

    private static List<org.pangrova.parser.ast.TableRef> extractAllTimeTravelRefs(
            org.pangrova.parser.ast.Statement ast) {
        List<org.pangrova.parser.ast.TableRef> allRefs = new ArrayList<>();
        if (ast instanceof org.pangrova.parser.ast.Query query) {
            allRefs.addAll(query.from());
            for (org.pangrova.parser.ast.JoinClause join : query.joins()) {
                allRefs.add(join.right());
            }
        } else if (ast instanceof org.pangrova.parser.ast.CteQuery cte
                && cte.body() instanceof org.pangrova.parser.ast.Query query) {
            allRefs.addAll(query.from());
            for (org.pangrova.parser.ast.JoinClause join : query.joins()) {
                allRefs.add(join.right());
            }
        }
        return allRefs.stream()
                .filter(r -> r.timeTravelSpec().isPresent())
                .toList();
    }

    private static long extractSnapshotId(org.pangrova.parser.ast.Expression expr) {
        if (expr instanceof org.pangrova.parser.ast.Literal lit
                && lit.value() instanceof org.pangrova.parser.ast.LiteralValue.IntegerValue iv) {
            return iv.value();
        }
        throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "FOR VERSION/SYSTEM_TIME AS OF requires an integer snapshot ID, got: " + expr);
    }
}
