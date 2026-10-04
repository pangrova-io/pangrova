package org.pangrova.coordinator.query.execution;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.parser.TimeTravelSqlClauses;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.TimeTravelSpec;
import org.pangrova.spi.connector.ConnectorSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Resolves source selectors before schema binding; SQL spans remain owned by the parser. */
final class TimeTravelExtractor {
    @FunctionalInterface
    interface RefSnapshotResolver {
        OptionalLong resolve(String catalogName, String schemaName,
                             String tableName, String refType, String refName);
    }

    private TimeTravelExtractor() {}

    static CompilationPhase.TimeTravelResult extract(String sql, CoordinatorSessionContext context,
                                                      RefSnapshotResolver resolver) {
        var parsed = TimeTravelSqlClauses.extract(sql);
        if (parsed.references().stream().noneMatch(ref -> ref.table().timeTravelSpec().isPresent())) {
            return CompilationPhase.TimeTravelResult.passthrough(sql);
        }
        ConnectorSession session = SessionContextConverter.toConnectorSession(context);
        Map<String, OptionalLong> selections = new HashMap<>();
        Map<String, Long> overrides = new HashMap<>();
        for (var reference : parsed.references()) {
            var ref = reference.table();
            if (reference.cte()) {
                if (ref.timeTravelSpec().isPresent()) throw invalid("Time travel cannot target a logical CTE");
                continue;
            }
            List<String> parts = qualify(ref, session);
            String key = PlanningSnapshotSelectors.tableKey(parts.get(0), parts.get(1), parts.get(2));
            OptionalLong selected = ref.timeTravelSpec().isEmpty() ? OptionalLong.empty()
                    : OptionalLong.of(snapshot(parts, ref.timeTravelSpec().orElseThrow(), resolver));
            var previous = selections.putIfAbsent(key, selected);
            if (previous != null && !previous.equals(selected)) {
                throw invalid("Multiple source snapshots for one table require per-relation read bindings: " + key);
            }
            if (selected.isPresent()) overrides.put(key, selected.getAsLong());
        }
        return new CompilationPhase.TimeTravelResult(parsed.strippedSql(), Map.copyOf(overrides));
    }

    private static List<String> qualify(TableRef ref, ConnectorSession session) {
        List<String> name = new ArrayList<>(ref.name().parts());
        if (ref.systemTableRef().isPresent()) {
            int last = name.size() - 1;
            name.set(last, name.get(last) + "$" + ref.systemTableRef().orElseThrow().systemTableName());
        }
        List<String> parts = switch (name.size()) {
            case 1 -> List.of(session.getCatalog().orElseThrow(() -> invalid("Time travel needs a default catalog")),
                    session.getSchema().orElseThrow(() -> invalid("Time travel needs a default schema")), name.get(0));
            case 2 -> List.of(session.getCatalog().orElseThrow(() -> invalid("Time travel needs a default catalog")),
                    name.get(0), name.get(1));
            case 3 -> List.copyOf(name);
            default -> throw invalid("Invalid qualified source table name");
        };
        if (parts.stream().anyMatch(String::isBlank)) throw invalid("Source table identifiers must not be empty");
        return parts;
    }

    private static long snapshot(List<String> parts, TimeTravelSpec spec, RefSnapshotResolver resolver) {
        long selected;
        if (spec.type() == TimeTravelSpec.Type.BRANCH || spec.type() == TimeTravelSpec.Type.TAG) {
            if (resolver == null) throw invalid("BRANCH/TAG time travel requires a ref resolver");
            var resolved = resolver.resolve(parts.get(0), parts.get(1), parts.get(2), spec.type().name(), spec.refName());
            if (resolved.isEmpty()) throw invalid("Selected source ref is unavailable: " + spec.refName());
            selected = resolved.getAsLong();
        } else if (spec.expression() instanceof Literal literal
                && literal.value() instanceof LiteralValue.IntegerValue integer) {
            selected = integer.value();
        } else {
            throw invalid("FOR VERSION/SYSTEM_TIME AS OF requires an integer snapshot ID");
        }
        if (selected <= 0) throw invalid("Selected source snapshot must be positive");
        return selected;
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
