package org.pangrova.coordinator.query.execution;

import org.pangrova.parser.api.enums.TimeTravelType;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.planner.metadata.catalog.PlanningSnapshotSelectors;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.parser.parse.TimeTravelSqlClauses;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.ast.TimeTravelSpec;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorReadDiscovery;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Resolves source selectors before schema binding; SQL spans remain owned by the parser. */
final class TimeTravelExtractor {
    @FunctionalInterface
    interface RefSnapshotResolver {
        Optional<ResolvedReference> resolve(String catalogName, String schemaName,
                                            String tableName, String refType, String refName);
    }

    record ResolvedReference(long snapshotId, QualifiedTableName table, Optional<ConnectorReadDiscovery> discovery) {}

    private record NamedReference(List<String> table, TimeTravelType type, String name) {}

    private TimeTravelExtractor() {}

    static CompilationPhase.TimeTravelResult extract(String sql, QuerySession context,
                                                      RefSnapshotResolver resolver) {
        var parsed = TimeTravelSqlClauses.extract(sql);
        var routes = org.pangrova.query.session.BranchRoutingContext.fromSessionProperties(context.sessionProperties());
        if (!routes.hasRouting()
                && parsed.references().stream().noneMatch(ref -> ref.table().timeTravelSpec().isPresent())) {
            return CompilationPhase.TimeTravelResult.passthrough(sql);
        }
        ConnectorSession session = SessionContextConverter.toConnectorSession(context);
        Map<String, OptionalLong> selections = new HashMap<>();
        Map<String, Long> overrides = new HashMap<>();
        Map<NamedReference, ResolvedReference> namedReferences = new HashMap<>();
        Map<QualifiedTableName, ConnectorReadDiscovery> discoveries = new HashMap<>();
        for (var reference : parsed.references()) {
            var ref = reference.table();
            if (reference.cte()) {
                if (ref.timeTravelSpec().isPresent()) throw invalid("Time travel cannot target a logical CTE");
                continue;
            }
            List<String> parts = qualify(ref, session);
            String key = PlanningSnapshotSelectors.tableKey(parts.get(0), parts.get(1), parts.get(2));
            var selection = ref.timeTravelSpec().or(() -> routes.getBranch(key).map(TimeTravelSpec::branch));
            OptionalLong selected = selection.isPresent()
                    ? OptionalLong.of(snapshot(parts, selection.orElseThrow(), resolver, namedReferences, discoveries))
                    : OptionalLong.empty();
            var previous = selections.putIfAbsent(key, selected);
            if (previous != null && !previous.equals(selected)) {
                throw invalid("Multiple source snapshots for one table require per-relation read bindings: " + key);
            }
            if (selected.isPresent()) overrides.put(key, selected.getAsLong());
        }
        return new CompilationPhase.TimeTravelResult(parsed.strippedSql(), overrides, discoveries);
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

    private static long snapshot(List<String> parts, TimeTravelSpec spec, RefSnapshotResolver resolver,
            Map<NamedReference, ResolvedReference> references,
            Map<QualifiedTableName, ConnectorReadDiscovery> discoveries) {
        long selected;
        if (spec.type() == TimeTravelType.BRANCH || spec.type() == TimeTravelType.TAG) {
            if (resolver == null) throw invalid("BRANCH/TAG time travel requires a ref resolver");
            var resolved = references.computeIfAbsent(new NamedReference(parts, spec.type(), spec.refName()),
                    reference -> resolver.resolve(parts.get(0), parts.get(1), parts.get(2),
                            reference.type().name(), reference.name())
                            .orElseThrow(() -> invalid("Selected source ref is unavailable: " + reference.name())));
            resolved.discovery().ifPresent(discovery -> discoveries.putIfAbsent(resolved.table(), discovery));
            selected = resolved.snapshotId();
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
