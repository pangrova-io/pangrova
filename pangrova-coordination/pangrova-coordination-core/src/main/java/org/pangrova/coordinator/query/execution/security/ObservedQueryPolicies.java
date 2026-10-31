package org.pangrova.coordinator.query.execution.security;

import org.pangrova.planner.security.PolicyObservationSink;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.security.api.AccessDeniedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Query-local observations of policy values actually consumed by the planner. */
final class ObservedQueryPolicies implements PolicyObservationSink {

    private final Map<String, Optional<String>> filters = new HashMap<>();
    private final Map<String, Map<String, Optional<String>>> masks = new HashMap<>();

    @Override
    public void rowFilter(QualifiedTableName table, Optional<String> expression) {
        record(filters, table.toString(), expression);
    }

    @Override
    public void columnMask(QualifiedTableName table, String column, Optional<String> expression) {
        record(masks.computeIfAbsent(table.toString(), ignored -> new HashMap<>()), column, expression);
    }

    Map<String, PolicySnapshot> snapshot() {
        if (!filters.keySet().equals(masks.keySet())) {
            throw new AccessDeniedException("Protected compilation has incomplete policy observations");
        }
        Map<String, PolicySnapshot> result = new HashMap<>();
        filters.forEach((table, filter) -> result.put(table, new PolicySnapshot(filter, masks.get(table))));
        return Map.copyOf(result);
    }

    private static void record(Map<String, Optional<String>> values, String key, Optional<String> value) {
        Optional<String> previous = values.putIfAbsent(key, value);
        if (previous != null && !previous.equals(value)) {
            throw new AccessDeniedException("Security policy changed while compiling the protected plan");
        }
    }
}
