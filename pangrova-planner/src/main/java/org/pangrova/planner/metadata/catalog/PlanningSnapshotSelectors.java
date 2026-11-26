package org.pangrova.planner.metadata.catalog;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;

import org.pangrova.connector.api.metadata.ConnectorReadDiscovery;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.parser.api.ast.TableRef;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Carries resolved table-qualified version selectors into the sole authenticated session planner. */
public final class PlanningSnapshotSelectors {
    /** Caller-supplied properties with this prefix must be removed at query admission conversion. */
    public static final String PROPERTY_PREFIX = "pangrova.source_snapshot.";

    private PlanningSnapshotSelectors() {}

    /** Binds selectors produced by SQL/ref resolution, never by client session properties. */
    public static ConnectorSession bind(ConnectorSession session, Map<String, Long> snapshots) {
        return bind(session, snapshots, Map.of());
    }

    /** Transfers exact named-reference observations to the same authenticated compilation. */
    public static ConnectorSession bind(ConnectorSession session, Map<String, Long> snapshots,
            Map<QualifiedTableName, ConnectorReadDiscovery> discoveries) {
        var properties = new LinkedHashMap<>(session.getProperties());
        properties.keySet().removeIf(key -> key.startsWith(PROPERTY_PREFIX));
        snapshots.forEach((table, snapshot) -> {
            if (table == null || table.isBlank() || snapshot == null || snapshot <= 0) {
                throw new IllegalArgumentException("Resolved source snapshot selector is invalid");
            }
            String key = PROPERTY_PREFIX + table.toLowerCase(Locale.ROOT);
            String previous = properties.putIfAbsent(key, Long.toString(snapshot));
            if (previous != null && !previous.equals(Long.toString(snapshot))) {
                throw new IllegalArgumentException("Conflicting resolved source snapshots for " + table);
            }
        });
        return new SelectedSession(session.getUser(), session.getCatalog(), session.getSchema(),
                Map.copyOf(properties), session.getQueryAuthority(), Map.copyOf(discoveries));
    }

    static Map<QualifiedTableName, ConnectorReadDiscovery> discoveries(ConnectorSession session) {
        return session instanceof SelectedSession selected ? selected.discoveries() : Map.of();
    }

    /** Retains the compiler's named-source observations when its session identity is frozen. */
    public static ConnectorSession preserveDiscoveries(ConnectorSession source, ConnectorSession frozen) {
        var selected = discoveries(source);
        return selected.isEmpty() ? frozen : new SelectedSession(frozen.getUser(),
                frozen.getCatalog(), frozen.getSchema(), frozen.getProperties(), frozen.getQueryAuthority(), selected);
    }

    /** Returns the snapshot pinned for a table in the session properties, if any. */
    public static OptionalLong snapshot(ConnectorSession session, String catalog, String schema, String table) {
        String value = session.getProperties().get(
                PROPERTY_PREFIX + tableKey(catalog, schema, table));
        if (value == null) return OptionalLong.empty();
        long snapshot = Long.parseLong(value);
        if (snapshot <= 0) throw new IllegalArgumentException("Resolved source snapshot must be positive");
        return OptionalLong.of(snapshot);
    }

    /** Checks that a parsed source can use an explicitly selected snapshot in this bound session. */
    public static boolean hasSelection(ConnectorSession session, TableRef table) {
        var parts = table.name().parts();
        if (parts.isEmpty() || parts.size() > 3) {
            return false;
        }
        String catalog = parts.size() == 3 ? parts.get(0) : session.getCatalog().orElse(null);
        String schema = parts.size() == 1 ? session.getSchema().orElse(null) : parts.get(parts.size() - 2);
        String name = parts.get(parts.size() - 1);
        if (table.systemTableRef().isPresent()) {
            name += "$" + table.systemTableRef().orElseThrow().systemTableName();
        }
        return catalog != null && schema != null && snapshot(session, catalog, schema, name).isPresent();
    }

    /** Encodes resolved identifier components without confusing literal dots with qualification. */
    public static String tableKey(String catalog, String schema, String table) {
        return java.util.stream.Stream.of(catalog, schema, table)
                .map(PlanningSnapshotSelectors::component).collect(java.util.stream.Collectors.joining("."));
    }

    private static String component(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException("Resolved table identifier must not be empty");
        }
        String normalized = identifier.toLowerCase(Locale.ROOT);
        return normalized.contains(".") || normalized.contains("\"")
                ? "\"" + normalized.replace("\"", "\"\"") + "\"" : normalized;
    }

    private record SelectedSession(String user, Optional<String> catalog, Optional<String> schema,
            Map<String, String> properties,
            Optional<ConnectorQueryAuthority> queryAuthority,
            Map<QualifiedTableName, ConnectorReadDiscovery> discoveries) implements ConnectorSession {
        @Override public String getUser() { return user; }
        @Override public Optional<String> getCatalog() { return catalog; }
        @Override public Optional<String> getSchema() { return schema; }
        @Override public Map<String, String> getProperties() { return properties; }
        @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return queryAuthority;
        }
    }
}
