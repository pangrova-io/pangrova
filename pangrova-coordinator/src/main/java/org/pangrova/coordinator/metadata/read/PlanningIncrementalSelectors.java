package org.pangrova.coordinator.metadata.read;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.SnapshotRange;

/**
 * Carries trusted append-only snapshot ranges into the authenticated session planner.
 *
 * <p>Ranges are derived exclusively from a refresh plan produced by the
 * materialization kernel. Client session properties using this prefix are
 * removed at admission, so a query can never request an incremental read that
 * the kernel did not plan and pin.
 */
public final class PlanningIncrementalSelectors {

    /** Caller-supplied properties with this prefix must be removed at admission. */
    public static final String PROPERTY_PREFIX = "pangrova.source_incremental.";

    private PlanningIncrementalSelectors() {
    }

    /** Binds kernel-produced ranges, replacing any value previously present. */
    public static ConnectorSession bind(ConnectorSession session, Map<String, SnapshotRange> ranges) {
        var properties = new LinkedHashMap<>(session.getProperties());
        properties.keySet().removeIf(key -> key.startsWith(PROPERTY_PREFIX));
        ranges.forEach((table, range) -> {
            if (table == null || table.isBlank() || range == null) {
                throw new IllegalArgumentException("Resolved incremental range selector is invalid");
            }
            if (table.startsWith(PROPERTY_PREFIX)) {
                throw new IllegalArgumentException(
                        "Incremental range selector must be an unqualified table key: " + table);
            }
            properties.put(PROPERTY_PREFIX + table.toLowerCase(Locale.ROOT), range.encode());
        });
        return new SelectedSession(session.getUser(), session.getCatalog(), session.getSchema(),
                Map.copyOf(properties), session.getQueryAuthority());
    }

    /** Returns the trusted range for one table, if the session carries one. */
    public static Optional<SnapshotRange> range(
            ConnectorSession session, String catalog, String schema, String table) {
        String value = session.getProperties().get(
                PROPERTY_PREFIX + PlanningSnapshotSelectors.tableKey(catalog, schema, table));
        return value == null ? Optional.empty() : Optional.of(SnapshotRange.parse(value));
    }

    private record SelectedSession(String user, Optional<String> catalog, Optional<String> schema,
            Map<String, String> properties,
            Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> queryAuthority)
            implements ConnectorSession {
        @Override
        public String getUser() {
            return user;
        }

        @Override
        public Optional<String> getCatalog() {
            return catalog;
        }

        @Override
        public Optional<String> getSchema() {
            return schema;
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return queryAuthority;
        }
    }
}
