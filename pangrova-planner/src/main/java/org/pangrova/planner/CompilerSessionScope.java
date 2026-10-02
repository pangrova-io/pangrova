package org.pangrova.planner;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/** Immutable query identity snapshot, never used as a process-wide cache key or logged. */
record CompilerSessionScope(
        String user, Optional<String> catalog, Optional<String> schema,
        Map<String, String> properties) implements ConnectorSession {
    CompilerSessionScope {
        properties = Map.copyOf(properties);
    }

    static CompilerSessionScope of(ConnectorSession source) {
        return new CompilerSessionScope(source.getUser(), source.getCatalog(), source.getSchema(),
                source.getProperties());
    }

    @Override public String getUser() { return user; }
    @Override public Optional<String> getCatalog() { return catalog; }
    @Override public Optional<String> getSchema() { return schema; }
    @Override public Map<String, String> getProperties() { return properties; }
}
