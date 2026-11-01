package org.pangrova.planner;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/** Immutable query identity snapshot, never used as a process-wide cache key or logged. */
record CompilerSessionScope(
        String user, Optional<String> catalog, Optional<String> schema,
        Map<String, String> properties,
        Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> queryAuthority) implements ConnectorSession {
    CompilerSessionScope {
        properties = Map.copyOf(properties);
    }

    static CompilerSessionScope of(ConnectorSession source) {
        return new CompilerSessionScope(source.getUser(), source.getCatalog(), source.getSchema(),
                source.getProperties(), source.getQueryAuthority());
    }

    @Override public String getUser() { return user; }
    @Override public Optional<String> getCatalog() { return catalog; }
    @Override public Optional<String> getSchema() { return schema; }
    @Override public Map<String, String> getProperties() { return properties; }
    @Override public Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
        return queryAuthority;
    }
}
