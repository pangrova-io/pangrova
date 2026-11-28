package org.pangrova.planner.analysis;

import org.pangrova.connector.api.session.ConnectorSession;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Immutable query identity snapshot, never used as a process-wide cache key or logged. */
record CompilerSessionScope(
        String user, Optional<String> catalog, Optional<String> schema,
        Map<String, String> properties,
        Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> queryAuthority,
        OptionalLong publicationRevision) implements ConnectorSession {
    CompilerSessionScope {
        properties = Map.copyOf(properties);
        if (publicationRevision.isPresent() && (publicationRevision.getAsLong() < 0 || queryAuthority.isEmpty())) {
            throw new IllegalArgumentException("Publication revision requires admitted query authority");
        }
    }

    static CompilerSessionScope of(ConnectorSession source, OptionalLong publicationRevision) {
        return new CompilerSessionScope(source.getUser(), source.getCatalog(), source.getSchema(),
                source.getProperties(), source.getQueryAuthority(), publicationRevision);
    }

    @Override public String getUser() { return user; }
    @Override public Optional<String> getCatalog() { return catalog; }
    @Override public Optional<String> getSchema() { return schema; }
    @Override public Map<String, String> getProperties() { return properties; }
    @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
        return queryAuthority;
    }
}
