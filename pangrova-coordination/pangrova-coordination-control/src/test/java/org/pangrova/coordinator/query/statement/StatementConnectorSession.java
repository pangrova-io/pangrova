package org.pangrova.coordinator.query.statement;

import org.pangrova.connector.api.session.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/**
 * Minimal ConnectorSession for internal coordinator operations (split fetching, DDL routing).
 */
public record StatementConnectorSession(String catalog, String schema) implements ConnectorSession {
    @Override
    public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
        return java.util.Optional.empty();
    }


    StatementConnectorSession(String catalog) {
        this(catalog, "default");
    }

    @Override
    public String getUser() {
        return "system";
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.of(catalog);
    }

    @Override
    public Optional<String> getSchema() {
        return Optional.of(schema);
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of();
    }
}
