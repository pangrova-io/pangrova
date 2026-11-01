package org.pangrova.coordinator.ai.branch;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/**
 * Minimal ConnectorSession for branch/tag DDL operations.
 */
record BranchConnectorSession(String catalog) implements ConnectorSession {
    @Override
    public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
        return java.util.Optional.empty();
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
        return Optional.of("default");
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of();
    }
}
