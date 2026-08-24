package org.pangrova.coordinator.ai.training;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/**
 * Minimal ConnectorSession for training data serving operations.
 */
record TrainingConnectorSession(String catalog) implements ConnectorSession {

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
