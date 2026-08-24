package org.pangrova.coordinator.metadata;

import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

/** Minimal ConnectorSession for Calcite schema resolution and internal metadata operations. */
record SystemConnectorSession(String catalog) implements ConnectorSession {
    @Override public String getUser() { return "system"; }
    @Override public Optional<String> getCatalog() { return Optional.of(catalog); }
    @Override public Optional<String> getSchema() { return Optional.empty(); }
    @Override public Map<String, String> getProperties() { return Map.of(); }
}
