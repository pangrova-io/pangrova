package org.pangrova.governance.lineage;

/**
 * SPI for sending lineage events to a backend (Marquez, DataHub, etc.).
 */
public interface LineageBackend {
    void send(LineageEvent event);
    boolean isAvailable();
}
