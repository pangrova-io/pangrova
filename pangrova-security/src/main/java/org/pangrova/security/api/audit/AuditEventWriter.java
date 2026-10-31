package org.pangrova.security.api.audit;

/**
 * SPI for persisting audit events to a configurable backend (MySQL, ES, Kafka, etc.).
 */
public interface AuditEventWriter {

    void write(AuditEvent event);
}
