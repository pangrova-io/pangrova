package org.pangrova.security.spi.audit;

import org.pangrova.security.api.audit.AuditEvent;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for persisting audit events to a configurable backend (MySQL, ES, Kafka, etc.).
 */
@Experimental
public interface AuditEventWriter {

    void write(AuditEvent event);
}
