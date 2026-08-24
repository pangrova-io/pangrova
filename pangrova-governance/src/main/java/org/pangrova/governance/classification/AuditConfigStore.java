package org.pangrova.governance.classification;

/**
 * SPI for managing audit configuration per table.
 */
public interface AuditConfigStore {
    void enableDetailedAudit(String catalog, String schema, String table);
    void disableDetailedAudit(String catalog, String schema, String table);
    boolean isDetailedAuditEnabled(String catalog, String schema, String table);
}
