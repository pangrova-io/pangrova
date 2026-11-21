package org.pangrova.governance.api.classification;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for managing audit configuration per table.
 */
@Experimental
public interface AuditConfigStore {
    void enableDetailedAudit(String catalog, String schema, String table);
    void disableDetailedAudit(String catalog, String schema, String table);
    boolean isDetailedAuditEnabled(String catalog, String schema, String table);
}
