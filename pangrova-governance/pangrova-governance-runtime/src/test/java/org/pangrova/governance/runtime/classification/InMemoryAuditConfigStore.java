package org.pangrova.governance.runtime.classification;

import org.pangrova.governance.api.classification.AuditConfigStore;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory audit configuration store.
 * Tracks which tables have detailed audit enabled.
 */
public final class InMemoryAuditConfigStore implements AuditConfigStore {

    private final Set<String> enabledTables = ConcurrentHashMap.newKeySet();

    @Override
    public void enableDetailedAudit(String catalog, String schema, String table) {
        enabledTables.add(key(catalog, schema, table));
    }

    @Override
    public void disableDetailedAudit(String catalog, String schema, String table) {
        enabledTables.remove(key(catalog, schema, table));
    }

    @Override
    public boolean isDetailedAuditEnabled(String catalog, String schema, String table) {
        return enabledTables.contains(key(catalog, schema, table));
    }

    private static String key(String catalog, String schema, String table) {
        return catalog + "." + schema + "." + table;
    }
}
