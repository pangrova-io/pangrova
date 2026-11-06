package org.pangrova.connector.iceberg;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Enforces column-level encryption access: columns encrypted with a key that
 * is not part of the session's accessible key set are rejected at projection
 * time.
 */
final class IcebergColumnAccessGuard {

    private final Set<String> accessibleEncryptionKeys;

    IcebergColumnAccessGuard(Set<String> accessibleEncryptionKeys) {
        this.accessibleEncryptionKeys = accessibleEncryptionKeys;
    }

    /**
     * Validates that all projected columns are either unencrypted or encrypted
     * with a key accessible in the current session.
     */
    void validate(ConnectorTableHandle table, List<String> projectedColumns) {
        if (accessibleEncryptionKeys.isEmpty()) {
            return;
        }
        IcebergTableHandle handle = (IcebergTableHandle) table;
        String encryptionSpec = handle.properties().get("encryption.column.keys");
        if (encryptionSpec == null || encryptionSpec.isBlank()) {
            return;
        }
        Map<String, String> columnToKey = parseEncryptionColumnKeys(encryptionSpec);
        for (String column : projectedColumns) {
            String keyId = columnToKey.get(column.toLowerCase(Locale.ROOT));
            if (keyId != null && !accessibleEncryptionKeys.contains(keyId)) {
                throw new PangrovaException(
                        PangrovaErrorCode.CONNECTOR_PERMISSION_DENIED,
                        "ENCRYPTION_ACCESS_DENIED: column '" + column
                                + "' is encrypted with key '" + keyId
                                + "' which is not accessible in the current session");
            }
        }
    }

    private static Map<String, String> parseEncryptionColumnKeys(String spec) {
        Map<String, String> result = new HashMap<>();
        for (String group : spec.split(";")) {
            parseKeyGroup(group, result);
        }
        return result;
    }

    private static void parseKeyGroup(String group, Map<String, String> result) {
        String trimmed = group.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        int colonIdx = trimmed.indexOf(':');
        if (colonIdx < 0) {
            return;
        }
        String keyId = trimmed.substring(0, colonIdx).trim();
        String columns = trimmed.substring(colonIdx + 1).trim();
        for (String col : columns.split(",")) {
            result.put(col.trim().toLowerCase(Locale.ROOT), keyId);
        }
    }
}
