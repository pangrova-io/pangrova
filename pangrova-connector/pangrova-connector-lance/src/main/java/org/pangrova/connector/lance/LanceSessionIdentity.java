package org.pangrova.connector.lance;

import org.pangrova.connector.spi.ConnectorSession;

/** Enforces tenant identity on every directory and split operation. */
final class LanceSessionIdentity {
    private LanceSessionIdentity() {}
    static String tenant(ConnectorSession session) {
        String tenant = session.getProperties().get("tenant_id");
        if (tenant == null || tenant.isBlank() || session.getUser() == null || session.getUser().isBlank()) {
            throw new SecurityException("Lance catalog requires a resolved tenant and principal");
        }
        return tenant;
    }
    static void requireTenant(ConnectorSession session, String expected) {
        if (!expected.equals(tenant(session))) throw new SecurityException("Lance table handle tenant mismatch");
    }
}
