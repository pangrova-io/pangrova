package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.ConnectorSession;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceSessionIdentityTest {
    @Test
    void ignoresClientAliasAndRequiresCanonicalAuthenticatedTenant() {
        var session = session(Map.of("tenant_id", "authenticated", "pangrova.tenant_id", "spoofed"));
        assertThat(LanceSessionIdentity.tenant(session)).isEqualTo("authenticated");
        assertThatThrownBy(() -> LanceSessionIdentity.requireTenant(session, "spoofed"))
                .isInstanceOf(SecurityException.class).hasMessageContaining("mismatch");
        assertThatThrownBy(() -> LanceSessionIdentity.tenant(session(Map.of("pangrova.tenant_id", "spoofed"))))
                .isInstanceOf(SecurityException.class).hasMessageContaining("resolved tenant");
    }

    @Test
    void tenantlessSchemaAndDirectoryAccessRejectBeforeOpeningJdbc() {
        var connector = new LanceConnectorFactory().create("lance_catalog", Map.of(
                "jdbc.url", "jdbc:postgresql://127.0.0.1:1/unopened", "warehouse", "s3://warehouse"), Optional::empty);
        var session = session(Map.of("pangrova.tenant_id", "spoofed"));
        var metadata = connector.getMetadata(session);
        assertThatThrownBy(() -> metadata.createSchema(session, "db")).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> metadata.listSchemaNames(session)).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> metadata.listTables(session, Optional.empty())).isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> metadata.getTableHandle(session,
                new org.pangrova.spi.connector.metadata.table.SchemaTableName("db", "table")))
                .isInstanceOf(SecurityException.class);
    }

    private static ConnectorSession session(Map<String, String> properties) {
        return new ConnectorSession() {
            @Override
            public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
                return java.util.Optional.empty();
            }

            @Override public String getUser() { return "authenticated-user"; }
            @Override public Optional<String> getCatalog() { return Optional.of("lance_catalog"); }
            @Override public Optional<String> getSchema() { return Optional.of("db"); }
            @Override public Map<String, String> getProperties() { return properties; }
        };
    }
}
