package org.pangrova.coordinator.file;

import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.security.storage.store.JdbcFileSpaceAuthority;
import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Real immutable tenant facts shared by focused issuer tests; no business authorization is fabricated. */
public final class FileAuthorityTestFixture {
    private FileAuthorityTestFixture() {}

    public static JdbcFileSpaceAuthority authority() {
        return Holder.AUTHORITY;
    }

    public static JdbcFileSpaceAuthority provision(DataSource source) {
        var tenants = new JdbcTenantStore(source);
        tenants.createTenant(new Tenant("tenant-a", "File issuer tests", TenantStatus.ACTIVE,
                new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0), List.of(), List.of(), "", Map.of(),
                Instant.EPOCH, "fixture"));
        tenants.addUserToTenant("tenant-a", "alice", TenantRole.MEMBER);
        return new JdbcFileSpaceAuthority(source);
    }

    private static final class Holder {
        private static final JdbcFileSpaceAuthority AUTHORITY = create();

        private static JdbcFileSpaceAuthority create() {
            try {
                return provision(SharedPostgresContainer.migratedDataSource("file_issuer_subject"));
            } catch (java.sql.SQLException failure) {
                throw new IllegalStateException("File issuer PostgreSQL fixture unavailable", failure);
            }
        }
    }
}
