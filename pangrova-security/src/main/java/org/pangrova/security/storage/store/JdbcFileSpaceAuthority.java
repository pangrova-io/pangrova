package org.pangrova.security.storage.store;

import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.file.FileSpace;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Supplier;

/** Keeps file-ticket issuance ordered with durable role, tenant, namespace and profile mutations. */
public final class JdbcFileSpaceAuthority implements FileSpaceAuthority {
    private final DataSource source;
    private final JdbcTenantStore tenants;

    public JdbcFileSpaceAuthority(DataSource source) {
        this.source = Objects.requireNonNull(source, "source");
        tenants = new JdbcTenantStore(source);
    }

    @Override
    public <T> T read(Supplier<T> operation) {
        try (var connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (var statement = connection.createStatement()) {
                    statement.execute("SET LOCAL lock_timeout = '5s'");
                    statement.execute("SET LOCAL statement_timeout = '5s'");
                }
                AuthorizationTransactionGate.lockPublication(connection);
                T result = operation.get();
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "File access authority unavailable", failure);
        }
    }

    @Override
    public UserIdentity resolve(UserIdentity authenticated, FileSpace space) {
        if (authenticated == null || authenticated.principal() == null || authenticated.principal().isBlank()) {
            throw new AccessDeniedException("Authenticated principal is required for file access");
        }
        var tenant = tenants.getTenantForUser(authenticated.principal()).orElseThrow(
                () -> new AccessDeniedException("File subject has no current tenant membership")).tenant();
        if (tenant.status() != TenantStatus.ACTIVE) {
            throw new AccessDeniedException("File subject requires an ACTIVE tenant");
        }
        String claimed = authenticated.attributes().get("tenant_id");
        if (claimed != null && !claimed.equals(tenant.tenantId())) {
            throw new AccessDeniedException("File tenant claim no longer matches current membership");
        }
        if ((!tenant.allowedCatalogs().isEmpty() && !tenant.allowedCatalogs().contains(space.catalog()))
                || (!tenant.allowedSchemas().isEmpty() && !tenant.allowedSchemas().contains(space.schema()))) {
            throw new AccessDeniedException("File space is outside the current tenant namespace");
        }
        var attributes = new HashMap<>(authenticated.attributes());
        attributes.put("tenant_id", tenant.tenantId());
        return new UserIdentity(authenticated.principal(), authenticated.groups(), attributes,
                authenticated.authenticatedAt());
    }
}
