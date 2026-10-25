package org.pangrova.coordinator.file;

import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;

import jakarta.ws.rs.core.UriBuilder;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.spi.security.UserIdentity;

/** Role-authorized stable Stage links; bytes are transferred by the selected Worker. */
public final class StageFileUrlService {
    private final DataSource authority;
    private final FileSpaceAccessService access;

    /** The authority data source must be the same store used by the file and RBAC registries. */
    public StageFileUrlService(DataSource authority, FileSpaceAccessService access) {
        this.authority = Objects.requireNonNull(authority, "authority");
        this.access = Objects.requireNonNull(access, "access");
    }

    /** Builds a credential-free URL. Construction does not require an online Worker or an existing object. */
    public URI build(UserIdentity user, String catalog, String schema, String name, String path, URI baseUri) {
        String relative = FileSpaceAccessService.requireObjectPath(path);
        return withAuthority(() -> {
            var stage = access.stageLink(user, catalog, schema, name, relative);
            return UriBuilder.fromUri(baseUri).path("v1/stage-files/{id}")
                    .queryParam("path", relative).build(stage.id());
        });
    }

    /** Re-authorizes each access and exchanges the stable identity for one bounded Worker read ticket. */
    public URI read(UserIdentity user, UUID id, String path) {
        return withAuthority(() -> {
            var grant = access.stageRead(user, id, path);
            return UriBuilder.fromUri(grant.workerUrl()).path("v1/file-space/object")
                    .queryParam("descriptor", grant.descriptor())
                    .queryParam("signature", grant.signature()).build();
        });
    }

    private <T> T withAuthority(Supplier<T> operation) {
        try (var connection = authority.getConnection()) {
            connection.setAutoCommit(false);
            try {
                // Bound a queued mutation/publication conflict; never issue a ticket on lock failure.
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
                } catch (SQLException rollback) {
                    failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "Stage URL authority is unavailable", failure);
        }
    }
}
