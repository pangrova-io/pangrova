package org.pangrova.persistence.security.postgresql.grant;

import org.pangrova.persistence.security.api.grant.PrivilegeMutationRepository;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.grant.PrivilegeMutation;
import org.pangrova.security.api.exception.PrivilegeMutationOutcomeUnknownException;
import org.pangrova.security.api.grant.PrivilegeMutationReceipt;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Objects;

/** PostgreSQL owner of current delegation authorization, exact-scope changes and their durable receipt. */
public final class JdbcPrivilegeMutationRepository implements PrivilegeMutationRepository {
    private static final int MAX_PRINCIPAL_LENGTH = 255;

    private final DataSource dataSource;

    public JdbcPrivilegeMutationRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public java.util.Optional<PrivilegeMutationReceipt> findReceipt(UserIdentity actor, java.util.UUID requestId) {
        Objects.requireNonNull(requestId, "requestId");
        requireActor(actor);
        try (var connection = dataSource.getConnection();
                var query = connection.prepareStatement("SELECT authorization_revision, changed_grants, recorded_at "
                        + "FROM security_privilege_mutations WHERE request_id=? AND principal=?")) {
            query.setObject(1, requestId);
            query.setString(2, actor.principal());
            try (var rows = query.executeQuery()) {
                if (!rows.next()) return java.util.Optional.empty();
                return java.util.Optional.of(new PrivilegeMutationReceipt(requestId, rows.getLong(1),
                        rows.getInt(2), rows.getTimestamp(3).toInstant(), true));
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed confirming privilege mutation " + requestId, failure);
        }
    }

    private static void requireActor(UserIdentity actor) {
        if (actor == null || actor.principal() == null || actor.principal().isBlank()
                || actor.principal().length() > MAX_PRINCIPAL_LENGTH) {
            throw new AuthorizationDeniedException("Authenticated privilege administrator is required");
        }
    }

    @Override
    public PrivilegeMutationReceipt apply(UserIdentity actor, PrivilegeMutation mutation) {
        Objects.requireNonNull(mutation, "mutation");
        requireActor(actor);
        byte[] encoded = PrivilegeMutationEncoding.encode(mutation);
        boolean committing = false;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try {
                try (var settings = connection.createStatement()) {
                    settings.execute("SET LOCAL lock_timeout = '5s'");
                    settings.execute("SET LOCAL statement_timeout = '30s'");
                }
                // Acquire before role/grant reads. Waiting for the gate must not freeze an old MVCC snapshot.
                AuthorizationTransactionGate.lockMutation(connection);
                var replay = PrivilegeMutationLedger.replay(connection, actor, mutation, encoded);
                PrivilegeMutationReceipt receipt;
                if (replay.isPresent()) {
                    receipt = replay.orElseThrow();
                } else {
                    var grants = GrantMutationAuthority.read(connection, actor);
                    var supporting = new EnumMap<Privilege, PrivilegeGrant>(Privilege.class);
                    for (var privilege : mutation.privileges()) {
                        supporting.put(privilege, GrantMutationAuthority.requireSupportingGrant(
                                grants, privilege, mutation.scope()));
                    }
                    GrantMutationAuthority.requireRoles(connection, mutation.roles());
                    int changed = 0;
                    if (mutation.operation() == PrivilegeMutation.Operation.GRANT) {
                        for (var privilege : mutation.privileges().stream().sorted().toList()) {
                            for (var role : mutation.roles().stream().sorted().toList()) {
                                changed += PrivilegeGrantRows.grant(connection, actor.principal(), role, privilege,
                                        mutation.scope(), mutation.withGrantOption(),
                                        Long.parseLong(supporting.get(privilege).id()));
                            }
                        }
                    } else {
                        changed = PrivilegeGrantRows.revoke(connection, actor.principal(), mutation,
                                GrantMutationAuthority.owns(grants, mutation.scope()));
                    }
                    receipt = PrivilegeMutationLedger.record(connection, actor, mutation, encoded, changed);
                }
                committing = true;
                connection.commit();
                return receipt;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            if (committing) throw new PrivilegeMutationOutcomeUnknownException(mutation.requestId(), failure);
            throw new IllegalStateException("Failed applying privilege mutation " + mutation.requestId(), failure);
        }
    }
}
