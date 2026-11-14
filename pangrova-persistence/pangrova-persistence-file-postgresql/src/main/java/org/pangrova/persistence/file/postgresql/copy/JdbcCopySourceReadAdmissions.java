package org.pangrova.persistence.file.postgresql.copy;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.common.util.DigestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions;
import org.pangrova.common.enums.StorageType;
import org.pangrova.file.api.copy.CopySourceEndpointPolicy;
import org.pangrova.persistence.jdbc.api.query.ExecutionTransactionFence;
import org.pangrova.file.api.profile.StorageProfileRevision;
import org.pangrova.file.api.exception.FileStorageException;
import org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.authorization.StorageProfileAccessControl;
import org.pangrova.security.api.identity.UserIdentity;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions.commandSha256;

/**
 * Durable Coordinator authority for one exact COPY source read. The random
 * bearer nonce is never persisted or derived from the internal-service token.
 * Both issue and redemption recheck the current profile and READ permission
 * while holding the same PostgreSQL authority mutation gate.
 */
public final class JdbcCopySourceReadAdmissions implements CopySourceReadAdmissions {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Set<String>> GROUPS = new TypeReference<>() {};
    private static final TypeReference<Map<String, String>> ATTRIBUTES = new TypeReference<>() {};
    private static final int NONCE_BYTES = 32;
    private static final int MAX_IDENTITY_GROUPS = 256;
    private static final int MAX_IDENTITY_ATTRIBUTES = 256;

    private final DataSource dataSource;
    private final JdbcStorageProfileRegistry profiles;
    private final StorageProfileAccessControl accessControl;
    private final ExecutionTransactionFence anchorFence;
    private final boolean allowLoopbackSourceTest;
    private final SecureRandom random = new SecureRandom();

    public JdbcCopySourceReadAdmissions(DataSource dataSource,
                                        StorageProfileAccessControl accessControl,
                                        ExecutionTransactionFence anchorFence) {
        this(dataSource, accessControl, anchorFence, false);
    }

    /** Local MinIO integration tests only; production issuance requires HTTPS. */
    public static JdbcCopySourceReadAdmissions forLoopbackTest(
            DataSource dataSource, StorageProfileAccessControl accessControl,
            ExecutionTransactionFence anchorFence) {
        return new JdbcCopySourceReadAdmissions(dataSource, accessControl, anchorFence, true);
    }

    private JdbcCopySourceReadAdmissions(DataSource dataSource,
                                         StorageProfileAccessControl accessControl,
                                         ExecutionTransactionFence anchorFence,
                                         boolean allowLoopbackSourceTest) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.profiles = new JdbcStorageProfileRegistry(dataSource);
        this.accessControl = Objects.requireNonNull(accessControl, "accessControl");
        this.anchorFence = Objects.requireNonNull(anchorFence, "anchorFence");
        this.allowLoopbackSourceTest = allowLoopbackSourceTest;
    }

    /**
     * Issues only after a current READ check. The authority gate serializes
     * this check with grant, role, and profile mutations, including direct SQL.
     */
    @Override
    public Ticket issue(Scope scope, UserIdentity identity, ExecutionAnchor expectedAnchor) {
        Objects.requireNonNull(scope, "scope");
        validateIdentity(scope, identity);
        byte[] nonceBytes = new byte[NONCE_BYTES];
        random.nextBytes(nonceBytes);
        String nonce = Base64.getUrlEncoder().withoutPadding().encodeToString(nonceBytes);
        String nonceHash = DigestUtils.sha256Hex(nonceBytes);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                lockCopyIssuance(connection, scope.tenantId(), scope.executionId(), scope.stagingAttemptId());
                requireCurrentFences(connection, scope, expectedAnchor);
                requireTenantMembership(connection, scope);
                StorageProfileRevision profile = currentProfile(connection, scope);
                accessControl.checkCanReadStorageProfile(identity, profile.profile().name());
                if (!Instant.ofEpochMilli(scope.deadlineEpochMs()).isAfter(databaseNow(connection))) {
                    throw denied();
                }
                requireUnissuedAttempt(connection, scope);
                insert(connection, nonceHash, scope, identity, authorizationRevision(connection), null, null);
                requireCurrentFences(connection, scope, expectedAnchor);
                connection.commit();
                return new Ticket(nonce);
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to issue COPY source read admission", failure);
        } finally {
            java.util.Arrays.fill(nonceBytes, (byte) 0);
        }
    }

    /**
     * Atomically derives a final task's capability from an already staged root.
     * No source GET, caller-provided proof, old bearer nonce or old owner is needed.
     * Current authorization must still be the root's exact authority revision.
     * Repeated delivery may issue another nonce only for the same frozen scope;
     * the Worker still performs the first redemption of each derived nonce.
     */
    @Override
    public FrozenInput issueFrozenInput(FrozenInputAttempt attempt, ExecutionAnchor expectedAnchor) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(expectedAnchor, "expectedAnchor");
        byte[] nonceBytes = new byte[NONCE_BYTES];
        random.nextBytes(nonceBytes);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                lockCopyIssuance(connection, attempt.tenantId(), attempt.executionId(), attempt.taskId());
                String parentHash = rootAdmissionHash(connection, attempt);
                Admission root = lockAdmission(connection, parentHash);
                if (root == null || !root.redeemed() || root.stagedSource().isEmpty()
                        || !root.scope().principal().equals(attempt.principal())
                        || root.authorizationRevision() != authorizationRevision(connection)) {
                    throw denied();
                }
                Scope original = root.scope();
                Scope scope = new Scope(original.tenantId(), original.principal(), original.profileName(),
                        original.profileIncarnationId(), original.profileRevision(), original.canonicalSourceUrl(),
                        original.managedStorageRootUri(), original.managedBackendIdentitySha256(),
                        attempt.executionId(), attempt.taskId(), expectedAnchor.ownerId(),
                        expectedAnchor.ownerIncarnation(), attempt.controlEpoch(), expectedAnchor.fencingToken(),
                        attempt.deadlineEpochMs());
                requireCurrentFences(connection, scope, expectedAnchor);
                requireTenantMembership(connection, scope);
                StorageProfileRevision current = currentProfile(connection, scope);
                accessControl.checkCanReadStorageProfile(root.identity(), current.profile().name());
                String nonce = Base64.getUrlEncoder().withoutPadding().encodeToString(nonceBytes);
                StagedSource proof = root.stagedSource().orElseThrow();
                insert(connection, DigestUtils.sha256Hex(nonceBytes), scope, root.identity(), root.authorizationRevision(),
                        parentHash, proof);
                requireCurrentFences(connection, scope, expectedAnchor);
                connection.commit();
                return new FrozenInput(new Ticket(nonce), scope, proof);
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to bind frozen COPY input", failure);
        } finally {
            java.util.Arrays.fill(nonceBytes, (byte) 0);
        }
    }

    // Acquire a distinct issuance gate before admission row locks. Never upgrade
    // the shared authorization gate: concurrent issuers would deadlock each other.
    private static void lockCopyIssuance(Connection connection, String tenant, String execution, String task)
            throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT pg_advisory_xact_lock(207578818,
                    hashtext(json_build_array(?::text, ?::text, ?::text)::text))
                """)) {
            statement.setString(1, tenant);
            statement.setString(2, execution);
            statement.setString(3, task);
            statement.execute();
        }
    }

    private static String rootAdmissionHash(Connection connection, FrozenInputAttempt attempt) throws SQLException {
        try (var query = connection.prepareStatement("""
                SELECT nonce_sha256 FROM copy_source_read_admissions
                WHERE tenant_id=? AND execution_id=? AND staging_attempt_id=? AND parent_nonce_sha256 IS NULL
                FOR UPDATE
                """)) {
            query.setString(1, attempt.tenantId());
            query.setString(2, attempt.executionId());
            query.setString(3, attempt.sourceAttemptId());
            try (var rows = query.executeQuery()) {
                if (!rows.next()) throw denied();
                return rows.getString(1);
            }
        }
    }

    /**
     * Atomically redeems a nonce for its exact admitted task. A repeat by the
     * same task is idempotent; another task, source, revision or epoch is denied.
     */
    @Override
    public Redemption redeem(Ticket ticket, Scope presented, ExecutionAnchor expectedAnchor) {
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(presented, "presented");
        String nonceHash = DigestUtils.sha256Hex(Base64.getUrlDecoder().decode(ticket.nonce()));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                requireCurrentFences(connection, presented, expectedAnchor);
                Admission admitted = requireAdmission(connection, nonceHash, presented);
                requireTenantMembership(connection, admitted.scope());
                StorageProfileRevision current = currentProfile(connection, admitted.scope());
                accessControl.checkCanReadStorageProfile(admitted.identity(), current.profile().name());
                if (!admitted.redeemed()) {
                    try (var update = connection.prepareStatement(
                            "UPDATE copy_source_read_admissions SET redeemed_at=clock_timestamp() "
                                    + "WHERE nonce_sha256=? AND redeemed_at IS NULL")) {
                        update.setString(1, nonceHash);
                        if (update.executeUpdate() != 1) {
                            throw new IllegalStateException("COPY source admission redemption lost its row lock");
                        }
                    }
                }
                requireCurrentFences(connection, presented, expectedAnchor);
                connection.commit();
                return new Redemption(current, admitted.stagedSource());
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to redeem COPY source read admission", failure);
        }
    }

    /**
     * Persists the result of an exact-version GET and managed-object readback.
     * Conflicting second results are rejected instead of re-pinning a changed URL.
     */
    @Override
    public StagedSource recordStagedSource(Ticket ticket, Scope presented,
                                          StagedSource staged, ExecutionAnchor expectedAnchor) {
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(presented, "presented");
        Objects.requireNonNull(staged, "staged");
        if (!expectedTenantScope(presented.tenantId()).equals(staged.tenantScope())) {
            throw denied();
        }
        if (!expectedManagedObjectUri(presented, staged).equals(staged.managedObjectUri())) {
            throw denied();
        }
        String nonceHash = DigestUtils.sha256Hex(Base64.getUrlDecoder().decode(ticket.nonce()));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                requireCurrentFences(connection, presented, expectedAnchor);
                Admission admitted = requireAdmission(connection, nonceHash, presented);
                requireTenantMembership(connection, admitted.scope());
                StorageProfileRevision current = currentProfile(connection, admitted.scope());
                accessControl.checkCanReadStorageProfile(admitted.identity(), current.profile().name());
                if (!admitted.redeemed()) {
                    throw denied();
                }
                if (admitted.stagedSource().isPresent()) {
                    if (!admitted.stagedSource().orElseThrow().equals(staged)) {
                        throw new IllegalStateException("COPY source staged proof conflicts with an earlier result");
                    }
                } else {
                    try (var update = connection.prepareStatement("""
                            UPDATE copy_source_read_admissions
                            SET staged_source_version_id=?, staged_managed_object_uri=?,
                                staged_content_sha256=?, staged_content_size=?,
                                staged_tenant_scope=?, staged_at=clock_timestamp()
                            WHERE nonce_sha256=? AND redeemed_at IS NOT NULL AND staged_at IS NULL
                            """)) {
                        update.setString(1, staged.sourceVersionId());
                        update.setString(2, staged.managedObjectUri());
                        update.setString(3, staged.contentSha256());
                        update.setLong(4, staged.contentSize());
                        update.setString(5, staged.tenantScope());
                        update.setString(6, nonceHash);
                        if (update.executeUpdate() != 1) {
                            throw new IllegalStateException("COPY source staged proof lost its row lock");
                        }
                    }
                }
                requireCurrentFences(connection, presented, expectedAnchor);
                connection.commit();
                return staged;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to record COPY staged source", failure);
        }
    }

    private void requireCurrentFences(Connection connection, Scope scope,
                                      ExecutionAnchor expected) throws SQLException {
        if (expected == null || !scope.executionId().equals(expected.executionId())
                || !scope.ownerId().equals(expected.ownerId())
                || !scope.ownerIncarnation().equals(expected.ownerIncarnation())
                || scope.ownerFencingToken() != expected.fencingToken()) {
            throw denied();
        }
        anchorFence.requireCurrent(connection, expected.executionId(), expected.ownerId(),
                expected.ownerIncarnation(), expected.fencingToken());
        // This lock serializes the observed control term with a real leader
        // takeover. The Worker TaskRequest epoch fence does not protect the
        // earlier source staging command.
        Instant leaseExpiresAt;
        try (var query = connection.prepareStatement(
                "SELECT epoch, expires_at FROM distributed_lease "
                        + "WHERE lease_key='coordinator_leader' FOR SHARE");
                var rows = query.executeQuery()) {
            if (!rows.next() || rows.getLong("epoch") != scope.controlEpoch()) {
                throw denied();
            }
            leaseExpiresAt = rows.getTimestamp("expires_at").toInstant();
        }
        Instant now = databaseNow(connection);
        if (!leaseExpiresAt.isAfter(now)
                || !Instant.ofEpochMilli(scope.deadlineEpochMs()).isAfter(now)) {
            throw denied();
        }
    }

    private static Admission requireAdmission(Connection connection, String nonceHash, Scope presented)
            throws SQLException {
        Admission admitted = lockAdmission(connection, nonceHash);
        if (admitted == null || !admitted.scope().equals(presented)
                || !Instant.ofEpochMilli(presented.deadlineEpochMs()).isAfter(databaseNow(connection))
                || admitted.authorizationRevision() != authorizationRevision(connection)) {
            throw denied();
        }
        return admitted;
    }

    private static void requireUnissuedAttempt(Connection connection, Scope scope) throws SQLException {
        try (var query = connection.prepareStatement("""
                SELECT 1 FROM copy_source_read_admissions
                WHERE tenant_id=? AND execution_id=? AND staging_attempt_id=?
                FOR UPDATE
                """)) {
            query.setString(1, scope.tenantId());
            query.setString(2, scope.executionId());
            query.setString(3, scope.stagingAttemptId());
            try (var rows = query.executeQuery()) {
                if (rows.next()) {
                    throw new IllegalStateException("COPY source staging attempt already has a private ticket");
                }
            }
        }
    }

    private StorageProfileRevision currentProfile(Connection connection, Scope scope) throws SQLException {
        try (var query = connection.prepareStatement(
                "SELECT incarnation_id, revision FROM storage_profile_definitions "
                        + "WHERE name=? FOR KEY SHARE")) {
            query.setString(1, scope.profileName());
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next() || !scope.profileIncarnationId().equals(
                        rows.getObject("incarnation_id", UUID.class).toString())
                        || scope.profileRevision() != rows.getLong("revision")) {
                    throw denied();
                }
            }
        }
        // Historical revision is immutable; the current row is locked through commit.
        StorageProfileRevision profile = profiles.findRevision(scope.profileName(),
                scope.profileIncarnationId(), scope.profileRevision()).orElseThrow(
                        JdbcCopySourceReadAdmissions::denied);
        if (profile.profile().type() != StorageType.S3
                || !scope.canonicalSourceUrl().startsWith("s3://")
                || !profile.profile().matchesUrl(scope.canonicalSourceUrl())) {
            throw denied();
        }
        try {
            CopySourceEndpointPolicy.require(profile.profile().endpoint(), allowLoopbackSourceTest);
        } catch (IllegalArgumentException invalidEndpoint) {
            throw denied();
        }
        return profile;
    }

    private static void requireTenantMembership(Connection connection, Scope scope) throws SQLException {
        // The production TenantResolver uses this metadata authority. Keeping
        // the observation in this transaction prevents a Worker from choosing
        // a different tenant than the authenticated SQL principal belongs to.
        try (var query = connection.prepareStatement("""
                SELECT 1 FROM tenant_user_memberships membership
                JOIN tenants tenant ON tenant.tenant_id = membership.tenant_id
                WHERE membership.user_principal=? AND membership.tenant_id=?
                    AND tenant.status='ACTIVE'
                FOR KEY SHARE OF membership, tenant
                """)) {
            query.setString(1, scope.principal());
            query.setString(2, scope.tenantId());
            try (var rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw denied();
                }
            }
        }
    }

    private static Admission lockAdmission(Connection connection, String hash) throws SQLException {
        try (var query = connection.prepareStatement(
                "SELECT * FROM copy_source_read_admissions WHERE nonce_sha256=? FOR UPDATE")) {
            query.setString(1, hash);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) return null;
                Scope scope = new Scope(rows.getString("tenant_id"), rows.getString("principal"),
                        rows.getString("profile_name"), rows.getObject("profile_incarnation_id", UUID.class).toString(),
                        rows.getLong("profile_revision"), rows.getString("canonical_source_url"),
                        rows.getString("managed_storage_root_uri"),
                        rows.getString("managed_backend_identity_sha256"),
                        rows.getString("execution_id"), rows.getString("staging_attempt_id"),
                        rows.getString("owner_id"), rows.getString("owner_incarnation"),
                        rows.getLong("control_epoch"), rows.getLong("owner_fencing_token"),
                        rows.getTimestamp("deadline_at").getTime());
                if (!commandSha256(scope).equals(rows.getString("staging_command_sha256"))) {
                    throw new IllegalStateException("Stored COPY staging command digest is invalid");
                }
                UserIdentity identity = new UserIdentity(scope.principal(),
                        decodeGroups(rows.getString("identity_groups")),
                        decodeAttributes(rows.getString("identity_attributes")),
                        rows.getTimestamp("authenticated_at").toInstant());
                String version = rows.getString("staged_source_version_id");
                Optional<StagedSource> staged = version == null ? Optional.empty()
                        : Optional.of(new StagedSource(version, rows.getString("staged_managed_object_uri"),
                        rows.getString("staged_content_sha256"),
                        rows.getLong("staged_content_size"), rows.getString("staged_tenant_scope")));
                return new Admission(scope, identity, rows.getLong("authorization_revision"),
                        rows.getTimestamp("redeemed_at") != null, staged);
            }
        }
    }

    private static void insert(Connection connection, String nonceHash, Scope scope,
                               UserIdentity identity, long authorizationRevision, String parentHash,
                               StagedSource proof) throws SQLException {
        try (var statement = connection.prepareStatement("""
                INSERT INTO copy_source_read_admissions (
                    nonce_sha256, tenant_id, principal, identity_groups, identity_attributes,
                    authenticated_at, authorization_revision, profile_name, profile_incarnation_id, profile_revision,
                    canonical_source_url, managed_storage_root_uri,
                    managed_backend_identity_sha256,
                    execution_id, staging_attempt_id, staging_command_sha256,
                    owner_id, owner_incarnation, control_epoch, owner_fencing_token, deadline_at,
                    parent_nonce_sha256, staged_source_version_id, staged_managed_object_uri,
                    staged_content_sha256, staged_content_size, staged_tenant_scope, staged_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, CASE WHEN ? THEN clock_timestamp() ELSE NULL END)
                """)) {
            int parameter = 1;
            statement.setString(parameter++, nonceHash);
            statement.setString(parameter++, scope.tenantId());
            statement.setString(parameter++, scope.principal());
            statement.setObject(parameter++, inheritedGroups(connection, identity, parentHash), Types.OTHER);
            statement.setObject(parameter++, encodeJson(identity.attributes()), Types.OTHER);
            statement.setTimestamp(parameter++, Timestamp.from(identity.authenticatedAt()));
            statement.setLong(parameter++, authorizationRevision);
            statement.setString(parameter++, scope.profileName());
            statement.setObject(parameter++, UUID.fromString(scope.profileIncarnationId()));
            statement.setLong(parameter++, scope.profileRevision());
            statement.setString(parameter++, scope.canonicalSourceUrl());
            statement.setString(parameter++, scope.managedStorageRootUri());
            statement.setString(parameter++, scope.managedBackendIdentitySha256());
            statement.setString(parameter++, scope.executionId());
            statement.setString(parameter++, scope.stagingAttemptId());
            statement.setString(parameter++, commandSha256(scope));
            statement.setString(parameter++, scope.ownerId());
            statement.setString(parameter++, scope.ownerIncarnation());
            statement.setLong(parameter++, scope.controlEpoch());
            statement.setLong(parameter++, scope.ownerFencingToken());
            statement.setTimestamp(parameter++, new Timestamp(scope.deadlineEpochMs()));
            statement.setString(parameter++, parentHash);
            statement.setString(parameter++, proof == null ? null : proof.sourceVersionId());
            statement.setString(parameter++, proof == null ? null : proof.managedObjectUri());
            statement.setString(parameter++, proof == null ? null : proof.contentSha256());
            if (proof == null) statement.setNull(parameter++, Types.BIGINT);
            else statement.setLong(parameter++, proof.contentSize());
            statement.setString(parameter++, proof == null ? null : proof.tenantScope());
            statement.setBoolean(parameter, proof != null);
            statement.executeUpdate();
        }
    }

    private static String inheritedGroups(Connection connection, UserIdentity identity, String parentHash)
            throws SQLException {
        if (parentHash == null) return encodeJson(identity.groups());
        // UserIdentity groups are a set. Re-encoding after JVM restart can reorder
        // its JSON array, so inherit the root's exact persisted declaration.
        try (var query = connection.prepareStatement(
                "SELECT identity_groups::text FROM copy_source_read_admissions WHERE nonce_sha256=?")) {
            query.setString(1, parentHash);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) throw denied();
                return rows.getString(1);
            }
        }
    }

    private static Instant databaseNow(Connection connection) throws SQLException {
        try (var query = connection.prepareStatement("SELECT clock_timestamp()");
                var rows = query.executeQuery()) {
            rows.next();
            return rows.getTimestamp(1).toInstant();
        }
    }

    private static long authorizationRevision(Connection connection) throws SQLException {
        try (var query = connection.prepareStatement(
                "SELECT revision FROM pangrova_authorization_revision WHERE singleton=TRUE");
                var rows = query.executeQuery()) {
            if (!rows.next() || rows.getLong(1) <= 0) {
                throw new IllegalStateException("COPY authorization revision is unavailable");
            }
            return rows.getLong(1);
        }
    }

    private static void validateIdentity(Scope scope, UserIdentity identity) {
        if (identity == null || !scope.principal().equals(identity.principal())
                || identity.authenticatedAt() == null || identity.groups().size() > MAX_IDENTITY_GROUPS
                || identity.attributes().size() > MAX_IDENTITY_ATTRIBUTES) {
            throw denied();
        }
    }

    private static String encodeJson(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("COPY source identity cannot be encoded", failure);
        }
    }

    private static Set<String> decodeGroups(String json) {
        try {
            return JSON.readValue(json, GROUPS);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Stored COPY source groups are invalid", failure);
        }
    }

    private static Map<String, String> decodeAttributes(String json) {
        try {
            return JSON.readValue(json, ATTRIBUTES);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Stored COPY source attributes are invalid", failure);
        }
    }

    private static String expectedManagedObjectUri(Scope scope, StagedSource staged) {
        return scope.managedStorageRootUri() + "/tenants/" + staged.tenantScope()
                + "/sha256/" + staged.contentSha256().substring(0, 2)
                + "/" + staged.contentSha256();
    }

    private static String expectedTenantScope(String tenantId) {
        return DigestUtils.sha256Hex(("copy\0" + tenantId).getBytes(StandardCharsets.UTF_8));
    }

    private static AccessDeniedException denied() {
        return new AccessDeniedException("COPY source read admission is absent, revoked, expired or mismatched");
    }

    private record Admission(Scope scope, UserIdentity identity,
                             long authorizationRevision, boolean redeemed,
                             Optional<StagedSource> stagedSource) {}
}
