package org.pangrova.query.transaction;

import org.pangrova.metadata.api.error.MetastoreException;
import org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.Admission;
import org.pangrova.security.storage.store.JdbcStorageProfileRegistry;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.security.StorageProfileAccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.CopySourceEndpointPolicy;
import org.pangrova.spi.storage.CopySourceStagingDigest;
import org.pangrova.spi.storage.StorageProfileRevision;
import org.pangrova.spi.storage.StorageType;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.NONCE_BYTES;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.authorizationRevision;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.databaseNow;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.decodeNonce;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.denied;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.expectedManagedObjectUri;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.expectedTenantScope;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.insert;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.isManagedObjectUri;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.isManagedStorageRootUri;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.isSha256;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.lockAdmission;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.lockCopyIssuance;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.requireAdmission;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.requireCurrentFences;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.requireTenantMembership;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.requireText;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.requireUnissuedAttempt;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.rollback;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.rootAdmissionHash;
import static org.pangrova.query.transaction.JdbcCopySourceReadAdmissionsMappings.sha256;

/**
 * Durable Coordinator authority for one exact COPY source read. The random
 * bearer nonce is never persisted or derived from the internal-service token.
 * Both issue and redemption recheck the current profile and READ permission
 * while holding the same PostgreSQL authority mutation gate. All SQL, identity
 * encoding and digest helpers live in {@link JdbcCopySourceReadAdmissionsMappings}.
 */
public final class JdbcCopySourceReadAdmissions {
    private static final int MAX_IDENTITY_GROUPS = 256;
    private static final int MAX_IDENTITY_ATTRIBUTES = 256;

    private final DataSource dataSource;
    private final JdbcStorageProfileRegistry profiles;
    private final StorageProfileAccessControl accessControl;
    private final boolean allowLoopbackSourceTest;
    private final SecureRandom random = new SecureRandom();

    public JdbcCopySourceReadAdmissions(DataSource dataSource,
                                        StorageProfileAccessControl accessControl) {
        this(dataSource, accessControl, false);
    }

    /** Local MinIO integration tests only; production issuance requires HTTPS. */
    public static JdbcCopySourceReadAdmissions forLoopbackTest(
            DataSource dataSource, StorageProfileAccessControl accessControl) {
        return new JdbcCopySourceReadAdmissions(dataSource, accessControl, true);
    }

    private JdbcCopySourceReadAdmissions(DataSource dataSource,
                                         StorageProfileAccessControl accessControl,
                                         boolean allowLoopbackSourceTest) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.profiles = new JdbcStorageProfileRegistry(dataSource);
        this.accessControl = Objects.requireNonNull(accessControl, "accessControl");
        this.allowLoopbackSourceTest = allowLoopbackSourceTest;
    }

    /** The nonce itself is a bearer capability and must only travel in a private task payload. */
    public record Ticket(String nonce) {
        public Ticket {
            decodeNonce(nonce);
        }

        @Override public String toString() {
            return "CopySourceReadTicket[REDACTED]";
        }
    }

    /** Exact staging command, issued before the later Native TaskRequest exists. */
    public record Scope(String tenantId, String principal, String profileName,
                        String profileIncarnationId, long profileRevision,
                        String canonicalSourceUrl, String managedStorageRootUri,
                        String managedBackendIdentitySha256,
                        String executionId, String stagingAttemptId,
                        String ownerId, String ownerIncarnation, long controlEpoch,
                        long ownerFencingToken, long deadlineEpochMs) {
        public Scope {
            requireText(tenantId, "tenant");
            requireText(principal, "principal");
            requireText(profileName, "profile");
            UUID.fromString(profileIncarnationId);
            requireText(canonicalSourceUrl, "source URL");
            if (!isManagedStorageRootUri(managedStorageRootUri)) {
                throw new IllegalArgumentException("COPY managed storage root is not canonical");
            }
            if (!isSha256(managedBackendIdentitySha256)) {
                throw new IllegalArgumentException("COPY managed backend identity is incomplete");
            }
            requireText(executionId, "execution");
            requireText(stagingAttemptId, "staging attempt");
            requireText(ownerId, "owner");
            requireText(ownerIncarnation, "owner incarnation");
            if (profileRevision <= 0 || controlEpoch <= 0 || ownerFencingToken <= 0
                    || deadlineEpochMs <= 0) {
                throw new IllegalArgumentException("COPY source staging authority is incomplete");
            }
        }
    }

    /** Verified exact S3 version and managed content identity; no source credentials. */
    public record StagedSource(String sourceVersionId, String managedObjectUri, String contentSha256,
                               long contentSize, String tenantScope) {
        public StagedSource {
            if (sourceVersionId == null || sourceVersionId.isBlank()
                    || "null".equals(sourceVersionId)
                    || sourceVersionId.getBytes(StandardCharsets.UTF_8).length > 1024
                    || !isSha256(contentSha256) || contentSize < 0 || !isSha256(tenantScope)
                    || !isManagedObjectUri(managedObjectUri, tenantScope, contentSha256)) {
                throw new IllegalArgumentException("COPY staged source proof is incomplete");
            }
        }
    }

    /** A repeated exact attempt can reuse the durable staged proof without re-reading S3. */
    public record Redemption(StorageProfileRevision profile, Optional<StagedSource> stagedSource) {
        public Redemption {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(stagedSource, "stagedSource");
        }
    }

    /**
     * Issues only after a current READ check. The authority gate serializes
     * this check with grant, role, and profile mutations, including direct SQL.
     */
    public Ticket issue(Scope scope, UserIdentity identity, ExecutionAnchor expectedAnchor) {
        Objects.requireNonNull(scope, "scope");
        validateIdentity(scope, identity);
        byte[] nonceBytes = new byte[NONCE_BYTES];
        random.nextBytes(nonceBytes);
        String nonce = Base64.getUrlEncoder().withoutPadding().encodeToString(nonceBytes);
        String nonceHash = sha256(nonceBytes);
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
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to issue COPY source read admission", failure);
        } finally {
            java.util.Arrays.fill(nonceBytes, (byte) 0);
        }
    }

    /** Final execution identity; source/profile/content are loaded only from the durable root proof. */
    public record FrozenInputAttempt(String tenantId, String principal, String executionId,
                                     String sourceAttemptId, String taskId, long controlEpoch,
                                     long deadlineEpochMs) {
        public FrozenInputAttempt {
            requireText(tenantId, "tenant");
            requireText(principal, "principal");
            requireText(executionId, "execution");
            requireText(sourceAttemptId, "source attempt");
            requireText(taskId, "task");
        }
    }

    /** Fresh private capability and its immutable inherited input. */
    public record FrozenInput(Ticket ticket, Scope scope, StagedSource proof) {}

    /**
     * Atomically derives a final task's capability from an already staged root.
     * No source GET, caller-provided proof, old bearer nonce or old owner is needed.
     * Current authorization must still be the root's exact authority revision.
     * Repeated delivery may issue another nonce only for the same frozen scope;
     * the Worker still performs the first redemption of each derived nonce.
     */
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
                insert(connection, sha256(nonceBytes), scope, root.identity(), root.authorizationRevision(),
                        parentHash, proof);
                requireCurrentFences(connection, scope, expectedAnchor);
                connection.commit();
                return new FrozenInput(new Ticket(nonce), scope, proof);
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to bind frozen COPY input", failure);
        } finally {
            java.util.Arrays.fill(nonceBytes, (byte) 0);
        }
    }

    /**
     * Atomically redeems a nonce for its exact admitted task. A repeat by the
     * same task is idempotent; another task, source, revision or epoch is denied.
     */
    public Redemption redeem(Ticket ticket, Scope presented, ExecutionAnchor expectedAnchor) {
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(presented, "presented");
        String nonceHash = sha256(decodeNonce(ticket.nonce()));
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
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to redeem COPY source read admission", failure);
        }
    }

    /**
     * Persists the result of an exact-version GET and managed-object readback.
     * Conflicting second results are rejected instead of re-pinning a changed URL.
     */
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
        String nonceHash = sha256(decodeNonce(ticket.nonce()));
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
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to record COPY staged source", failure);
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
                        JdbcCopySourceReadAdmissionsMappings::denied);
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

    private static void validateIdentity(Scope scope, UserIdentity identity) {
        if (identity == null || !scope.principal().equals(identity.principal())
                || identity.authenticatedAt() == null || identity.groups().size() > MAX_IDENTITY_GROUPS
                || identity.attributes().size() > MAX_IDENTITY_ATTRIBUTES) {
            throw denied();
        }
    }

    /** Length-prefixed, versioned command encoding excludes the bearer nonce. */
    public static String commandSha256(Scope scope) {
        Objects.requireNonNull(scope, "scope");
        return CopySourceStagingDigest.sha256(new CopySourceStagingDigest.Scope(
                scope.tenantId(), scope.principal(), scope.profileName(),
                scope.profileIncarnationId(), scope.profileRevision(), scope.canonicalSourceUrl(),
                scope.managedStorageRootUri(), scope.managedBackendIdentitySha256(),
                scope.executionId(), scope.stagingAttemptId(),
                scope.ownerId(), scope.ownerIncarnation(), scope.controlEpoch(),
                scope.ownerFencingToken(), scope.deadlineEpochMs()));
    }
}
