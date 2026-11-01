package org.pangrova.query.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.query.transaction.JdbcCopySourceReadAdmissions.FrozenInputAttempt;
import org.pangrova.query.transaction.JdbcCopySourceReadAdmissions.Scope;
import org.pangrova.query.transaction.JdbcCopySourceReadAdmissions.StagedSource;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Shared SQL, identity encoding and durability guards behind
 * {@link JdbcCopySourceReadAdmissions}. Every method is static and receives
 * all state explicitly; nothing is held here between calls.
 */
final class JdbcCopySourceReadAdmissionsMappings {
    static final ObjectMapper JSON = new ObjectMapper();
    static final TypeReference<Set<String>> GROUPS = new TypeReference<>() {};
    static final TypeReference<Map<String, String>> ATTRIBUTES = new TypeReference<>() {};
    static final int NONCE_BYTES = 32;
    static final int NONCE_CHARS = 43;
    static final int MAX_MANAGED_URI_BYTES = 2048;

    private JdbcCopySourceReadAdmissionsMappings() {}

    // Acquire a distinct issuance gate before admission row locks. Never upgrade
    // the shared authorization gate: concurrent issuers would deadlock each other.
    static void lockCopyIssuance(Connection connection, String tenant, String execution, String task)
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

    static String rootAdmissionHash(Connection connection, FrozenInputAttempt attempt) throws SQLException {
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

    static void requireCurrentFences(Connection connection, Scope scope,
                                     ExecutionAnchor expected) throws SQLException {
        if (expected == null || !scope.executionId().equals(expected.executionId())
                || !scope.ownerId().equals(expected.ownerId())
                || !scope.ownerIncarnation().equals(expected.ownerIncarnation())
                || scope.ownerFencingToken() != expected.fencingToken()) {
            throw denied();
        }
        JdbcExecutionAnchorStore.requireCurrent(connection, expected);
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

    static Admission requireAdmission(Connection connection, String nonceHash, Scope presented)
            throws SQLException {
        Admission admitted = lockAdmission(connection, nonceHash);
        if (admitted == null || !admitted.scope().equals(presented)
                || !Instant.ofEpochMilli(presented.deadlineEpochMs()).isAfter(databaseNow(connection))
                || admitted.authorizationRevision() != authorizationRevision(connection)) {
            throw denied();
        }
        return admitted;
    }

    static void requireUnissuedAttempt(Connection connection, Scope scope) throws SQLException {
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

    static void requireTenantMembership(Connection connection, Scope scope) throws SQLException {
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

    static Admission lockAdmission(Connection connection, String hash) throws SQLException {
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
                if (!JdbcCopySourceReadAdmissions.commandSha256(scope)
                        .equals(rows.getString("staging_command_sha256"))) {
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

    static void insert(Connection connection, String nonceHash, Scope scope,
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
            statement.setString(parameter++, JdbcCopySourceReadAdmissions.commandSha256(scope));
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

    static Instant databaseNow(Connection connection) throws SQLException {
        try (var query = connection.prepareStatement("SELECT clock_timestamp()");
                var rows = query.executeQuery()) {
            rows.next();
            return rows.getTimestamp(1).toInstant();
        }
    }

    static long authorizationRevision(Connection connection) throws SQLException {
        try (var query = connection.prepareStatement(
                "SELECT revision FROM pangrova_authorization_revision WHERE singleton=TRUE");
                var rows = query.executeQuery()) {
            if (!rows.next() || rows.getLong(1) <= 0) {
                throw new IllegalStateException("COPY authorization revision is unavailable");
            }
            return rows.getLong(1);
        }
    }

    static String encodeJson(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("COPY source identity cannot be encoded", failure);
        }
    }

    static Set<String> decodeGroups(String json) {
        try {
            return JSON.readValue(json, GROUPS);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Stored COPY source groups are invalid", failure);
        }
    }

    static Map<String, String> decodeAttributes(String json) {
        try {
            return JSON.readValue(json, ATTRIBUTES);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Stored COPY source attributes are invalid", failure);
        }
    }

    static byte[] decodeNonce(String nonce) {
        if (nonce == null || nonce.length() != NONCE_CHARS) {
            throw new IllegalArgumentException("COPY source ticket is not canonical");
        }
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(nonce);
            if (bytes.length != NONCE_BYTES || !nonce.equals(
                    Base64.getUrlEncoder().withoutPadding().encodeToString(bytes))) {
                throw new IllegalArgumentException("COPY source ticket is not canonical");
            }
            return bytes;
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("COPY source ticket is not canonical", failure);
        }
    }

    static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    static boolean isSha256(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    static boolean isManagedObjectUri(String value, String scope, String checksum) {
        if (value == null || value.getBytes(StandardCharsets.UTF_8).length > MAX_MANAGED_URI_BYTES) {
            return false;
        }
        try {
            URI uri = new URI(value);
            String key = uri.getRawPath();
            String suffix = "/tenants/" + scope + "/sha256/"
                    + checksum.substring(0, 2) + "/" + checksum;
            return isCanonicalManagedS3Uri(uri)
                    && key != null && safeManagedKey(key) && key.endsWith(suffix)
                    && value.equals("s3://" + uri.getRawAuthority() + key);
        } catch (URISyntaxException | IllegalArgumentException failure) {
            return false;
        }
    }

    static boolean isManagedStorageRootUri(String value) {
        if (value == null || value.getBytes(StandardCharsets.UTF_8).length > 1024
                || value.endsWith("/")) {
            return false;
        }
        try {
            URI uri = new URI(value);
            String path = uri.getRawPath();
            return isCanonicalManagedS3Uri(uri)
                    && (path == null || path.isEmpty() || safeManagedKey(path))
                    && value.equals("s3://" + uri.getRawAuthority() + (path == null ? "" : path));
        } catch (URISyntaxException | IllegalArgumentException failure) {
            return false;
        }
    }

    static String expectedManagedObjectUri(Scope scope, StagedSource staged) {
        return scope.managedStorageRootUri() + "/tenants/" + staged.tenantScope()
                + "/sha256/" + staged.contentSha256().substring(0, 2)
                + "/" + staged.contentSha256();
    }

    private static boolean isCanonicalManagedS3Uri(URI uri) {
        return "s3".equals(uri.getScheme()) && !uri.isOpaque()
                && uri.getRawAuthority() != null && !uri.getRawAuthority().isBlank()
                && uri.getHost() != null && uri.getRawAuthority().equals(uri.getHost())
                && uri.getRawUserInfo() == null && uri.getPort() == -1
                && uri.getRawQuery() == null && uri.getRawFragment() == null;
    }

    private static boolean safeManagedKey(String key) {
        if (!key.startsWith("/") || key.indexOf('%') >= 0 || key.indexOf('\\') >= 0) {
            return false;
        }
        for (String segment : key.substring(1).split("/", -1)) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    static String expectedTenantScope(String tenantId) {
        return sha256(("copy\0" + tenantId).getBytes(StandardCharsets.UTF_8));
    }

    static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("COPY source " + name + " is required");
        }
    }

    static AccessDeniedException denied() {
        return new AccessDeniedException("COPY source read admission is absent, revoked, expired or mismatched");
    }

    static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    record Admission(Scope scope, UserIdentity identity,
                     long authorizationRevision, boolean redeemed,
                     Optional<StagedSource> stagedSource) {}
}
