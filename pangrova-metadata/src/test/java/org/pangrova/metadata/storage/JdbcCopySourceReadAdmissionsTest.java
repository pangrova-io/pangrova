package org.pangrova.metadata.storage;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.HexFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real PostgreSQL and persistent RBAC proof for one-task COPY source read admission. */
class JdbcCopySourceReadAdmissionsTest {
    private static final String SOURCE = "s3://bucket/data/file.parquet";
    private static final String MANAGED_ROOT = "s3://managed/copy-managed";
    private static final UserIdentity ALICE = new UserIdentity("alice", Set.of("team-a"),
            Map.of("region", "west"), Instant.parse("2026-10-16T00:00:00Z"));

    @Test
    void nonceIsOnlyPersistedAsHashAndExactTaskRetryIsIdempotent() throws Exception {
        var fixture = fixture("copy_source_admission_exact");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        assertThat(ticket.nonce()).hasSize(43);
        assertThat(ticket.toString()).doesNotContain(ticket.nonce());
        try (var connection = fixture.source().getConnection();
                var statement = connection.prepareStatement(
                        "SELECT nonce_sha256, redeemed_at FROM copy_source_read_admissions")) {
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.getString(1)).hasSize(64).isNotEqualTo(ticket.nonce());
                assertThat(row.getTimestamp(2)).isNull();
            }
        }
        assertThat(fixture.redeem(ticket, scope).profile().profile().bucket()).isEqualTo("bucket");
        assertThat(new JdbcCopySourceReadAdmissions(fixture.source(), fixture.accessControl())
                .redeem(ticket, scope, fixture.anchor()).profile().profile().bucket()).isEqualTo("bucket");
        try (var connection = fixture.source().getConnection();
                var statement = connection.prepareStatement(
                        "SELECT redeemed_at FROM copy_source_read_admissions")) {
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.getTimestamp(1)).isNotNull();
            }
        }
    }

    @Test
    void canonicalStagingCommandDigestExcludesNonceAndAttemptCannotBeIssuedTwice() throws Exception {
        var fixture = fixture("copy_source_admission_command");
        var scope = fixture.scope();
        var first = fixture.issue(scope, ALICE);
        assertThatThrownBy(() -> fixture.issue(scope, ALICE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("already has a private ticket");
        assertThat(first.nonce()).hasSize(43);
        assertThat(JdbcCopySourceReadAdmissions.commandSha256(scope)).hasSize(64);
        try (var connection = fixture.source().getConnection();
                var query = connection.prepareStatement("""
                        SELECT count(*), count(DISTINCT staging_command_sha256)
                        FROM copy_source_read_admissions
                        """); var rows = query.executeQuery()) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(1);
            assertThat(rows.getInt(2)).isEqualTo(1);
        }
    }

    @Test
    void everyScopeFieldAndUnknownNonceAreDenied() throws Exception {
        var fixture = fixture("copy_source_admission_scope");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        for (var changed : new JdbcCopySourceReadAdmissions.Scope[]{
                scopeWith(scope, "tenant-b", scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), "bob", scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), UUID.randomUUID().toString(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision() + 1, scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), "s3://bucket/data/other.parquet", scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), "other-execution",
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(), "other-attempt",
                        scope.ownerId(), scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), "other-owner", scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(), scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), "other-incarnation",
                        scope.controlEpoch(), scope.ownerFencingToken(), scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch() + 1, scope.ownerFencingToken(),
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken() + 1,
                        scope.deadlineEpochMs()),
                scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                        scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                        scope.controlEpoch(), scope.ownerFencingToken(),
                        scope.deadlineEpochMs() + 1)}) {
            assertThatThrownBy(() -> fixture.redeem(ticket, changed))
                    .isInstanceOf(AccessDeniedException.class);
        }
        var redirectedRoot = new JdbcCopySourceReadAdmissions.Scope(scope.tenantId(),
                scope.principal(), scope.profileName(), scope.profileIncarnationId(),
                scope.profileRevision(), scope.canonicalSourceUrl(), "s3://untrusted/copy-managed",
                scope.managedBackendIdentitySha256(),
                scope.executionId(), scope.stagingAttemptId(), scope.ownerId(),
                scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                scope.deadlineEpochMs());
        assertThatThrownBy(() -> fixture.redeem(ticket, redirectedRoot))
                .isInstanceOf(AccessDeniedException.class);
        var redirectedBackend = new JdbcCopySourceReadAdmissions.Scope(scope.tenantId(),
                scope.principal(), scope.profileName(), scope.profileIncarnationId(),
                scope.profileRevision(), scope.canonicalSourceUrl(), scope.managedStorageRootUri(),
                "b".repeat(64), scope.executionId(), scope.stagingAttemptId(), scope.ownerId(),
                scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                scope.deadlineEpochMs());
        assertThatThrownBy(() -> fixture.redeem(ticket, redirectedBackend))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> fixture.redeem(
                new JdbcCopySourceReadAdmissions.Ticket("A".repeat(43)), scope))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(fixture.redeem(ticket, scope).profile().profile().bucket()).isEqualTo("bucket");
    }

    @Test
    void issueRequiresRealReadGrantAndAuthenticatedPrincipal() throws Exception {
        var fixture = fixture("copy_source_admission_grant");
        var scope = fixture.scope();
        assertThatThrownBy(() -> fixture.issue(scope,
                new UserIdentity("bob", ALICE.groups(), ALICE.attributes(), ALICE.authenticatedAt())))
                .isInstanceOf(AccessDeniedException.class);
        var otherTenant = scopeWith(scope, "tenant-b", scope.principal(), scope.profileIncarnationId(),
                scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                scope.controlEpoch(), scope.ownerFencingToken(), scope.deadlineEpochMs());
        assertThatThrownBy(() -> fixture.issue(otherTenant, ALICE))
                .isInstanceOf(AccessDeniedException.class);
        fixture.grants().revokeGrants("reader", Privilege.READ_STORAGE_PROFILE, "incoming", null, null);
        assertThatThrownBy(() -> fixture.issue(scope, ALICE))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void issueRejectsNoncanonicalAndOutOfProfileSourceUrl() throws Exception {
        var fixture = fixture("copy_source_admission_url");
        var scope = fixture.scope();
        for (String url : new String[]{"S3://bucket/data/file.parquet",
                "s3://bucket/data/../file.parquet", "s3://bucket/data/%2e%2e/file.parquet",
                "s3://other/data/file.parquet", "s3://bucket/other/file.parquet",
                "s3://bucket/data/file.parquet?versionId=latest"}) {
            var changed = scopeWith(scope, scope.tenantId(), scope.principal(),
                    scope.profileIncarnationId(), scope.profileRevision(), url,
                    scope.executionId(), scope.stagingAttemptId(), scope.ownerId(),
                    scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                    scope.deadlineEpochMs());
            assertThatThrownBy(() -> fixture.issue(changed, ALICE))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    @Test
    void revokedReadAndRecreatedProfileBothFenceOutstandingTickets() throws Exception {
        var fixture = fixture("copy_source_admission_revoke");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        fixture.grants().revokeGrants("reader", Privilege.READ_STORAGE_PROFILE, "incoming", null, null);
        assertThatThrownBy(() -> fixture.redeem(ticket, scope))
                .isInstanceOf(AccessDeniedException.class);
        fixture.grants().addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        assertThatThrownBy(() -> fixture.redeem(ticket, scope))
                .isInstanceOf(AccessDeniedException.class);
        var nextScope = scopeWith(scope, scope.tenantId(), scope.principal(),
                scope.profileIncarnationId(), scope.profileRevision(), scope.canonicalSourceUrl(),
                scope.executionId(), "stage-two", scope.ownerId(), scope.ownerIncarnation(),
                scope.controlEpoch(), scope.ownerFencingToken(), scope.deadlineEpochMs());
        var next = fixture.issue(nextScope, ALICE);
        assertThat(fixture.redeem(next, nextScope).profile().profile().bucket()).isEqualTo("bucket");
        var exactVersion = new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                managedObjectUri(nextScope.tenantId(), "b".repeat(64)), "b".repeat(64),
                123, tenantScope(nextScope.tenantId()));
        assertThat(fixture.recordStagedSource(next, nextScope, exactVersion)).isEqualTo(exactVersion);
        assertThat(fixture.profiles().unregister("incoming")).isTrue();
        // A new physical source endpoint with the same s3:// URI and version
        // must not inherit the previous incarnation's READ admission.
        var moved = new StorageProfile("incoming", StorageType.S3, "bucket", "data/",
                "https://other-s3.example", "us-east-1", Map.of(), Map.of());
        assertThat(fixture.profiles().createIfAbsent(moved)).isTrue();
        fixture.grants().addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        assertThatThrownBy(() -> fixture.redeem(next, nextScope))
                .isInstanceOf(AccessDeniedException.class);
        var replacement = fixture.profiles().getCurrentRevision("incoming").orElseThrow();
        var movedScope = scopeWith(scope, scope.tenantId(), scope.principal(),
                replacement.incarnationId(), replacement.revision(), scope.canonicalSourceUrl(),
                scope.executionId(), "stage-after-endpoint-move", scope.ownerId(),
                scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                scope.deadlineEpochMs());
        var movedTicket = fixture.issue(movedScope, ALICE);
        assertThat(fixture.redeem(movedTicket, movedScope).stagedSource()).isEmpty();
    }

    @Test
    void concurrentSameAttemptRedemptionUsesOneDurableTransition() throws Exception {
        var fixture = fixture("copy_source_admission_race");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        var other = new JdbcCopySourceReadAdmissions(fixture.source(), fixture.accessControl());
        var executor = Executors.newFixedThreadPool(2);
        try {
            var calls = java.util.List.<Callable<String>>of(
                    () -> fixture.redeem(ticket, scope).profile().incarnationId(),
                    () -> other.redeem(ticket, scope, fixture.anchor()).profile().incarnationId());
            var results = executor.invokeAll(calls);
            assertThat(results.get(0).get()).isEqualTo(scope.profileIncarnationId());
            assertThat(results.get(1).get()).isEqualTo(scope.profileIncarnationId());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentIssueOfOneAttemptCannotCreateTwoTickets() throws Exception {
        var fixture = fixture("copy_source_admission_issue_race");
        var scope = fixture.scope();
        var executor = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            Callable<Boolean> issue = () -> {
                assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                try {
                    fixture.issue(scope, ALICE);
                    return true;
                } catch (MetastoreException failure) {
                    assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                    assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("23505");
                    return false;
                } catch (IllegalStateException alreadyIssued) {
                    assertThat(alreadyIssued).hasMessageContaining("already has a private ticket");
                    return false;
                }
            };
            var first = executor.submit(issue);
            var second = executor.submit(issue);
            start.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS) ^ second.get(10, TimeUnit.SECONDS)).isTrue();
            try (var connection = fixture.source().getConnection();
                    var query = connection.prepareStatement(
                            "SELECT count(*) FROM copy_source_read_admissions");
                    var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(1);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentGrantRevocationFencesBlockedRedemption() throws Exception {
        var fixture = fixture("copy_source_admission_revoke_race");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        var executor = Executors.newSingleThreadExecutor();
        try (var mutation = fixture.source().getConnection()) {
            mutation.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(mutation);
            try (var delete = mutation.prepareStatement(
                    "DELETE FROM security_grants WHERE role_name='reader' "
                            + "AND privilege='READ_STORAGE_PROFILE' AND catalog_name='incoming'")) {
                assertThat(delete.executeUpdate()).isEqualTo(1);
            }
            var started = new CountDownLatch(1);
            var result = executor.submit(() -> {
                started.countDown();
                return fixture.redeem(ticket, scope);
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(result.isDone()).isFalse();
            mutation.commit();
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(AccessDeniedException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void realExecutionTakeoverFencesOldTicketAndStagedProof() throws Exception {
        var fixture = fixture("copy_source_admission_owner_takeover");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        fixture.redeem(ticket, scope);
        var anchors = new JdbcExecutionAnchorStore(fixture.source());
        assertThat(anchors.yield(fixture.anchor())).isTrue();
        var replacement = anchors.tryAcquire(scope.executionId(), "coordinator-two",
                "incarnation-two", Duration.ofMinutes(5)).orElseThrow();
        assertThat(replacement.fencingToken()).isGreaterThan(fixture.anchor().fencingToken());

        assertThatThrownBy(() -> fixture.redeem(ticket, scope))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("owner is absent, replaced or expired");
        var staged = new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                managedObjectUri(scope.tenantId(), "b".repeat(64)), "b".repeat(64),
                123, tenantScope(scope.tenantId()));
        assertThatThrownBy(() -> fixture.recordStagedSource(ticket, scope, staged))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("owner is absent, replaced or expired");
        assertThatThrownBy(() -> fixture.issue(scope, ALICE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("owner is absent, replaced or expired");
    }

    @Test
    void realControlLeaderFailoverFencesOldTicketAndStagedProof() throws Exception {
        var fixture = fixture("copy_source_admission_control_failover");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        fixture.redeem(ticket, scope);
        var lease = new JdbcDistributedLease(fixture.source());
        assertThat(lease.release("coordinator_leader", "coordinator-one", scope.controlEpoch())).isTrue();
        assertThatThrownBy(() -> fixture.redeem(ticket, scope)).isInstanceOf(AccessDeniedException.class);
        var nextLeader = lease.tryAcquire("coordinator_leader", "coordinator-two",
                Duration.ofMinutes(5)).orElseThrow();
        assertThat(nextLeader.epoch()).isGreaterThan(scope.controlEpoch());
        var staged = new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                managedObjectUri(scope.tenantId(), "b".repeat(64)), "b".repeat(64),
                123, tenantScope(scope.tenantId()));
        assertThatThrownBy(() -> fixture.recordStagedSource(ticket, scope, staged))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> fixture.issue(scope, ALICE)).isInstanceOf(AccessDeniedException.class);
        var currentScope = scopeWith(scope, scope.tenantId(), scope.principal(),
                scope.profileIncarnationId(), scope.profileRevision(), scope.canonicalSourceUrl(),
                scope.executionId(), "stage-after-failover", scope.ownerId(), scope.ownerIncarnation(),
                nextLeader.epoch(), scope.ownerFencingToken(), scope.deadlineEpochMs());
        var currentTicket = fixture.issue(currentScope, ALICE);
        assertThat(fixture.redeem(currentTicket, currentScope).profile().profile().bucket())
                .isEqualTo("bucket");
    }

    @Test
    void stagedProofRequiresRedemptionAndExactRetryReusesDurableResult() throws Exception {
        var fixture = fixture("copy_source_admission_staged");
        var scope = fixture.scope();
        var ticket = fixture.issue(scope, ALICE);
        var staged = new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                managedObjectUri(scope.tenantId(), "b".repeat(64)), "b".repeat(64),
                123, tenantScope(scope.tenantId()));
        assertThatThrownBy(() -> fixture.recordStagedSource(ticket, scope, staged))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(fixture.redeem(ticket, scope).stagedSource()).isEmpty();
        assertThat(fixture.recordStagedSource(ticket, scope, staged)).isEqualTo(staged);
        var restarted = new JdbcCopySourceReadAdmissions(fixture.source(), fixture.accessControl());
        assertThat(restarted.redeem(ticket, scope, fixture.anchor()).stagedSource()).contains(staged);
        assertThat(restarted.redeem(ticket, scope, fixture.anchor()).stagedSource().orElseThrow()
                .managedObjectUri()).isEqualTo(staged.managedObjectUri());
        assertThat(restarted.recordStagedSource(ticket, scope, staged, fixture.anchor())).isEqualTo(staged);
        assertThatThrownBy(() -> restarted.recordStagedSource(ticket, scope,
                new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                        "s3://untrusted/copy-managed/tenants/" + tenantScope(scope.tenantId())
                                + "/sha256/bb/" + "b".repeat(64), "b".repeat(64),
                        123, tenantScope(scope.tenantId())), fixture.anchor()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> restarted.recordStagedSource(ticket, scope,
                new JdbcCopySourceReadAdmissions.StagedSource("version-two",
                        managedObjectUri(scope.tenantId(), "c".repeat(64)), "c".repeat(64),
                        123, tenantScope(scope.tenantId())), fixture.anchor()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("conflicts");
        assertThatThrownBy(() -> restarted.recordStagedSource(ticket, scope,
                new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                        "s3://managed/copy-managed/tenants/" + "0".repeat(64)
                                + "/sha256/bb/" + "b".repeat(64), "b".repeat(64),
                        123, "0".repeat(64)), fixture.anchor()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> new JdbcCopySourceReadAdmissions.StagedSource("version-one",
                "s3://managed/copy-managed/tenants/" + "0".repeat(64)
                        + "/sha256/bb/" + "b".repeat(64),
                "b".repeat(64), 123, tenantScope(scope.tenantId())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void expiredAndUnavailableAuthorityFailClosed() throws Exception {
        var fixture = fixture("copy_source_admission_failclosed");
        var scope = fixture.scope();
        var soon = scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                scope.controlEpoch(), scope.ownerFencingToken(),
                System.currentTimeMillis() + 2_000);
        var ticket = fixture.issue(soon, ALICE);
        await().atMost(Duration.ofSeconds(5)).until(
                () -> databaseTimeAfter(fixture.source(), soon.deadlineEpochMs()));
        assertThatThrownBy(() -> fixture.redeem(ticket, soon))
                .isInstanceOf(AccessDeniedException.class);
        var live = scopeWith(scope, scope.tenantId(), scope.principal(), scope.profileIncarnationId(),
                scope.profileRevision(), scope.canonicalSourceUrl(), scope.executionId(),
                "unavailable-attempt", scope.ownerId(), scope.ownerIncarnation(),
                scope.controlEpoch(), scope.ownerFencingToken(), scope.deadlineEpochMs());
        var liveTicket = fixture.issue(live, ALICE);
        try (var connection = fixture.source().getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE copy_source_read_admissions RENAME TO unavailable_copy_source_admissions");
        }
        assertThatThrownBy(() -> fixture.issue(scope, ALICE))
                .isInstanceOf(MetastoreException.class);
        assertThatThrownBy(() -> fixture.redeem(liveTicket, live))
                .isInstanceOf(MetastoreException.class);
    }

    @Test
    void directSqlCannotRewriteTaskIdentityOrEraseTicket() throws Exception {
        var fixture = fixture("copy_source_admission_immutable");
        fixture.issue(fixture.scope(), ALICE);
        try (var connection = fixture.source().getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE copy_source_read_admissions SET staging_attempt_id='stolen'"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM copy_source_read_admissions"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be deleted");
            assertThatThrownBy(() -> statement.executeUpdate("TRUNCATE copy_source_read_admissions"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be deleted");
        }
    }

    @Test
    void freshMigrationThenSecurityBootstrapTracksDirectAuthorityMutations() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_source_authority_fresh");
        new PersistentRoleStore(source).initSchema();
        assertDirectMutationBumpsRevision(source);
    }

    @Test
    void existingSecurityTriggersAdoptV59FunctionDuringUpgrade() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("copy_source_authority_upgrade");
        Flyway.configure().dataSource(source).locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion("58")).load().migrate();
        new PersistentRoleStore(source).initSchema();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        assertDirectMutationBumpsRevision(source);
    }

    @Test
    void directSqlCannotResetOrEraseAuthorizationRevision() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_source_authority_immutable");
        new PersistentRoleStore(source).initSchema();
        long before = authorityRevision(source);
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            for (String mutation : new String[]{
                    "UPDATE pangrova_authorization_revision SET revision=revision+1",
                    "UPDATE pangrova_authorization_revision SET revision=1",
                    "DELETE FROM pangrova_authorization_revision",
                    "TRUNCATE pangrova_authorization_revision",
                    "INSERT INTO pangrova_authorization_revision(singleton,revision) VALUES(FALSE,1)"}) {
                assertThatThrownBy(() -> statement.execute(mutation))
                        .isInstanceOf(SQLException.class).hasMessageContaining("trigger-managed");
                assertThat(authorityRevision(source)).isEqualTo(before);
            }
        }
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate(
                    "INSERT INTO pangrova_roles(role_name) VALUES('revision-positive')"))
                    .isEqualTo(1);
        }
        assertThat(authorityRevision(source)).isGreaterThan(before);
    }

    @Test
    void frozenInputIsInheritedBeforeWorkerRedemptionAndReissuePreservesItsScope() throws Exception {
        var fixture = fixture("copy_frozen_input_exact");
        var proof = stageRoot(fixture);
        var attempt = inputAttempt(fixture, "leaf-one");
        var bound = fixture.admissions().issueFrozenInput(attempt, fixture.anchor());
        try (var connection = fixture.source().getConnection(); var query = connection.prepareStatement(
                "SELECT redeemed_at, staged_content_sha256, parent_nonce_sha256 "
                        + "FROM copy_source_read_admissions WHERE staging_attempt_id='leaf-one'");
                var rows = query.executeQuery()) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getTimestamp(1)).isNull();
            assertThat(rows.getString(2)).isEqualTo(proof.contentSha256());
            assertThat(rows.getString(3)).hasSize(64).isNotEqualTo(bound.ticket().nonce());
        }
        var restarted = new JdbcCopySourceReadAdmissions(fixture.source(), fixture.accessControl());
        var redelivered = restarted.issueFrozenInput(attempt, fixture.anchor());
        assertThat(redelivered.ticket()).isNotEqualTo(bound.ticket());
        assertThat(redelivered.scope()).isEqualTo(bound.scope());
        assertThat(restarted.redeem(bound.ticket(), bound.scope(), fixture.anchor()).stagedSource()).contains(proof);
        assertThat(restarted.redeem(redelivered.ticket(), redelivered.scope(), fixture.anchor()).stagedSource())
                .contains(proof);
        var changed = new JdbcCopySourceReadAdmissions.FrozenInputAttempt(attempt.tenantId(), attempt.principal(),
                attempt.executionId(), attempt.sourceAttemptId(), attempt.taskId(), attempt.controlEpoch(),
                attempt.deadlineEpochMs() + 1);
        assertThatThrownBy(() -> restarted.issueFrozenInput(changed, fixture.anchor()))
                .isInstanceOf(MetastoreException.class).hasStackTraceContaining("another input scope");
    }

    @Test
    void frozenInputRejectsMissingProofOtherSubjectsAndRevokedRegrantedAuthority() throws Exception {
        var fixture = fixture("copy_frozen_input_authority");
        var attempt = inputAttempt(fixture, "leaf-one");
        assertThatThrownBy(() -> fixture.admissions().issueFrozenInput(attempt, fixture.anchor()))
                .isInstanceOf(AccessDeniedException.class);
        stageRoot(fixture);
        for (var changed : new JdbcCopySourceReadAdmissions.FrozenInputAttempt[]{
                new JdbcCopySourceReadAdmissions.FrozenInputAttempt("other", attempt.principal(),
                        attempt.executionId(), attempt.sourceAttemptId(), "leaf-two", attempt.controlEpoch(),
                        attempt.deadlineEpochMs()),
                new JdbcCopySourceReadAdmissions.FrozenInputAttempt(attempt.tenantId(), "other",
                        attempt.executionId(), attempt.sourceAttemptId(), "leaf-two", attempt.controlEpoch(),
                        attempt.deadlineEpochMs())}) {
            assertThatThrownBy(() -> fixture.admissions().issueFrozenInput(changed, fixture.anchor()))
                    .isInstanceOf(AccessDeniedException.class);
        }
        fixture.grants().revokeGrants("reader", Privilege.READ_STORAGE_PROFILE, "incoming", null, null);
        fixture.grants().addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        assertThatThrownBy(() -> fixture.admissions().issueFrozenInput(attempt, fixture.anchor()))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void newExecutionOwnerDerivesOriginalProofAndOldOwnerCannotIssueInput() throws Exception {
        var fixture = fixture("copy_frozen_input_takeover");
        var proof = stageRoot(fixture);
        var anchors = new JdbcExecutionAnchorStore(fixture.source());
        assertThat(anchors.yield(fixture.anchor())).isTrue();
        var replacement = anchors.tryAcquire(fixture.scope().executionId(), "coordinator-two",
                "incarnation-two", Duration.ofMinutes(5)).orElseThrow();
        var attempt = inputAttempt(fixture, "leaf-takeover");
        assertThatThrownBy(() -> fixture.admissions().issueFrozenInput(attempt, fixture.anchor()))
                .isInstanceOf(IllegalStateException.class);
        var bound = fixture.admissions().issueFrozenInput(attempt, replacement);
        assertThat(bound.scope().ownerFencingToken()).isEqualTo(replacement.fencingToken());
        assertThat(fixture.admissions().redeem(bound.ticket(), bound.scope(), replacement).stagedSource())
                .contains(proof);
    }

    static JdbcCopySourceReadAdmissions.StagedSource stageRoot(Fixture fixture) throws Exception {
        var ticket = fixture.issue(fixture.scope(), ALICE);
        fixture.redeem(ticket, fixture.scope());
        return fixture.recordStagedSource(ticket, fixture.scope(), new JdbcCopySourceReadAdmissions.StagedSource(
                "version-one", managedObjectUri(fixture.scope().tenantId(), "b".repeat(64)), "b".repeat(64),
                123, tenantScope(fixture.scope().tenantId())));
    }

    static JdbcCopySourceReadAdmissions.FrozenInputAttempt inputAttempt(Fixture fixture, String taskId) {
        var scope = fixture.scope();
        return new JdbcCopySourceReadAdmissions.FrozenInputAttempt(scope.tenantId(), scope.principal(),
                scope.executionId(), scope.stagingAttemptId(), taskId, scope.controlEpoch(), scope.deadlineEpochMs());
    }

    private static void assertDirectMutationBumpsRevision(DataSource source) throws Exception {
        String[] mutations = {
                "INSERT INTO pangrova_roles(role_name) VALUES ('rev-role')",
                "UPDATE pangrova_roles SET description='changed' WHERE role_name='rev-role'",
                "INSERT INTO pangrova_user_roles(user_id,role_name) VALUES ('alice','rev-role')",
                "DELETE FROM pangrova_user_roles WHERE user_id='alice'",
                "INSERT INTO pangrova_group_roles(group_name,role_name) VALUES ('team-a','rev-role')",
                "DELETE FROM pangrova_group_roles WHERE group_name='team-a'",
                "INSERT INTO security_grants(role_name,privilege,object_type,catalog_name) "
                        + "VALUES ('rev-role','MANAGE_STORAGE_PROFILE','STORAGE_PROFILE','incoming')",
                "UPDATE security_grants SET granted_by='admin' WHERE role_name='rev-role'",
                "DELETE FROM security_grants WHERE role_name='rev-role'",
                "INSERT INTO tenants(tenant_id,name,status,created_at,updated_at) "
                        + "VALUES ('tenant-a','A','ACTIVE',clock_timestamp(),clock_timestamp())",
                "INSERT INTO tenant_user_memberships(user_principal,tenant_id) "
                        + "VALUES ('alice','tenant-a')",
                "UPDATE tenant_user_memberships SET role='ADMIN' WHERE user_principal='alice'",
                "DELETE FROM tenant_user_memberships WHERE user_principal='alice'",
                "DELETE FROM tenants WHERE tenant_id='tenant-a'",
                "INSERT INTO tenant(tenant_id,name) VALUES ('tenant-a','A')",
                "INSERT INTO tenant_membership(user_principal,tenant_id,role) "
                        + "VALUES ('alice','tenant-a','MEMBER')",
                "UPDATE tenant_membership SET role='ADMIN' WHERE user_principal='alice'",
                "DELETE FROM tenant_membership WHERE user_principal='alice'",
                "DELETE FROM tenant WHERE tenant_id='tenant-a'",
                "DELETE FROM pangrova_roles WHERE role_name='rev-role'"
        };
        for (String mutation : mutations) {
            long before = authorityRevision(source);
            try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                assertThat(statement.executeUpdate(mutation)).isEqualTo(1);
            }
            // Role deletion also runs guarded cleanup statements for dependent
            // grants and memberships; every mutation must advance, not exactly once.
            assertThat(authorityRevision(source)).isGreaterThan(before);
        }
        long before = authorityRevision(source);
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("TRUNCATE security_grants"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be truncated");
            assertThatThrownBy(() -> statement.execute("TRUNCATE tenant_membership"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be truncated");
        }
        assertThat(authorityRevision(source)).isEqualTo(before);
    }

    private static long authorityRevision(DataSource source) throws Exception {
        try (var connection = source.getConnection();
                var query = connection.prepareStatement(
                        "SELECT revision FROM pangrova_authorization_revision WHERE singleton=TRUE");
                var rows = query.executeQuery()) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private static boolean databaseTimeAfter(DataSource source, long deadlineEpochMs) throws SQLException {
        try (var connection = source.getConnection();
                var query = connection.prepareStatement("SELECT clock_timestamp()");
                var rows = query.executeQuery()) {
            assertThat(rows.next()).isTrue();
            return rows.getTimestamp(1).toInstant().isAfter(Instant.ofEpochMilli(deadlineEpochMs));
        }
    }

    static Fixture fixture(String schema) throws Exception {
        return fixture(SharedPostgresContainer.migratedDataSource(schema));
    }

    static Fixture fixture(DataSource source) throws Exception {
        var roles = new PersistentRoleStore(source);
        roles.initSchema();
        var tenants = new JdbcTenantStore(source);
        tenants.createTenant(new Tenant("tenant-a", "Tenant A", TenantStatus.ACTIVE,
                new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0),
                java.util.List.of(), java.util.List.of(), null, Map.of(), Instant.now(), "system"));
        tenants.addUserToTenant("tenant-a", "alice", TenantRole.MEMBER);
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        var grants = new PersistentGrantStore(source);
        var profiles = new JdbcStorageProfileRegistry(source);
        assertThat(profiles.createIfAbsent(profile())).isTrue();
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        var access = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new PersistentRowPolicyStore(source)),
                new ColumnMaskingProcessor(new PersistentColumnMaskStore(source)));
        var revision = profiles.getCurrentRevision("incoming").orElseThrow();
        ExecutionAnchor anchor = new JdbcExecutionAnchorStore(source).tryAcquire(
                "execution-one", "coordinator-one", "incarnation-one", Duration.ofMinutes(5))
                .orElseThrow();
        var controlLease = new JdbcDistributedLease(source).tryAcquire(
                "coordinator_leader", "coordinator-one", Duration.ofMinutes(5)).orElseThrow();
        var scope = new JdbcCopySourceReadAdmissions.Scope("tenant-a", "alice", "incoming",
                revision.incarnationId(), revision.revision(), SOURCE, MANAGED_ROOT,
                "a".repeat(64),
                "execution-one", "stage-one",
                anchor.ownerId(), anchor.ownerIncarnation(), controlLease.epoch(), anchor.fencingToken(),
                System.currentTimeMillis() + 60_000);
        return new Fixture(source, profiles, grants, access,
                new JdbcCopySourceReadAdmissions(source, access), scope, anchor);
    }

    private static StorageProfile profile() {
        return new StorageProfile("incoming", StorageType.S3, "bucket", "data/",
                "https://s3.us-east-1.amazonaws.com", "us-east-1",
                Map.of(), Map.of());
    }

    private static String tenantScope(String tenant) throws Exception {
        byte[] bytes = MessageDigest.getInstance("SHA-256")
                .digest(("copy\0" + tenant).getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(bytes);
    }

    private static String managedObjectUri(String tenant, String checksum) throws Exception {
        return "s3://managed/copy-managed/tenants/" + tenantScope(tenant)
                + "/sha256/" + checksum.substring(0, 2) + "/" + checksum;
    }

    private static JdbcCopySourceReadAdmissions.Scope scopeWith(
            JdbcCopySourceReadAdmissions.Scope base, String tenant, String principal,
            String incarnation, long revision, String url, String execution, String attempt,
            String owner, String ownerIncarnation, long epoch, long fence, long deadline) {
        return new JdbcCopySourceReadAdmissions.Scope(tenant, principal, base.profileName(),
                incarnation, revision, url, base.managedStorageRootUri(),
                base.managedBackendIdentitySha256(),
                execution, attempt, owner, ownerIncarnation,
                epoch, fence, deadline);
    }

    record Fixture(DataSource source, JdbcStorageProfileRegistry profiles,
                           PersistentGrantStore grants, DynamicRbacAccessControl accessControl,
                           JdbcCopySourceReadAdmissions admissions,
                           JdbcCopySourceReadAdmissions.Scope scope, ExecutionAnchor anchor) {
        JdbcCopySourceReadAdmissions.Ticket issue(JdbcCopySourceReadAdmissions.Scope requested,
                                                  UserIdentity identity) {
            return admissions.issue(requested, identity, anchor);
        }

        JdbcCopySourceReadAdmissions.Redemption redeem(JdbcCopySourceReadAdmissions.Ticket ticket,
                                                       JdbcCopySourceReadAdmissions.Scope presented) {
            return admissions.redeem(ticket, presented, anchor);
        }

        JdbcCopySourceReadAdmissions.StagedSource recordStagedSource(
                JdbcCopySourceReadAdmissions.Ticket ticket,
                JdbcCopySourceReadAdmissions.Scope presented,
                JdbcCopySourceReadAdmissions.StagedSource staged) {
            return admissions.recordStagedSource(ticket, presented, staged, anchor);
        }
    }
}
