package org.pangrova.coordinator.coordination.store.jdbc;

import org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.postgresql.lease.JdbcDistributedLease;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.coordination.store.jdbc.JdbcCopySourceReadAdmissionsTest.fixture;
import static org.pangrova.coordinator.coordination.store.jdbc.JdbcCopySourceReadAdmissionsTest.inputAttempt;
import static org.pangrova.coordinator.coordination.store.jdbc.JdbcCopySourceReadAdmissionsTest.stageRoot;

/** Real PostgreSQL lineage, concurrent delivery and migration boundaries. */
class JdbcCopyFrozenInputLineageTest {
    @Test
    void concurrentDeliveryGetsDistinctNoncesForOneImmutableScope() throws Exception {
        var fixture = fixture("copy_input_concurrent");
        var proof = stageRoot(fixture);
        var attempt = inputAttempt(fixture, "concurrent-leaf");
        var executor = Executors.newFixedThreadPool(4);
        var start = new CountDownLatch(1);
        try {
            var results = new java.util.ArrayList<java.util.concurrent.Future<CopySourceReadAdmissions.FrozenInput>>();
            for (int i = 0; i < 4; i++) {
                results.add(executor.submit(() -> {
                    if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("No start signal");
                    return fixture.admissions().issueFrozenInput(attempt, fixture.anchor());
                }));
            }
            start.countDown();
            var nonces = new java.util.HashSet<String>();
            for (var result : results) {
                var input = result.get(10, TimeUnit.SECONDS);
                assertThat(nonces.add(input.ticket().nonce())).isTrue();
                assertThat(fixture.redeem(input.ticket(), input.scope()).stagedSource()).contains(proof);
            }
            assertThat(nonces).hasSize(4);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void issuanceGateSerializesOneAttemptWithoutBlockingAnother() throws Exception {
        var fixture = fixture("copy_input_gate_scope");
        var proof = stageRoot(fixture);
        var blocked = inputAttempt(fixture, "blocked-leaf");
        var independent = inputAttempt(fixture, "independent-leaf");
        var executor = Executors.newFixedThreadPool(2);
        try (var gate = fixture.source().getConnection()) {
            gate.setAutoCommit(false);
            try (var query = gate.prepareStatement("""
                    SELECT pg_advisory_xact_lock(207578818,
                        hashtext(json_build_array(?::text,?::text,?::text)::text))
                    """)) {
                query.setString(1, blocked.tenantId());
                query.setString(2, blocked.executionId());
                query.setString(3, blocked.taskId());
                query.execute();
            }
            var waiting = executor.submit(() -> fixture.admissions().issueFrozenInput(blocked, fixture.anchor()));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                try (var connection = fixture.source().getConnection(); var query = connection.prepareStatement(
                        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=207578818 AND NOT granted");
                        var rows = query.executeQuery()) {
                    return rows.next() && rows.getLong(1) == 1;
                }
            });
            assertThat(waiting.isDone()).isFalse();
            var ready = executor.submit(() -> fixture.admissions().issueFrozenInput(independent, fixture.anchor()))
                    .get(5, TimeUnit.SECONDS);
            assertThat(fixture.redeem(ready.ticket(), ready.scope()).stagedSource()).contains(proof);
            gate.commit();
            assertThat(waiting.get(5, TimeUnit.SECONDS).proof()).isEqualTo(proof);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void directSqlCannotRewriteInheritedProofOrParentBeforeWorkerRedemption() throws Exception {
        var fixture = fixture("copy_input_immutable");
        stageRoot(fixture);
        fixture.admissions().issueFrozenInput(inputAttempt(fixture, "leaf-one"), fixture.anchor());
        try (var connection = fixture.source().getConnection(); var statement = connection.createStatement()) {
            for (String set : new String[]{"parent_nonce_sha256=NULL", "staged_content_sha256=repeat('c',64)",
                    "staged_source_version_id='replacement'", "staged_content_size=124"}) {
                assertThatThrownBy(() -> statement.executeUpdate(
                        "UPDATE copy_source_read_admissions SET " + set + " WHERE staging_attempt_id='leaf-one'"))
                        .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            }
            assertThatThrownBy(() -> statement.executeUpdate("""
                    INSERT INTO copy_source_read_admissions
                    SELECT (jsonb_populate_record(NULL::copy_source_read_admissions,
                        to_jsonb(a) || jsonb_build_object('nonce_sha256', repeat('c',64),
                            'staging_attempt_id','injected','staged_content_size',999))).*
                    FROM copy_source_read_admissions a WHERE staging_attempt_id='leaf-one'
                    """)).isInstanceOf(SQLException.class).hasMessageContaining("exact frozen root proof");
        }
    }

    @Test
    void controlFailoverNeedsNewTaskIdentityAndDoesNotRepinItsSource() throws Exception {
        var fixture = fixture("copy_input_control_takeover");
        var proof = stageRoot(fixture);
        var old = inputAttempt(fixture, "old-leaf");
        var bound = fixture.admissions().issueFrozenInput(old, fixture.anchor());
        var leases = new JdbcDistributedLease(fixture.source());
        assertThat(leases.release("coordinator_leader", fixture.scope().ownerId(), old.controlEpoch())).isTrue();
        var leader = leases.tryAcquire("coordinator_leader", "replacement", Duration.ofMinutes(5)).orElseThrow();
        assertThatThrownBy(() -> fixture.admissions().issueFrozenInput(old, fixture.anchor()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> fixture.redeem(bound.ticket(), bound.scope()))
                .isInstanceOf(AccessDeniedException.class);
        var current = new CopySourceReadAdmissions.FrozenInputAttempt(old.tenantId(), old.principal(),
                old.executionId(), old.sourceAttemptId(), "new-leaf", leader.epoch(), old.deadlineEpochMs());
        var resumed = fixture.admissions().issueFrozenInput(current, fixture.anchor());
        assertThat(fixture.redeem(resumed.ticket(), resumed.scope()).stagedSource()).contains(proof);
    }

    @Test
    void upgradePreservesExistingRootProofAndRequiresNoOldBearerForDerivedInput() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("copy_input_upgrade");
        Flyway.configure().dataSource(source).locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion("64")).load().migrate();
        createV64AuthorityFixture(source);
        var fixture = JdbcCopySourceReadAdmissionsTest.fixtureWithExistingTenant(source);
        var proof = insertLegacyRoot(fixture);
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        var input = fixture.admissions().issueFrozenInput(inputAttempt(fixture, "after-upgrade"), fixture.anchor());
        assertThat(fixture.redeem(input.ticket(), input.scope()).stagedSource()).contains(proof);
        try (var connection = source.getConnection(); var query = connection.prepareStatement("""
                SELECT child.identity_groups = parent.identity_groups
                FROM copy_source_read_admissions child
                JOIN copy_source_read_admissions parent ON parent.nonce_sha256=child.parent_nonce_sha256
                WHERE child.staging_attempt_id='after-upgrade'
                """); var rows = query.executeQuery()) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getBoolean(1)).isTrue();
        }
    }

    /** Test-only pre-canonical schema: current stores must not initialize or upgrade it implicitly. */
    private static void createV64AuthorityFixture(javax.sql.DataSource source) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    INSERT INTO tenants(tenant_id,name,status,created_at,updated_at)
                    VALUES ('tenant-a','Tenant A','ACTIVE',clock_timestamp(),clock_timestamp());
                    INSERT INTO tenant_user_memberships(user_principal,tenant_id,role)
                    VALUES ('alice','tenant-a','MEMBER');
                    CREATE TABLE pangrova_roles (
                        role_name VARCHAR(255) PRIMARY KEY, description VARCHAR(1024),
                        created_by VARCHAR(255), created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);
                    CREATE TABLE pangrova_user_roles (
                        user_id VARCHAR(255) NOT NULL, role_name VARCHAR(255) NOT NULL,
                        granted_by VARCHAR(255), granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(user_id,role_name));
                    CREATE TABLE pangrova_group_roles (
                        group_name VARCHAR(255) NOT NULL, role_name VARCHAR(255) NOT NULL,
                        mapped_by VARCHAR(255), mapped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(group_name,role_name));
                    CREATE TABLE security_grants (
                        grant_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        role_name VARCHAR(255) NOT NULL, privilege VARCHAR(64) NOT NULL,
                        object_type VARCHAR(64) NOT NULL, catalog_name VARCHAR(255),
                        schema_name VARCHAR(255), table_name VARCHAR(255), column_name VARCHAR(255),
                        with_grant_option BOOLEAN NOT NULL DEFAULT FALSE,
                        cascade_revoke BOOLEAN NOT NULL DEFAULT FALSE, granted_by VARCHAR(255),
                        granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(role_name,privilege,object_type,catalog_name,schema_name,table_name,column_name));
                    """);
            try (var rows = statement.executeQuery("""
                    SELECT count(*) FROM information_schema.columns
                    WHERE table_schema=current_schema() AND table_name='tenants' AND column_name='quota_present'
                    """)) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isZero();
            }
        }
    }

    private static CopySourceReadAdmissions.StagedSource insertLegacyRoot(
            JdbcCopySourceReadAdmissionsTest.Fixture fixture) throws Exception {
        var scope = fixture.scope();
        String tenant = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                .digest(("copy\0" + scope.tenantId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        var order = new java.util.ArrayList<>(new org.pangrova.security.api.identity.UserIdentity("alice",
                java.util.Set.of("alpha", "beta"), java.util.Map.of(), java.time.Instant.now()).groups());
        java.util.Collections.reverse(order);
        String groups = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(order);
        var proof = new CopySourceReadAdmissions.StagedSource("legacy-version",
                scope.managedStorageRootUri() + "/tenants/" + tenant + "/sha256/bb/" + "b".repeat(64),
                "b".repeat(64), 123, tenant);
        try (var connection = fixture.source().getConnection(); var insert = connection.prepareStatement("""
                INSERT INTO copy_source_read_admissions (
                    nonce_sha256, tenant_id, principal, identity_groups, identity_attributes, authenticated_at,
                    authorization_revision, profile_name, profile_incarnation_id, profile_revision,
                    canonical_source_url, managed_storage_root_uri, managed_backend_identity_sha256,
                    execution_id, staging_attempt_id, staging_command_sha256, owner_id, owner_incarnation,
                    control_epoch, owner_fencing_token, deadline_at, redeemed_at,
                    staged_source_version_id, staged_managed_object_uri, staged_content_sha256,
                    staged_content_size, staged_tenant_scope, staged_at)
                VALUES (repeat('a',64), ?, ?, ?::jsonb, '{}', clock_timestamp(),
                    (SELECT revision FROM pangrova_authorization_revision WHERE singleton),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, clock_timestamp(), ?, ?, ?, ?, ?, clock_timestamp())
                """)) {
            int i = 1;
            insert.setString(i++, scope.tenantId());
            insert.setString(i++, scope.principal());
            insert.setString(i++, groups);
            insert.setString(i++, scope.profileName());
            insert.setObject(i++, java.util.UUID.fromString(scope.profileIncarnationId()));
            insert.setLong(i++, scope.profileRevision());
            insert.setString(i++, scope.canonicalSourceUrl());
            insert.setString(i++, scope.managedStorageRootUri());
            insert.setString(i++, scope.managedBackendIdentitySha256());
            insert.setString(i++, scope.executionId());
            insert.setString(i++, scope.stagingAttemptId());
            insert.setString(i++, CopySourceReadAdmissions.commandSha256(scope));
            insert.setString(i++, scope.ownerId());
            insert.setString(i++, scope.ownerIncarnation());
            insert.setLong(i++, scope.controlEpoch());
            insert.setLong(i++, scope.ownerFencingToken());
            insert.setTimestamp(i++, new java.sql.Timestamp(scope.deadlineEpochMs()));
            insert.setString(i++, proof.sourceVersionId());
            insert.setString(i++, proof.managedObjectUri());
            insert.setString(i++, proof.contentSha256());
            insert.setLong(i++, proof.contentSize());
            insert.setString(i, proof.tenantScope());
            assertThat(insert.executeUpdate()).isEqualTo(1);
        }
        return proof;
    }

}
