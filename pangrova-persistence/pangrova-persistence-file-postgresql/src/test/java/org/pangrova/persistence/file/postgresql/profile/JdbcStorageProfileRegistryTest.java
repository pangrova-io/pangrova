package org.pangrova.persistence.file.postgresql.profile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.file.api.exception.FileStorageException;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests real PostgreSQL uniqueness and immutable profile identity across coordinator instances. */
class JdbcStorageProfileRegistryTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void uniqueLongestMatchWinsDespiteShorterTiesRegardlessOfProfileNameOrder(boolean specificFirst)
            throws Exception {
        var registry = new JdbcStorageProfileRegistry(MigratedPostgres.create());
        var specific = profile(specificFirst ? "a_specific" : "z_specific", "bucket-a", "data/nested/");
        assertThat(registry.createIfAbsent(profile("b_broad", "bucket-a", "data/"))).isTrue();
        assertThat(registry.createIfAbsent(profile("c_broad", "bucket-a", "data/"))).isTrue();
        assertThat(registry.createIfAbsent(specific)).isTrue();
        // Longer non-matching candidates must not affect selection either.
        assertThat(registry.createIfAbsent(profile("d_unrelated", "bucket-b", "data/nested/deeper/"))).isTrue();

        var resolved = assertDoesNotThrow(() -> registry.resolveByUrl("s3://bucket-a/data/nested/file.parquet"));
        assertThat(resolved).contains(specific);
        assertThat(registry.resolveByUrl("s3://missing/data/nested/file.parquet")).isEmpty();
    }

    @Test
    void exactRevisionSurvivesDropAndNewIncarnationAcrossInstances() throws Exception {
        var source = MigratedPostgres.create();
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("profile-reader"));
        var grants = new JdbcGrantStore(source);
        var first = new JdbcStorageProfileRegistry(source);
        var second = new JdbcStorageProfileRegistry(source);
        StorageProfile old = profile("incoming", "bucket-a", "data/");

        assertThat(first.createIfAbsent(old)).isTrue();
        var original = second.getCurrentRevision("incoming").orElseThrow();
        assertThat(original.profile()).isEqualTo(old);
        assertThat(original.revision()).isEqualTo(1);
        assertThat(second.createIfAbsent(profile("incoming", "forged", ""))).isFalse();
        assertThat(first.get("incoming")).contains(old);
        assertThat(first.resolveByUrl("s3://bucket-a/data/file.parquet")).contains(old);
        grants.addGrant(PrivilegeGrant.of("profile-reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));

        assertThat(second.unregister("incoming")).isTrue();
        assertThat(grants.listGrantsForRole("profile-reader")).isEmpty();
        assertThat(first.getCurrentRevision("incoming")).isEmpty();
        assertThat(first.findRevision("incoming", original.incarnationId(), 1))
                .contains(original);

        StorageProfile replacement = profile("incoming", "bucket-b", "new/");
        assertThat(second.createIfAbsent(replacement)).isTrue();
        var current = first.getCurrentRevision("incoming").orElseThrow();
        assertThat(current.profile()).isEqualTo(replacement);
        assertThat(current.incarnationId()).isNotEqualTo(original.incarnationId());
        assertThat(first.findRevision("incoming", original.incarnationId(), 1))
                .contains(original);
        assertThat(first.findRevision("incoming", current.incarnationId(), 1))
                .contains(current);
        assertThat(first.findRevision("incoming", original.incarnationId(), 2)).isEmpty();
        grants.addGrant(PrivilegeGrant.of("profile-reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM storage_profile_definitions WHERE name='incoming'");
            assertThat(grants.listGrantsForRole("profile-reader")).isEmpty();
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE storage_profile_revisions SET bucket='forged' WHERE name='incoming'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("immutable");
        }
    }

    @Test
    void concurrentCreateHasOneWinnerAndCannotReplacePayload() throws Exception {
        var source = MigratedPostgres.create();
        var first = new JdbcStorageProfileRegistry(source);
        var second = new JdbcStorageProfileRegistry(source);
        var calls = new ArrayList<Callable<Boolean>>();
        for (int i = 0; i < 12; i++) {
            int index = i;
            calls.add(() -> (index % 2 == 0 ? first : second).createIfAbsent(
                    profile("race", "bucket-" + index, "data/")));
        }
        var executor = Executors.newFixedThreadPool(6);
        try {
            var results = executor.invokeAll(calls);
            int winners = 0;
            for (var result : results) {
                if (result.get()) winners++;
            }
            assertThat(winners).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
        var exact = first.getCurrentRevision("race").orElseThrow();
        assertThat(second.findRevision("race", exact.incarnationId(), 1)).contains(exact);
        assertThat(first.list()).extracting(StorageProfile::name).containsExactly("race");
    }

    @Test
    void ambiguousScopeAndUnavailableAuthorityFailClosed() throws Exception {
        var source = MigratedPostgres.create();
        var registry = new JdbcStorageProfileRegistry(source);
        assertThat(registry.createIfAbsent(profile("one", "bucket-a", "data/"))).isTrue();
        assertThat(registry.createIfAbsent(profile("two", "bucket-a", "data/"))).isTrue();
        assertThatThrownBy(() -> registry.resolveByUrl("s3://bucket-a/data/file.parquet"))
                .isInstanceOf(FileStorageException.class).hasMessageContaining("Ambiguous");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE storage_profile_definitions RENAME TO unavailable_profiles");
        }
        assertThatThrownBy(registry::list).isInstanceOf(FileStorageException.class);
        assertThatThrownBy(() -> registry.get("one")).isInstanceOf(FileStorageException.class);
        assertThatThrownBy(() -> registry.createIfAbsent(profile("new", "bucket-a", "")))
                .isInstanceOf(FileStorageException.class);
    }

    @Test
    void concurrentRecreationCannotKeepGrantFromDroppedIncarnation() throws Exception {
        var source = MigratedPostgres.create();
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        var grants = new JdbcGrantStore(source);
        var first = new JdbcStorageProfileRegistry(source);
        var second = new JdbcStorageProfileRegistry(source);
        assertThat(first.createIfAbsent(profile("race", "old-bucket", ""))).isTrue();
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "race", null, null, null));
        var executor = Executors.newSingleThreadExecutor();
        try (var drop = source.getConnection()) {
            drop.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(drop);
            try (var statement = drop.prepareStatement(
                    "DELETE FROM storage_profile_definitions WHERE name=?")) {
                statement.setString(1, "race");
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            CountDownLatch started = new CountDownLatch(1);
            var replacement = executor.submit(() -> {
                started.countDown();
                return second.createIfAbsent(profile("race", "new-bucket", ""));
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(replacement.isDone()).isFalse();
            drop.commit();
            assertThat(replacement.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(grants.listGrantsForRole("reader")).isEmpty();
            assertThat(first.get("race").orElseThrow().bucket()).isEqualTo("new-bucket");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void absentProfileRejectsExactReadAndViewGrantsThroughStoreAndDirectSql() throws Exception {
        var source = MigratedPostgres.create();
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        var grants = new JdbcGrantStore(source);
        var registry = new JdbcStorageProfileRegistry(source);

        for (Privilege privilege : new Privilege[]{Privilege.READ_STORAGE_PROFILE,
                Privilege.VIEW_STORAGE_PROFILE, Privilege.ALL, Privilege.OWNER}) {
            assertThatThrownBy(() -> grants.addGrant(PrivilegeGrant.of("reader", privilege,
                    "STORAGE_PROFILE", "incoming", null, null, null)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasRootCauseInstanceOf(SQLException.class)
                    .hasStackTraceContaining("requires an existing exact profile");
        }
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate("""
                    INSERT INTO security_grants(role_name, privilege, object_type, catalog_name)
                    VALUES ('reader', 'READ_STORAGE_PROFILE', 'STORAGE_PROFILE', 'incoming')
                    """))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("requires an existing exact profile");
        }
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.MANAGE_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate("""
                    UPDATE security_grants SET privilege='READ_STORAGE_PROFILE'
                    WHERE role_name='reader' AND catalog_name='incoming'
                    """))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("requires an existing exact profile");
        }
        assertThat(registry.createIfAbsent(profile("incoming", "bucket-a", ""))).isTrue();
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.VIEW_STORAGE_PROFILE,
                "STORAGE_PROFILE", "incoming", null, null, null));
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.ALL,
                "STORAGE_PROFILE", "incoming", null, null, null));
        grants.addGrant(PrivilegeGrant.of("reader", Privilege.OWNER,
                "STORAGE_PROFILE", "incoming", null, null, null));
        assertThat(registry.unregister("incoming")).isTrue();
        assertThat(grants.listGrantsForRole("reader"))
                .extracting(PrivilegeGrant::privilege).containsExactly(Privilege.MANAGE_STORAGE_PROFILE);
        assertThat(registry.createIfAbsent(profile("incoming", "bucket-b", ""))).isTrue();
        assertThat(grants.listGrantsForRole("reader"))
                .extracting(PrivilegeGrant::privilege).containsExactly(Privilege.MANAGE_STORAGE_PROFILE);
    }

    @Test
    void concurrentPregrantWaitsForDropThenFailsWithoutLatentAuthority() throws Exception {
        var source = MigratedPostgres.create();
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        var grants = new JdbcGrantStore(source);
        var registry = new JdbcStorageProfileRegistry(source);
        assertThat(registry.createIfAbsent(profile("incoming", "old-bucket", ""))).isTrue();
        var executor = Executors.newSingleThreadExecutor();
        try (var drop = source.getConnection()) {
            drop.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(drop);
            try (var statement = drop.prepareStatement(
                    "DELETE FROM storage_profile_definitions WHERE name=?")) {
                statement.setString(1, "incoming");
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            CountDownLatch started = new CountDownLatch(1);
            var pregrant = executor.submit(() -> {
                started.countDown();
                grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                        "STORAGE_PROFILE", "incoming", null, null, null));
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(pregrant.isDone()).isFalse();
            drop.commit();
            assertThatThrownBy(() -> pregrant.get(5, TimeUnit.SECONDS))
                    .hasStackTraceContaining("requires an existing exact profile");
            assertThat(registry.createIfAbsent(profile("incoming", "new-bucket", ""))).isTrue();
            assertThat(grants.listGrantsForRole("reader")).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void directCurrentInsertRecordsHistoryAndImmutableTablesRejectMutation() throws Exception {
        var source = MigratedPostgres.create();
        var registry = new JdbcStorageProfileRegistry(source);
        UUID incarnation = UUID.randomUUID();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    INSERT INTO storage_profile_definitions
                    (name, incarnation_id, revision, storage_type, bucket,
                     object_prefix, endpoint, region, options)
                    VALUES ('direct', '%s', 1, 'S3', 'direct-bucket', '', '', 'us-east-1', '{}')
                    """.formatted(incarnation));
            var revision = registry.findRevision("direct", incarnation.toString(), 1).orElseThrow();
            assertThat(revision.profile().bucket()).isEqualTo("direct-bucket");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE storage_profile_definitions SET bucket='forged' WHERE name='direct'"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot be updated");
            assertThatThrownBy(() -> statement.execute("TRUNCATE storage_profile_definitions"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Authority tables cannot be truncated");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "DELETE FROM storage_profile_revisions WHERE name='direct'"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.execute("TRUNCATE storage_profile_revisions"))
                    // V59 admissions reference revisions, so PostgreSQL may
                    // reject at the FK before the immutability trigger runs.
                    .isInstanceOf(SQLException.class)
                    .satisfies(failure -> assertThat(failure.getMessage())
                            .containsAnyOf("immutable", "cannot truncate a table referenced"));
            assertThatThrownBy(() -> statement.executeUpdate("""
                    INSERT INTO storage_profile_revisions
                    (name, incarnation_id, revision, storage_type, bucket,
                     object_prefix, endpoint, region, options)
                    VALUES ('direct', '%s', 2, 'S3', 'forged', '', '', 'us-east-1', '{}')
                    """.formatted(incarnation)))
                    .isInstanceOf(SQLException.class).hasMessageContaining("must match the current definition");
            assertThat(registry.findRevision("direct", incarnation.toString(), 1))
                    .contains(revision);
        }
    }

    private static StorageProfile profile(String name, String bucket, String prefix) {
        return new StorageProfile(name, StorageType.S3, bucket, prefix,
                "", "us-east-1", Map.of(), Map.of("path-style-access", "true"));
    }
}
