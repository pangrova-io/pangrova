package org.pangrova.metadata.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.storage.JdbcStorageProfileRegistry;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.spi.coordination.CopyFileClaim;
import org.pangrova.spi.coordination.CopyFileClaimStore;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the immutable file-claim authority in real PostgreSQL across store instances. */
class JdbcCopyFileClaimStoreTest {

    private static final String TABLE_A = "00000000-0000-0000-0000-000000000001";
    private static final String TABLE_B = "00000000-0000-0000-0000-000000000002";
    private static final String PROFILE_A = "00000000-0000-0000-0000-000000000003";
    private static final String PROFILE_B = "00000000-0000-0000-0000-000000000004";

    @Test
    void concurrentExecutionsHaveOneDurableWinner() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_race");
        var first = new JdbcCopyFileClaimStore(source);
        var second = new JdbcCopyFileClaimStore(source);
        CopyFileClaim one = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        CopyFileClaim two = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-two");
        var executor = Executors.newFixedThreadPool(2);
        try {
            var results = executor.invokeAll(List.<Callable<CopyFileClaimStore.ClaimResult>>of(
                    () -> first.claim(one), () -> second.claim(two)));
            assertThat(List.of(results.get(0).get(), results.get(1).get()))
                    .containsExactlyInAnyOrder(CopyFileClaimStore.ClaimResult.ACQUIRED,
                            CopyFileClaimStore.ClaimResult.BLOCKED);
            assertThat(first.get(one.claimKey(), 0).orElseThrow().executionId())
                    .isIn("execution-one", "execution-two");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void restartRetainsPendingOwnerAndOnlySameExecutionCanReplay() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_restart");
        CopyFileClaim original = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        assertThat(new JdbcCopyFileClaimStore(source).claim(original))
                .isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);

        var recovered = new JdbcCopyFileClaimStore(source);
        assertThat(recovered.get(original.claimKey(), 0)).contains(original);
        assertThat(recovered.claim(original))
                .isEqualTo(CopyFileClaimStore.ClaimResult.SAME_EXECUTION);
        CopyFileClaim otherExecution = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-two");
        assertThat(recovered.claim(otherExecution))
                .isEqualTo(CopyFileClaimStore.ClaimResult.BLOCKED);
    }

    @Test
    void sameSourceVersionWithChangedBytesOrProfileEvidenceFailsClosed() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_evidence");
        var store = new JdbcCopyFileClaimStore(source);
        CopyFileClaim original = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        assertThat(store.claim(original)).isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);

        assertThatThrownBy(() -> store.claim(claim("tenant-a", TABLE_A, "version-1",
                "b".repeat(64), PROFILE_A, "execution-two")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("conflicting byte");
        assertThatThrownBy(() -> store.claim(claim("tenant-a", TABLE_A, "version-1",
                "a".repeat(64), PROFILE_B, "execution-two")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("conflicting byte");
        assertThat(store.get(original.claimKey(), 0)).contains(original);
    }

    @Test
    void profileRecreationCannotReclaimTheSameSourceVersion() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_profile_recreate");
        new PersistentRoleStore(source).initSchema();
        var profiles = new JdbcStorageProfileRegistry(source);
        var store = new JdbcCopyFileClaimStore(source);
        var profile = new StorageProfile("profile", StorageType.S3, "bucket", "data/",
                "", "", Map.of(), Map.of());
        assertThat(profiles.createIfAbsent(profile)).isTrue();
        String firstIncarnation = profiles.getCurrentRevision("profile").orElseThrow().incarnationId();
        CopyFileClaim before = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                firstIncarnation, "execution-one");
        assertThat(store.claim(before)).isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);

        assertThat(profiles.unregister("profile")).isTrue();
        assertThat(profiles.createIfAbsent(profile)).isTrue();
        String nextIncarnation = profiles.getCurrentRevision("profile").orElseThrow().incarnationId();
        CopyFileClaim after = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                nextIncarnation, "execution-two");
        assertThat(nextIncarnation).isNotEqualTo(firstIncarnation);
        assertThat(after.claimKey()).isEqualTo(before.claimKey());
        assertThat(after.claimDigest()).isNotEqualTo(before.claimDigest());
        assertThatThrownBy(() -> store.claim(after))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("conflicting byte");
        assertThat(store.get(before.claimKey(), 0)).contains(before);
    }

    @Test
    void tenantTableAndVersionAreIndependentFileIdentities() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_scope");
        var store = new JdbcCopyFileClaimStore(source);
        CopyFileClaim one = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        CopyFileClaim otherTenant = claim("tenant-b", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-two");
        CopyFileClaim otherTable = claim("tenant-a", TABLE_B, "version-1", "a".repeat(64),
                PROFILE_A, "execution-three");
        CopyFileClaim otherVersion = claim("tenant-a", TABLE_A, "version-2", "b".repeat(64),
                PROFILE_A, "execution-four");
        for (CopyFileClaim candidate : List.of(one, otherTenant, otherTable, otherVersion)) {
            assertThat(store.claim(candidate)).isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);
            assertThat(store.get(candidate.claimKey(), 0)).contains(candidate);
        }
    }

    @Test
    void directSqlCannotRewriteDeleteOrTruncateUnresolvedClaims() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_immutable");
        var store = new JdbcCopyFileClaimStore(source);
        CopyFileClaim original = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        assertThat(store.claim(original)).isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);

        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE copy_file_claims SET execution_id='stolen'"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM copy_file_claims"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate("TRUNCATE copy_file_claims"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
        }
        assertThat(store.get(original.claimKey(), 0)).contains(original);
    }

    @Test
    void unavailableClaimAuthorityNeverLooksLikeAnUnclaimedFile() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_unavailable");
        var store = new JdbcCopyFileClaimStore(source);
        CopyFileClaim original = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE copy_file_claims RENAME TO unavailable_copy_file_claims");
        }
        assertThatThrownBy(() -> store.claim(original)).isInstanceOf(MetastoreException.class);
        assertThatThrownBy(() -> store.get(original.claimKey(), 0)).isInstanceOf(MetastoreException.class);
    }

    @Test
    void onlyCanonicalVersionedS3IdentityCanBeClaimed() {
        assertThatThrownBy(() -> claim("tenant-a", TABLE_A, "null", "a".repeat(64),
                PROFILE_A, "execution-one"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-null S3 version");
        assertThatThrownBy(() -> claim("tenant-a", TABLE_A, "v".repeat(1025), "a".repeat(64),
                PROFILE_A, "execution-one"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("S3 version byte limit");
        assertThatThrownBy(() -> new CopyFileClaim("tenant-a", TABLE_A,
                "s3://bucket/data/../file.parquet", "version-1", "a".repeat(64), 4,
                "profile", PROFILE_A, 1, 0, "execution-one"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unsafe object key");
        assertThatThrownBy(() -> new CopyFileClaim("tenant-a", TABLE_A,
                "s3://bucket/data/file.parquet?versionId=latest", "version-1",
                "a".repeat(64), 4, "profile", PROFILE_A, 1, 0, "execution-one"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical S3");
    }

    @Test
    void reloadGenerationCannotBypassAnUnprovenPriorPublication() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("copy_claim_generation_fence");
        var store = new JdbcCopyFileClaimStore(source);
        CopyFileClaim first = claim("tenant-a", TABLE_A, "version-1", "a".repeat(64),
                PROFILE_A, "execution-one");
        assertThat(store.claim(first)).isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);
        CopyFileClaim reload = new CopyFileClaim(first.tenantId(), first.tableUuid(),
                first.sourceUrl(), first.sourceVersionId(), first.sourceSha256(), first.sourceSize(),
                first.profileName(), first.profileIncarnationId(), first.profileRevision(),
                1, "execution-two");
        assertThatThrownBy(() -> store.claim(reload)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("atomic catalog-head fence");
        assertThat(store.get(first.claimKey(), 1)).isEmpty();
        assertThat(store.get(first.claimKey(), 0)).contains(first);
    }

    private static CopyFileClaim claim(String tenant, String table, String version, String sha,
                                       String profileIncarnation, String execution) {
        return new CopyFileClaim(tenant, table, "s3://bucket/data/file.parquet", version,
                sha, 4, "profile", profileIncarnation, 1, 0, execution);
    }
}
