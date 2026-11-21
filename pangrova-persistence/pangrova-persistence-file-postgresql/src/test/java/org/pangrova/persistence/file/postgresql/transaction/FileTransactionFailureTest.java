package org.pangrova.persistence.file.postgresql.transaction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.enums.StorageType;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.persistence.file.api.claim.CopyFileClaim;
import org.pangrova.persistence.file.postgresql.claim.JdbcCopyFileClaimStore;
import org.pangrova.persistence.file.postgresql.directory.JdbcFileDirectoryIndex;
import org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry;
import org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry;
import org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Failures are injected after real SQL, including cascades and trigger-managed authority revisions. */
class FileTransactionFailureTest {
    private static final List<String> OPERATIONS = List.of("profileCreate", "profileDrop", "spaceCreate",
            "spaceMutate", "spaceRename", "spaceDrop", "directoryBegin", "directoryAppend",
            "directoryPublish", "directoryAbort", "claim");

    static Stream<Arguments> failures() {
        return OPERATIONS.stream().flatMap(operation -> Stream.of(
                Arguments.of(operation, "sql", "sql"), Arguments.of(operation, "runtime", "sql"),
                Arguments.of(operation, "error", "sql"), Arguments.of(operation, "runtime", "runtime"),
                Arguments.of(operation, "error", "error"), Arguments.of(operation, "error", "same"),
                Arguments.of(operation, "error", "none")));
    }

    @ParameterizedTest(name = "{0}: primary={1}, rollback={2}")
    @MethodSource("failures")
    void failedMutationPreservesAllFactsAndOriginalFailure(String name, String primaryKind, String rollbackKind)
            throws Exception {
        DataSource source = MigratedPostgres.create();
        Operation operation = operation(source, name);
        var before = snapshot(source);
        Throwable primary = failure(primaryKind);
        Throwable rollback = rollbackKind.equals("same") ? primary
                : rollbackKind.equals("none") ? null : failure(rollbackKind);
        DataSource faulty = JdbcMutationFault.afterMutation(source, operation.sql(), primary, rollback);

        Throwable actual = catchThrowable(operation.action().apply(faulty)::run);
        assertFailure(actual, primary, rollback);
        assertThat(snapshot(source)).isEqualTo(before);
        // The real connection must be closed even when explicit rollback fails; fresh callers can make progress.
        operation.action().apply(source).run();
        assertThat(snapshot(source)).isNotEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(strings = {"profileCreate", "profileDrop", "spaceCreate", "spaceMutate", "spaceRename", "spaceDrop"})
    void acquiredAuthorityGateIsInsideFailureCleanup(String name) throws Exception {
        DataSource source = MigratedPostgres.create();
        Operation operation = operation(source, name);
        var before = snapshot(source);
        var primary = new AssertionError("after real authority lock");
        var rollback = new SQLException("rollback failed");
        DataSource faulty = JdbcMutationFault.afterStatement(source, "SELECT pg_advisory_xact_lock(", primary, rollback);
        assertFailure(catchThrowable(operation.action().apply(faulty)::run), primary, rollback);
        assertThat(snapshot(source)).isEqualTo(before);
        operation.action().apply(source).run();
    }

    @ParameterizedTest
    @ValueSource(strings = {"sql", "runtime", "error", "same", "none"})
    void authorityCallbackFailureReleasesSharedGateAndPreservesPrimary(String rollbackKind) throws Exception {
        DataSource source = MigratedPostgres.create();
        var primary = new AssertionError("authority callback failed");
        Throwable rollback = rollbackKind.equals("same") ? primary
                : rollbackKind.equals("none") ? null : failure(rollbackKind);
        DataSource faulty = JdbcMutationFault.afterMutation(source, "never matched", primary, rollback);
        var authority = new JdbcFileSpaceAuthority(faulty, new JdbcTenantStore(source));
        assertFailure(catchThrowable(() -> authority.read(() -> { throw primary; })), primary, rollback);
        assertThat(new JdbcStorageProfileRegistry(source).createIfAbsent(profile("after"))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"sql", "runtime", "error"})
    void authorityGateFailureDoesNotCallOperation(String kind) throws Exception {
        DataSource source = MigratedPostgres.create();
        Throwable primary = failure(kind);
        var rollback = new SQLException("rollback failed");
        DataSource faulty = JdbcMutationFault.afterStatement(source,
                "SELECT pg_advisory_xact_lock_shared(", primary, rollback);
        var authority = new JdbcFileSpaceAuthority(faulty, new JdbcTenantStore(source));
        var called = new AtomicBoolean();
        assertFailure(catchThrowable(() -> authority.read(() -> called.getAndSet(true))), primary, rollback);
        assertThat(called).isFalse();
        assertThat(new JdbcStorageProfileRegistry(source).createIfAbsent(profile("after"))).isTrue();
    }

    private static Operation operation(DataSource source, String name) {
        new JdbcStorageProfileRegistry(source).createIfAbsent(profile("existing"));
        var spaces = new JdbcFileSpaceRegistry(source);
        var stage = spaces.create(definition("landing"), false);
        var index = new JdbcFileDirectoryIndex(source);
        var old = index.begin(stage, "");
        index.append(old, null, page("old.txt", null));
        index.publish(old);
        var token = index.begin(stage, "");
        if (!name.equals("directoryAppend")) index.append(token, null, page("new.txt", null));
        var claim = new CopyFileClaim("tenant-a", UUID.randomUUID().toString(), "s3://bucket/data/file.parquet",
                "version-1", "a".repeat(64), 123, "existing", UUID.randomUUID().toString(), 1, 0, "execution-1");
        return switch (name) {
            case "profileCreate" -> new Operation("INSERT INTO storage_profile_definitions", ds ->
                    () -> new JdbcStorageProfileRegistry(ds).createIfAbsent(profile("new")));
            case "profileDrop" -> new Operation("DELETE FROM storage_profile_definitions", ds ->
                    () -> new JdbcStorageProfileRegistry(ds).unregister("existing"));
            case "spaceCreate" -> new Operation("INSERT INTO file_space_registry", ds ->
                    () -> new JdbcFileSpaceRegistry(ds).create(definition("another"), false));
            case "spaceMutate" -> new Operation("DELETE FROM file_directory_index", ds ->
                    () -> new JdbcFileSpaceRegistry(ds).mutate(FileSpaceType.STAGE, "media", "raw", "landing",
                            new FileSpaceMutation.SetDirectoryEnabled(false)));
            case "spaceRename" -> new Operation("UPDATE file_space_registry SET space_name", ds ->
                    () -> new JdbcFileSpaceRegistry(ds).rename(FileSpaceType.STAGE, "media", "raw", "landing", "renamed"));
            case "spaceDrop" -> new Operation("DELETE FROM file_space_registry", ds ->
                    () -> new JdbcFileSpaceRegistry(ds).drop(FileSpaceType.STAGE, "media", "raw", "landing", false));
            case "directoryBegin" -> new Operation("INSERT INTO file_directory_refresh", ds ->
                    () -> new JdbcFileDirectoryIndex(ds).begin(stage, ""));
            case "directoryAppend" -> new Operation("UPDATE file_directory_refresh SET", ds ->
                    () -> new JdbcFileDirectoryIndex(ds).append(token, null, page("new.txt", "next")));
            case "directoryPublish" -> new Operation("INSERT INTO file_directory_index", ds ->
                    () -> new JdbcFileDirectoryIndex(ds).publish(token));
            case "directoryAbort" -> new Operation("DELETE FROM file_directory_refresh", ds ->
                    () -> new JdbcFileDirectoryIndex(ds).abort(token));
            case "claim" -> new Operation("INSERT INTO copy_file_claims", ds ->
                    () -> new JdbcCopyFileClaimStore(ds).claim(claim));
            default -> throw new IllegalArgumentException(name);
        };
    }

    private static FileSpaceDefinition definition(String name) {
        return new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", name,
                false, "local", "", true, "", "alice");
    }

    private static StorageProfile profile(String name) {
        return new StorageProfile(name, StorageType.S3, "bucket", "data/", "", "us-east-1", Map.of(), Map.of());
    }

    private static FileListing page(String name, String next) {
        return new FileListing(List.of(new FileStat(name, 123, Instant.parse("2026-11-01T00:00:00Z"),
                "etag", "application/octet-stream", "version-1", null)), next);
    }

    private static Throwable failure(String kind) {
        return switch (kind) {
            case "sql" -> new SQLException("injected SQL failure");
            case "runtime" -> new IllegalStateException("injected runtime failure");
            case "error" -> new AssertionError("injected error");
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private static void assertFailure(Throwable actual, Throwable primary, Throwable rollback) {
        if (primary instanceof SQLException) assertThat(actual).hasCauseReference(primary);
        else assertThat(actual).isSameAs(primary);
        if (rollback == null || rollback == primary) assertThat(primary.getSuppressed()).isEmpty();
        else assertThat(primary.getSuppressed()).containsExactly(rollback);
    }

    private static List<String> snapshot(DataSource source) throws SQLException {
        var facts = new ArrayList<String>();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            for (String table : List.of("storage_profile_definitions", "storage_profile_revisions",
                    "file_space_registry", "file_directory_refresh", "file_directory_pending",
                    "file_directory_index", "copy_file_claims", "pangrova_authorization_revision")) {
                try (var rows = statement.executeQuery("SELECT row_to_json(t)::text FROM " + table
                        + " t ORDER BY row_to_json(t)::text")) {
                    while (rows.next()) facts.add(table + ":" + rows.getString(1));
                }
            }
        }
        return List.copyOf(facts);
    }

    private record Operation(String sql, Function<DataSource, Runnable> action) {}
}
