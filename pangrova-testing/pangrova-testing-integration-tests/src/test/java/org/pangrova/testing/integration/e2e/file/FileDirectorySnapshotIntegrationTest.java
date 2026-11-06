package org.pangrova.testing.integration.e2e.file;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.persistence.file.postgresql.directory.JdbcFileDirectoryIndex;
import org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry;
import org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner;
import org.pangrova.file.api.directory.FileDirectoryReference;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.file.api.space.FileSpaceType;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.worker.file.JdbcFileDirectoryPageSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Production migrations/publisher and Worker reader over actual PostgreSQL snapshots. */
@E2ETest
@Testcontainers
class FileDirectorySnapshotIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    private static final Instant MODIFIED = Instant.parse("2001-02-03T04:05:06.123456Z");
    private PGSimpleDataSource authority;
    private PGSimpleDataSource worker;
    private JdbcFileSpaceRegistry spaces;
    private JdbcFileDirectoryIndex index;
    private FileSpace stage;
    private String application;

    @BeforeEach
    void setup() throws Exception {
        String schema = "directory_" + UUID.randomUUID().toString().replace("-", "");
        authority = source(null);
        try (var connection = authority.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        authority.setCurrentSchema(schema);
        SchemaMigrationRunner.migrate(authority);
        worker = source(schema);
        application = "reader_" + schema;
        worker.setApplicationName(application);
        spaces = new JdbcFileSpaceRegistry(authority);
        index = new JdbcFileDirectoryIndex(authority);
        stage = spaces.create(definition(), false);
    }

    @Test
    void boundedPagesPreserveActualValuesAndReleaseSnapshotAtEof() throws Exception {
        var files = new ArrayList<FileStat>();
        for (int i = 0; i < 2305; i++) {
            files.add(file(String.format(java.util.Locale.ROOT, "file-%04d", i), i));
        }
        publish(files);
        var observed = new ArrayList<FileStat>();
        try (var reader = open(index.reference(stage), 97)) {
            assertThat(activeWorkerTransactions()).isEqualTo(1);
            while (true) {
                var page = reader.nextPage();
                assertThat(page).hasSizeLessThanOrEqualTo(97);
                if (page.isEmpty()) break;
                observed.addAll(page);
            }
            assertThat(observed).containsExactlyElementsOf(files);
            assertThat(reader.nextPage()).isEmpty();
            assertReleased();
        }
    }

    @Test
    void readerWorksWithOnlySelectAuthorityOnTheTwoIndexTables() throws Exception {
        publish(List.of(file("a", 1)));
        String role = "reader_" + UUID.randomUUID().toString().replace("-", "");
        String password = UUID.randomUUID().toString();
        try (var connection = authority.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'");
            statement.execute("GRANT USAGE ON SCHEMA " + authority.getCurrentSchema() + " TO " + role);
            statement.execute("GRANT SELECT ON file_space_registry,file_directory_index TO " + role);
        }
        worker.setUser(role);
        worker.setPassword(password);
        try (var reader = open(index.reference(stage), 10)) {
            assertThat(reader.nextPage()).containsExactly(file("a", 1));
        }
        assertThatThrownBy(() -> {
            try (var connection = worker.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("DELETE FROM file_directory_index");
            }
        }).isInstanceOf(java.sql.SQLException.class).hasMessageContaining("permission denied");
        assertReleased();
    }

    @Test
    void refreshCannotSpliceNewRowsIntoAnEnteredSnapshot() throws Exception {
        publish(List.of(file("a", 1), file("b", 2), file("c", 3)));
        var reference = index.reference(stage);
        try (var reader = open(reference, 1)) {
            assertThat(reader.nextPage()).containsExactly(file("a", 1));
            publish(List.of(file("a", 10), file("bb", 20), file("d", 40)));
            assertThat(reader.nextPage()).containsExactly(file("b", 2));
            assertThat(reader.nextPage()).containsExactly(file("c", 3));
            assertThat(reader.nextPage()).isEmpty();
        }
        try (var fresh = open(reference, 10)) {
            assertThat(fresh.nextPage()).containsExactly(file("a", 10), file("bb", 20), file("d", 40));
        }
        assertReleased();
    }

    @Test
    void dropAndNameReuseCannotRebindAnEnteredSnapshotOrAnOldReference() throws Exception {
        publish(List.of(file("a", 1), file("b", 2)));
        var old = index.reference(stage);
        try (var reader = open(old, 1)) {
            assertThat(reader.nextPage()).containsExactly(file("a", 1));
            spaces.drop(FileSpaceType.STAGE, "media", "raw", "landing", false);
            stage = spaces.create(definition(), false);
            publish(List.of(file("replacement", 99)));
            assertThat(reader.nextPage()).containsExactly(file("b", 2));
            assertThat(reader.nextPage()).isEmpty();
        }
        assertThat(index.reference(stage).spaceId()).isNotEqualTo(old.spaceId());
        assertThatThrownBy(() -> open(old, 10)).isInstanceOf(PangrovaException.class);
        try (var fresh = open(index.reference(stage), 10)) {
            assertThat(fresh.nextPage()).containsExactly(file("replacement", 99));
        }
        assertReleased();
    }

    @Test
    void changedOrDisabledDefinitionRejectsNewReadersAndDoesNotLeakConnections() throws Exception {
        var original = stage;
        var old = index.reference(stage);
        stage = spaces.rename(FileSpaceType.STAGE, "media", "raw", "landing", "renamed");
        assertThatThrownBy(() -> index.reference(original)).hasMessageContaining("changed");
        assertThatThrownBy(() -> open(old, 10)).isInstanceOf(PangrovaException.class);
        stage = spaces.mutate(FileSpaceType.STAGE, "media", "raw", "renamed",
                new FileSpaceMutation.SetComment("updated definition"));
        var renamed = index.reference(stage);
        assertThat(renamed.definitionRevision()).isEqualTo(spaces.find(FileSpaceType.STAGE,
                "media", "raw", "renamed").orElseThrow().updatedAt());
        try (var current = open(renamed, 10)) {
            assertThat(current.nextPage()).isEmpty();
        }
        stage = spaces.mutate(FileSpaceType.STAGE, "media", "raw", "renamed",
                new FileSpaceMutation.SetDirectoryEnabled(false));
        assertThatThrownBy(() -> open(renamed, 10)).isInstanceOf(PangrovaException.class);
        assertThatThrownBy(() -> index.reference(stage)).hasMessageContaining("not enabled");
        assertReleased();
    }

    @Test
    void cancelInterruptsARealBlockedReadAndClosesItsTransaction() throws Exception {
        publish(List.of(file("a", 1)));
        var executor = Executors.newSingleThreadExecutor();
        try (var reader = open(index.reference(stage), 10);
                var locker = authority.getConnection()) {
            lockIndex(locker);
            var read = executor.submit(reader::nextPage);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(workerSessions("wait_event_type='Lock'")).isEqualTo(1));
            reader.cancel();
            assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(PangrovaException.class);
            reader.close();
            assertReleased();
            locker.rollback();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void queryDeadlineCancelsBlockedIoWithoutPublishingEof() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var reader = JdbcFileDirectoryPageSource.open(worker, index.reference(stage), 10,
                Instant.now().plusSeconds(3)); var locker = authority.getConnection()) {
            lockIndex(locker);
            var read = executor.submit(reader::nextPage);
            assertThatThrownBy(() -> read.get(8, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(PangrovaException.class);
            assertReleased();
            locker.rollback();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void invalidBoundsAndExpiredAdmissionFailBeforeOpeningAConnection() throws Exception {
        var reference = index.reference(stage);
        for (int size : List.of(0, 1001)) {
            assertThatThrownBy(() -> open(reference, size)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> JdbcFileDirectoryPageSource.open(worker, reference, 10, Instant.EPOCH))
                .isInstanceOf(PangrovaException.class);
        var reader = open(reference, 10);
        reader.close();
        reader.close();
        assertThatThrownBy(reader::nextPage).isInstanceOf(IllegalStateException.class);
        assertReleased();
    }

    private JdbcFileDirectoryPageSource open(FileDirectoryReference reference, int pageSize) {
        return JdbcFileDirectoryPageSource.open(worker, reference, pageSize, Instant.now().plusSeconds(30));
    }

    private void publish(List<FileStat> files) {
        var token = index.begin(stage, "");
        for (int start = 0; start < files.size(); start += 1000) {
            int end = Math.min(files.size(), start + 1000);
            index.append(token, start == 0 ? null : Integer.toString(start),
                    new FileListing(files.subList(start, end), end == files.size() ? null : Integer.toString(end)));
        }
        if (files.isEmpty()) index.append(token, null, new FileListing(List.of(), null));
        index.publish(token);
    }

    private int activeWorkerTransactions() throws Exception { return workerSessions("backend_xmin IS NOT NULL"); }
    private void assertReleased() {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(workerSessions("true")).isZero());
    }

    private int workerSessions(String predicate) throws Exception {
        try (var connection = authority.getConnection();
                var query = connection.prepareStatement("SELECT count(*) FROM pg_stat_activity "
                        + "WHERE application_name=? AND " + predicate)) {
            query.setString(1, application);
            try (var row = query.executeQuery()) {
                row.next();
                return row.getInt(1);
            }
        }
    }

    private static void lockIndex(Connection locker) throws Exception {
        locker.setAutoCommit(false);
        try (var statement = locker.createStatement()) {
            statement.execute("LOCK TABLE file_directory_index IN ACCESS EXCLUSIVE MODE");
        }
    }

    private static FileStat file(String path, long size) {
        return new FileStat(path, size, MODIFIED, "etag-" + size, "text/plain", "v-" + size, null);
    }

    private static FileSpaceDefinition definition() {
        return new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", "landing", false,
                "files", "", true, "", "owner");
    }

    private static PGSimpleDataSource source(String schema) {
        var source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        if (schema != null) source.setCurrentSchema(schema);
        return source;
    }
}
