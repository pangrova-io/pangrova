package org.pangrova.coordinator.query.execution.dispatch.task.file;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.file.FileListingClient;
import org.pangrova.coordinator.file.FileSpaceAccessService;
import org.pangrova.coordinator.file.FileSpaceAccessService.FileAccessGrant;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.statement.ListStageStatement;
import org.pangrova.spi.security.FileSpaceAccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileSpace;
import org.pangrova.spi.storage.file.FileSpaceDefinition;
import org.pangrova.spi.storage.file.FileSpaceMutation;
import org.pangrova.spi.storage.file.FileSpaceRegistry;
import org.pangrova.spi.storage.file.FileSpaceType;
import org.pangrova.spi.storage.file.FileStat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListStageTaskTest {

    private static final String SECRET = "list-stage-test-secret";
    private static final long NOW = 1_000_000L;
    private static final UserIdentity USER =
            new UserIdentity("alice", Set.of("analysts"), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);

    private final List<FileAccessGrant> grants = new ArrayList<>();

    @Test
    void listsEveryPageAndFiltersWithThePattern() {
        ListStageStatement statement = (ListStageStatement) new SqlParser().parse(
                "LIST @media.raw.stage_landing/images/ PATTERN = '.*[.]jpg'");
        FileListingClient client = (grant, token) -> {
            grants.add(grant);
            if ("next".equals(token)) {
                return new FileListing(List.of(stat("images/c.jpg", 30, "md5-c")), null);
            }
            return new FileListing(List.of(stat("images/a.jpg", 10, "md5-a"),
                    stat("images/b.png", 20, null)), "next");
        };

        StatementResult result = new ListStageTask(service(), client).execute(statement, context());

        assertThat(result.columnNames())
                .containsExactly("name", "size", "md5", "last_modified");
        assertThat(result.rows()).hasSize(2);
        assertThat(result.rows()).extracting(row -> row.getRaw("name"))
                .containsExactly("@media.raw.stage_landing/images/a.jpg",
                        "@media.raw.stage_landing/images/c.jpg");
        assertThat(result.rows()).extracting(row -> row.getRaw("size"))
                .containsExactly(10L, 30L);
        assertThat(result.rows()).extracting(row -> row.getRaw("md5"))
                .containsExactly("md5-a", "md5-c");
        assertThat(grants).hasSize(2);
        assertThat(grants.get(0).operation()).isEqualTo("list");
        assertThat(grants.get(0).path()).isEqualTo("images");
    }

    @Test
    void invalidPatternIsRejectedBeforeContactingAWorker() {
        ListStageStatement statement = (ListStageStatement) new SqlParser().parse(
                "LIST @media.raw.stage_landing PATTERN = '['");
        FileListingClient client = (grant, token) -> {
            throw new AssertionError("worker must not be called");
        };

        assertThatThrownBy(() -> new ListStageTask(service(), client)
                .execute(statement, context()))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT));
    }

    @Test
    void missingTicketIssuerFailsClosed() {
        ListStageStatement statement =
                (ListStageStatement) new SqlParser().parse("LIST @media.raw.stage_landing");

        assertThatThrownBy(() -> new ListStageTask(null, null).execute(statement, context()))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
    }

    private FileSpaceAccessService service() {
        Map<String, FileSpace> spaces = new HashMap<>();
        FileSpace stage = new FileSpace(UUID.randomUUID(), FileSpaceType.STAGE, "media", "raw", "stage_landing",
                true, "media", "s3://acme/stage/landing", "", false, "", "alice",
                Instant.EPOCH, Instant.EPOCH);
        spaces.put("media.raw.stage_landing", stage);
        StorageProfile profile = new StorageProfile("media", StorageType.S3, "acme", "",
                "http://localhost:9000", "us-east-1", Map.of(),
                Map.of("path-style-access", "true"));
        var context = new FileSpaceAccessService.FileSpaceAccessContext("media",
                Duration.ofMinutes(5), () -> NOW, () -> List.of("http://w1:8080"),
                new FileSpaceAuditEmitter(null), SECRET);
        var profiles = new org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry();
        profiles.register(profile);
        return new FileSpaceAccessService(new Spaces(spaces), profiles,
                new AllowAll(), context);
    }

    private static StatementExecutionContext context() {
        CoordinatorSessionContext session = new CoordinatorSessionContext("list-stage",
                Map.of("catalog", "media", "schema", "raw"), USER, "tenant-a");
        return new StatementExecutionContext(null, session.queryId(), session, null, null);
    }

    private static FileStat stat(String path, long size, String md5) {
        return new FileStat(path, size, Instant.parse("2026-10-25T00:00:00Z"), "etag-" + size,
                null, "v" + size, md5);
    }

    private static final class AllowAll implements FileSpaceAccessControl {

        @Override
        public void checkCanReadVolume(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanUseStage(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanReadStage(UserIdentity user, String catalog, String schema,
                String name) {
        }
    }

    private record Spaces(Map<String, FileSpace> byName) implements FileSpaceRegistry {

        @Override
        public FileSpace create(FileSpaceDefinition definition, boolean ifNotExists) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileSpace mutate(FileSpaceType type, String catalog, String schema, String name,
                FileSpaceMutation mutation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileSpace rename(FileSpaceType type, String catalog, String schema, String name,
                String newName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileSpace> find(java.util.UUID id) {
            return byName.values().stream().filter(space -> space.id().equals(id)).findFirst();
        }

        @Override
        public Optional<FileSpace> find(FileSpaceType type, String catalog, String schema,
                String name) {
            return Optional.ofNullable(byName.get(String.join(".",
                    List.of(catalog, schema, name))));
        }

        @Override
        public List<FileSpace> list(FileSpaceType type, String catalog, String schema) {
            return List.copyOf(byName.values());
        }

        @Override
        public boolean drop(FileSpaceType type, String catalog, String schema, String name,
                boolean ifExists) {
            throw new UnsupportedOperationException();
        }
    }


}
