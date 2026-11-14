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
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.file.api.access.FileDeletionClient;
import org.pangrova.file.api.access.FileListingClient;
import org.pangrova.file.access.FileSpaceAccessService;
import org.pangrova.file.api.access.FileAccessGrant;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.statement.RemoveStageStatement;
import org.pangrova.security.api.authorization.FileSpaceAccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.persistence.file.api.space.FileSpaceRegistry;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.filesystem.api.object.FileStat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoveStageTaskTest {

    private static final String SECRET = "remove-stage-test-secret";
    private static final long NOW = 1_000_000L;
    private static final UserIdentity USER =
            new UserIdentity("alice", Set.of("analysts"), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);

    private final List<FileAccessGrant> listingGrants = new ArrayList<>();
    private final List<FileAccessGrant> deleteGrants = new ArrayList<>();
    private final List<String> deleted = new ArrayList<>();
    private final List<String> events = new ArrayList<>();

    @Test
    void removesEveryPageAndFiltersWithThePattern() {
        RemoveStageStatement statement = (RemoveStageStatement) new AntlrSqlParser().parse(
                "REMOVE @media.raw.stage_landing/images/ PATTERN = '.*[.]jpg'");
        FileListingClient lister = (grant, token) -> {
            listingGrants.add(grant);
            events.add("list:" + token);
            if ("next".equals(token)) {
                return new FileListing(List.of(stat("images/c.jpg", 30, "v30")), null);
            }
            return new FileListing(List.of(stat("images/a.jpg", 10, "v10"),
                    stat("images/b.png", 20, "v20")), "next");
        };
        FileDeletionClient deleter = (grant, version) -> {
            deleteGrants.add(grant);
            events.add("delete:" + grant.path());
            deleted.add(version);
        };

        StatementResult result = new RemoveStageTask(service(), lister, deleter)
                .execute(statement, context());

        assertThat(result.columnNames()).containsExactly("name", "result");
        assertThat(result.rows()).extracting(row -> row.getRaw("name"))
                .containsExactly("@media.raw.stage_landing/images/a.jpg",
                        "@media.raw.stage_landing/images/c.jpg");
        assertThat(result.rows()).extracting(row -> row.getRaw("result"))
                .containsExactly("removed", "removed");
        assertThat(listingGrants.get(0).operation()).isEqualTo("list");
        assertThat(listingGrants.get(0).path()).isEqualTo("images");
        assertThat(deleteGrants).extracting(FileAccessGrant::operation)
                .containsExactly("delete", "delete");
        assertThat(deleteGrants).extracting(FileAccessGrant::path)
                .containsExactly("images/a.jpg", "images/c.jpg");
        assertThat(deleted).containsExactly("v10", "v30");
        // The complete listing finishes before anything is deleted.
        assertThat(events).containsExactly("list:null", "list:next",
                "delete:images/a.jpg", "delete:images/c.jpg");
    }

    @Test
    void fallsBackToTheEtagWhenNoProviderVersionIsReported() {
        RemoveStageStatement statement = (RemoveStageStatement) new AntlrSqlParser().parse(
                "REMOVE @media.raw.stage_landing/images/a.jpg");
        FileListingClient lister = (grant, token) -> new FileListing(
                List.of(new FileStat("images/a.jpg", 10, Instant.EPOCH, "etag-a", null, null,
                        null)), null);
        FileDeletionClient deleter = (grant, version) -> deleted.add(version);

        new RemoveStageTask(service(), lister, deleter).execute(statement, context());

        assertThat(deleted).containsExactly("etag-a");
    }

    @Test
    void perFileFailureIsReportedInsteadOfDiscardingOtherRows() {
        RemoveStageStatement statement =
                (RemoveStageStatement) new AntlrSqlParser().parse("REMOVE @media.raw.stage_landing");
        FileListingClient lister = (grant, token) -> new FileListing(List.of(
                stat("a.jpg", 1, "v1"), stat("b.jpg", 2, "v2")), null);
        FileDeletionClient deleter = (grant, version) -> {
            if ("v1".equals(version)) {
                throw new PangrovaException(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED,
                        "changed underneath");
            }
        };

        StatementResult result = new RemoveStageTask(service(), lister, deleter)
                .execute(statement, context());

        assertThat(result.rows()).extracting(row -> row.getRaw("result"))
                .containsExactly("error: changed underneath", "removed");
    }

    @Test
    void listingFailureLeavesEveryObjectUntouched() {
        RemoveStageStatement statement =
                (RemoveStageStatement) new AntlrSqlParser().parse("REMOVE @media.raw.stage_landing");
        FileListingClient lister = (grant, token) -> {
            if (token != null) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "worker went away");
            }
            return new FileListing(List.of(stat("a.jpg", 1, "v1")), "next");
        };
        FileDeletionClient deleter = (grant, version) -> deleted.add(version);

        assertThatThrownBy(() -> new RemoveStageTask(service(), lister, deleter)
                .execute(statement, context()))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
        assertThat(deleted).isEmpty();
    }

    @Test
    void invalidPatternIsRejectedBeforeContactingAWorker() {
        RemoveStageStatement statement = (RemoveStageStatement) new AntlrSqlParser().parse(
                "REMOVE @media.raw.stage_landing PATTERN = '['");

        assertThatThrownBy(() -> new RemoveStageTask(service(),
                (grant, token) -> {
                    throw new AssertionError("worker must not be called");
                },
                (grant, version) -> {
                    throw new AssertionError("worker must not be called");
                }).execute(statement, context()))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT));
    }

    @Test
    void missingTicketIssuerFailsClosed() {
        RemoveStageStatement statement =
                (RemoveStageStatement) new AntlrSqlParser().parse("REMOVE @media.raw.stage_landing");

        assertThatThrownBy(() -> new RemoveStageTask(null, null, null)
                .execute(statement, context()))
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
                new FileSpaceAuditEmitter(null), SECRET,
                org.pangrova.coordinator.file.FileAuthorityTestFixture.authority());
        var profiles = new org.pangrova.testing.testkit.file.InMemoryStorageProfileRegistry();
        profiles.register(profile);
        return new FileSpaceAccessService(new Spaces(spaces), profiles,
                new AllowAll(), context);
    }

    private static StatementExecutionContext context() {
        QuerySession session = new QuerySession("remove-stage",
                Map.of("catalog", "media", "schema", "raw"), USER, "tenant-a", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        return new StatementExecutionContext(null, session.queryId(), session);
    }

    private static FileStat stat(String path, long size, String version) {
        return new FileStat(path, size, Instant.parse("2026-10-25T00:00:00Z"),
                "etag-" + size, null, version, null);
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

        @Override
        public void checkCanWriteStage(UserIdentity user, String catalog, String schema,
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
