package org.pangrova.coordinator.file;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.file.access.FileGrantAudit;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.file.postgresql.directory.JdbcFileDirectoryIndex;
import org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry;
import org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.authorization.FileSpaceAccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.file.api.access.FileListingClient;
import org.pangrova.file.directory.StageDirectoryRefreshService;
import org.pangrova.file.access.FileSpaceAccessService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageDirectoryRefreshServiceTest {
    private static final UserIdentity USER = new UserIdentity("alice", Set.of(), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);
    private final AtomicBoolean readable = new AtomicBoolean(true);
    private final FileSpaceAccessControl authorization = new FileSpaceAccessControl() {
        @Override
        public void checkCanManageStage(UserIdentity user, String catalog, String schema, String name) {}
        @Override
        public void checkCanUseStage(UserIdentity user, String catalog, String schema, String name) {}
        @Override
        public void checkCanReadStage(UserIdentity user, String catalog, String schema, String name) {
            if (!readable.get()) {
                throw new AccessDeniedException("READ_STAGE revoked");
            }
        }
    };
    private JdbcFileDirectoryIndex index;
    private FileSpaceAccessService access;
    private FileSpace stage;

    @BeforeEach
    void setUp() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("directory_service");
        var spaces = new JdbcFileSpaceRegistry(source);
        var profiles = new JdbcStorageProfileRegistry(source);
        profiles.createIfAbsent(new StorageProfile("files", StorageType.S3,
                "test-bucket", "", "", "", Map.of(), Map.of()));
        stage = spaces.create(new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", "landing",
                false, "files", "", true, "", "alice"), false);
        index = new JdbcFileDirectoryIndex(source);
        access = new FileSpaceAccessService(spaces, profiles, authorization,
                FileSpaceAccessService.FileSpaceAccessContext.of("",
                        () -> List.of("http://worker.invalid"), new FileGrantAudit(event -> {}), "test-secret", FileAuthorityTestFixture.provision(source)));
        var initial = index.begin(stage, "");
        index.append(initial, null, new FileListing(List.of(file("old.txt")), null));
        index.publish(initial);
    }

    @Test
    void workerFailureAfterAPagePreservesPublishedIndex() {
        var calls = new AtomicInteger();
        var service = service((grant, token) -> {
            if (calls.getAndIncrement() == 0) {
                return new FileListing(List.of(file("new.txt")), "page-2");
            }
            throw new IllegalStateException("Worker unavailable");
        });
        assertThatThrownBy(() -> service.refresh(USER, stage, ""))
                .hasMessageContaining("Worker unavailable");
        assertThat(index.page(stage, null, 10)).containsExactly(file("old.txt"));
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void revocationBetweenPagesPreventsFurtherReadsAndPublication() {
        var calls = new AtomicInteger();
        var service = service((grant, token) -> {
            calls.incrementAndGet();
            readable.set(false);
            return new FileListing(List.of(file("new.txt")), "page-2");
        });
        assertThatThrownBy(() -> service.refresh(USER, stage, ""))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(index.page(stage, null, 10)).containsExactly(file("old.txt"));
    }

    @Test
    void emptyIntermediatePageContinuesWithTheExactWorkerCheckpoint() {
        var calls = new AtomicInteger();
        var service = service((grant, token) -> {
            assertThat(grant.operation()).isEqualTo("list");
            if (calls.getAndIncrement() == 0) {
                assertThat(token).isNull();
                return new FileListing(List.of(), "opaque-token");
            }
            assertThat(token).isEqualTo("opaque-token");
            return new FileListing(List.of(file("new.txt")), null);
        });
        assertThat(service.refresh(USER, stage, "")).isEqualTo(1);
        assertThat(index.page(stage, null, 10)).containsExactly(file("new.txt"));
    }

    private StageDirectoryRefreshService service(FileListingClient listings) {
        return new StageDirectoryRefreshService(index, access, listings, authorization);
    }

    private static FileStat file(String path) {
        return new FileStat(path, 1, Instant.EPOCH, "etag", null, "version", null);
    }
}
